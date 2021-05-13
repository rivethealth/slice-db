from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import enum
import functools
import logging
import shutil
import tempfile
import time
import typing

import asyncpg
import numpy
from pg_sql import SqlId, SqlObject, sql_list

from .collection.set import IntSet
from .concurrent import to_thread, wait_success
from .concurrent.lock import LifoSemaphore
from .formats.dump import (
    DUMP_DATA_JSON_FORMAT,
    DumpReferenceDirection,
    DumpRoot,
    DumpSchema,
)
from .formats.manifest import (
    MANIFEST_DATA_JSON_FORMAT,
    Manifest,
    ManifestTable,
    ManifestTableSegment,
    ManifestTableSegmentId,
)
from .formats.transform import TRANSFORM_DATA_JSON_FORMAT
from .log import TRACE
from .pg import Tid, export_snapshot, int_to_tid, set_snapshot, tid_to_int
from .resource import AsyncResourceFactory, ResourceFactory
from .slice import SliceWriter
from .sql import SqlWriter
from .transform import TableTransformer


class OutputType(enum.Enum):
    SQL = enum.auto()
    SLICE = enum.auto()


@dataclasses.dataclass
class DumpIo:
    conn: AsyncResourceFactory[asyncpg.Connection]
    output: ResourceFactory[typing.BinaryIO]
    schema_file: ResourceFactory[typing.TextIO]
    transform_file: typing.Optional[AsyncResourceFactory[typing.TextIO]]


@dataclasses.dataclass
class DumpParams:
    include_schema: bool
    parallelism: int
    pepper: bytes
    output_type: OutputType


async def dump(
    root_configs: typing.List[DumpRoot],
    io: DumpIo,
    params: DumpParams,
):
    """
    Dump
    """
    dump_config = DUMP_DATA_JSON_FORMAT.load(io.schema_file)
    schema = Schema(dump_config)
    roots = []
    for root_config in root_configs:
        try:
            table = schema.get_table(root_config.table)
        except KeyError:
            raise Exception(f"Root table {root_config.table} does not exist")
        roots.append(Root(table=table, condition=root_config.condition))

    if io.transform_file is None:
        transformers = {}
    else:
        transform = TRANSFORM_DATA_JSON_FORMAT.load(io.transform_file)
        transformers = {
            id: TableTransformer(transform_table.columns, schema.get_table(id).columns)
            for id, transform_table in transform.tables.items()
        }

    with io.output() as file, contextlib.ExitStack() as stack:
        if params.output_type == OutputType.SLICE:
            slice_writer = stack.enter_context(SliceWriter(file))
            output = _SliceOutput(slice_writer)
        elif params.output_type == OutputType.SQL:
            sql_writer = SqlWriter(file)
            if params.include_schema:
                # must add schema before others
                with sql_writer.open_predata() as f:
                    await _pg_dump_section("pre-data", f)
            output = _SqlOutput(sql_writer)

        result = _DiscoveryResult()

        isolation = "repeatable_read" if params.parallelism == 1 else None
        async with io.conn() as conn, conn.transaction(
            isolation=isolation,
            # https://github.com/MagicStack/asyncpg/issues/743
            # readonly=True
        ):
            if params.parallelism == 1:

                @contextlib.asynccontextmanager
                async def conn_factory():
                    yield conn

            else:
                snapshot = await export_snapshot(conn)
                logging.info("Running at snapshot %s", snapshot)

                @contextlib.asynccontextmanager
                async def conn_factory():
                    async with io.conn() as conn, conn.transaction(
                        isolation="repeatable_read",
                        # https://github.com/MagicStack/asyncpg/issues/743
                        # readonly=True
                    ):
                        await set_snapshot(conn, snapshot)
                        yield conn

            await _dump_rows(
                conn_factory=conn_factory,
                include_schema=params.include_schema
                and params.output_type != OutputType.SQL,
                output=output,
                parallelism=params.parallelism,
                pepper=params.pepper,
                result=result,
                roots=roots,
                transformers=transformers,
            )

        if params.output_type == OutputType.SLICE:
            manifest = Manifest(tables=result.table_manifests())
            MANIFEST_DATA_JSON_FORMAT.dump(slice_writer.open_manifest, manifest)
        elif params.output_type == OutputType.SQL:
            if params.include_schema:
                with sql_writer.open_postdata() as f:
                    await _pg_dump_section("post-data", f)


async def _dump_rows(
    conn_factory: ResourceFactory[asyncpg.Connection],
    include_schema: bool,
    output: typing.Union[_SliceOutput, _SqlOutput],
    parallelism: int,
    pepper: bytes,
    result,
    roots: typing.List[Root],
    transformers: typing.Dict[str, TableTransformer],
):
    """
    Dump rows
    """

    if include_schema:
        logging.info("Dumping schema and rows")
    else:
        logging.info("Dumping rows")
    start = time.perf_counter()

    lock = LifoSemaphore(parallelism)
    dump = _Dump(
        conn_factory=conn_factory,
        lock=lock,
        output=output,
        pepper=pepper,
        result=result,
        transformers=transformers,
    )
    items = dump.root_items(roots)
    if include_schema:
        items.append(SchemaItem(section="pre-data", output=output))
        items.append(SchemaItem(section="post-data", output=output))

    await wait_success(asyncio.create_task(item()) for item in items)

    end = time.perf_counter()
    if include_schema:
        logging.info(
            "Dumped schema and %d total rows (%.3fs)", result.row_count, end - start
        )
    else:
        logging.info("Dumped %d total rows (%.3fs)", result.row_count, end - start)


class _SliceOutput:
    """
    Concurrency-safe slice output
    """

    def __init__(self, writer: SliceWriter):
        self._lock = asyncio.Lock()
        self._writer = writer

    @contextlib.asynccontextmanager
    async def open_schema(self, section: str):
        """
        Open schema for writing
        """
        async with self._lock:
            with self._writer.open_schema(section) as f:
                yield f

    @contextlib.asynccontextmanager
    async def open_segment(self, segment: TableSegment):
        """
        Open segment for writing
        """
        async with self._lock:
            with self._writer.open_segment(segment.table.id, segment.index) as f:
                yield f


class _SqlOutput:
    """
    Concurrency-safe SQL output
    """

    def __init__(self, writer: SqlWriter):
        self._lock = asyncio.Lock()
        self._writer = writer

    def open_schema(self, section: str):
        raise Exception("Not supported")

    @contextlib.asynccontextmanager
    async def open_segment(self, segment: TableSegment):
        async with self._lock:
            with self._writer.open_data(
                segment.table.id,
                segment.index,
                segment.table.schema,
                segment.table.name,
                segment.table.columns,
            ) as f:
                yield f


async def _pg_dump_section(section: str, out: typing.BinaryIO) -> str:
    logging.log(TRACE, "Dumping %s schema", section)
    start = time.perf_counter()
    process = await asyncio.subprocess.create_subprocess_exec(
        "pg_dump",
        "-B",
        "--no-acl",
        "--section",
        section,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=out,
    )
    result = await process.wait()
    if result:
        raise Exception(f"pg_dump exited with code {result}")
    end = time.perf_counter()
    logging.debug("Dumped %s schema (%.3fs)", section, end - start)


class _DiscoveryResult:
    """
    Discovered IDs
    """

    _row_ids: typing.DefaultDict[str, IntSet]
    _table_manifests: typing.Dict[str, ManifestTable]

    def __init__(self):
        self._id_count = 0
        self._row_ids = collections.defaultdict(lambda: IntSet(numpy.int64))
        self._table_manifests = {}

    def add(
        self, table: Table, row_ids: typing.List[int], source: typing.List[TableSegment]
    ) -> typing.Optional[TableSegment]:
        """
        Add IDs and return list of newly added segment
        """
        existing_ids = self._row_ids[table.id]
        new_ids = existing_ids.add(row_ids)
        if not new_ids:
            return

        self._id_count += len(new_ids)

        if table.id not in self._table_manifests:
            self._table_manifests[table.id] = ManifestTable(
                columns=table.columns,
                name=table.name,
                schema=table.schema,
                segments=[],
            )
        table_manifest = self._table_manifests[table.id]

        segment = TableSegment(
            table=table,
            row_ids=new_ids,
            index=len(table_manifest.segments),
        )
        manifest_source = [
            ManifestTableSegmentId(table_id=s.table.id, index=s.index) for s in source
        ]
        table_manifest.segments.append(
            ManifestTableSegment(row_count=len(new_ids), source=manifest_source)
        )

        return segment

    @property
    def row_count(self):
        """
        Total rows
        """
        return self._id_count

    def table_manifests(self):
        """
        Dict of ManifestTables
        """
        return self._table_manifests


@dataclasses.dataclass
class _Dump:
    conn_factory: AsyncResourceFactory[asyncpg.Connection]
    lock: typing.AsyncContextManager
    output: _SliceOutput
    pepper: str
    result: _DiscoveryResult
    transformers: typing.Dict[str, TableTransformer]

    async def dump_segment(self, to_table: Table, segment: TableSegment):
        with tempfile.TemporaryFile() as tmp:
            async with self.conn_factory() as conn:
                await _dump_data(conn, to_table, segment.row_ids, tmp)
            tmp.seek(0)
            async with self.output.open_segment(segment) as f:
                try:
                    transformer = self.transformers[to_table.id]
                except KeyError:
                    await to_thread(shutil.copyfileobj, tmp, f)
                else:
                    await to_thread(
                        TableTransformer.transform_binary,
                        transformer,
                        self.pepper,
                        tmp,
                        f,
                    )

    def root_items(self, roots: typing.List[Root]):
        return [
            RootItem(table=root.table, condition=root.condition, dump=self)
            for root in roots
        ]

    async def next(
        self,
        segment: TableSegment,
        source: typing.List[TableSegment],
        reference_item: ReferenceItem = None,
    ):
        items: typing.List[ReferenceItem] = []

        """
        Create items for table
        """
        for reference in segment.table.references:
            if DumpReferenceDirection.FORWARD not in reference.directions:
                continue
            if (
                reference_item is not None
                and reference is reference_item.reference
                and reference_item.direction == DumpReferenceDirection.REVERSE
            ):
                continue
            items.append(
                ReferenceItem(
                    dump=self,
                    segment=segment,
                    reference=reference,
                    direction=DumpReferenceDirection.FORWARD,
                    source=source,
                )
            )
        for reference in segment.table.reverse_references:
            if DumpReferenceDirection.REVERSE not in reference.directions:
                continue
            if (
                reference_item is not None
                and reference is reference_item.reference
                and reference_item.direction == DumpReferenceDirection.FORWARD
            ):
                continue
            items.append(
                ReferenceItem(
                    dump=self,
                    segment=segment,
                    reference=reference,
                    direction=DumpReferenceDirection.REVERSE,
                    source=source,
                )
            )

        await wait_success(asyncio.create_task(item()) for item in items)


@dataclasses.dataclass
class RootItem:
    table: Table
    condition: str
    dump: _Dump

    async def __call__(self):
        task: asyncio.Task = None

        try:
            async with self.dump.lock:
                async with self.dump.conn_factory() as conn:
                    segment = await _discover_table_condition(
                        conn, self.table, self.condition, self.dump.result
                    )
                    if segment is None:
                        return

                task = asyncio.create_task(self.dump.next(segment, []))

                await self.dump.dump_segment(self.table, segment)
        except:
            if task is not None:
                task.cancel()
                asyncio.wait([task])
            raise

        if task is not None:
            await task


@dataclasses.dataclass
class ReferenceItem:
    direction: DumpReferenceDirection
    reference: Reference
    segment: TableSegment
    dump: _Dump
    source: typing.List[TableSegment]

    async def __call__(self):
        task: asyncio.Task = None

        try:
            async with self.dump.lock:
                async with self.dump.conn_factory() as conn:
                    segment = await _discover_reference(
                        conn,
                        self.reference,
                        self.direction,
                        self.segment,
                        self.source,
                        self.dump.result,
                    )
                    if segment is None:
                        return

                if self.direction == DumpReferenceDirection.FORWARD:
                    to_table = self.reference.reference_table
                elif self.direction == DumpReferenceDirection.REVERSE:
                    to_table = self.reference.table

                task = asyncio.create_task(
                    self.dump.next(segment, self.source, reference_item=self)
                )

                await self.dump.dump_segment(to_table, segment)
        except:
            if task is not None:
                task.cancel()
                asyncio.wait([task])
            raise

        if task is not None:
            await task


@dataclasses.dataclass
class SchemaItem:
    section: str
    output: _SliceOutput

    async def __call__(self):
        with tempfile.TemporaryFile() as tmp:
            await _pg_dump_section(self.section, tmp)
            tmp.seek(0)
            async with self.output.open_schema(self.section) as f:
                await to_thread(shutil.copyfileobj, tmp, f)


@dataclasses.dataclass
class Root:
    """Root"""

    table: Table
    """Table"""
    condition: str
    """Condition"""


@dataclasses.dataclass
class Table:
    """Table"""

    id: str
    """ID"""
    name: str
    """Name"""
    schema: str
    """Schema"""
    columns: typing.List[str]
    """Columns"""
    references: typing.List[Reference]
    """References to parent tables"""
    reverse_references: typing.List[Reference]
    """References to child tables"""

    @property
    def columns_sql(self):
        return [SqlId(column) for column in self.columns]

    @property
    def sql(self):
        return SqlObject(SqlId(self.schema), SqlId(self.name))


@dataclasses.dataclass
class Reference:
    """Reference"""

    directions: typing.List[DumpReferenceDirection]
    """Directions"""
    id: str
    """ID"""
    table: Table
    """Table"""
    columns: typing.List[str]
    """Columns"""
    reference_table: Table
    """Reference columns"""
    reference_columns: typing.List[str]


@dataclasses.dataclass
class TableSegment:
    index: int
    row_ids: typing.Any
    table: Table


class Schema:
    """
    Graph model of schema
    """

    def __init__(self, schema: DumpSchema):
        self._tables = {}
        for id, table_config in schema.tables.items():
            table = Table(
                columns=table_config.columns,
                references=[],
                id=id,
                name=table_config.name,
                reverse_references=[],
                schema=table_config.schema,
            )
            self._tables[table.id] = table

        self._references = {}
        for id, reference_config in schema.references.items():
            try:
                table = self._tables[reference_config.table]
            except KeyError:
                raise Exception(
                    f"No table {reference_config.table}, needed by reference {reference_config.id}"
                )
            try:
                reference_table = self._tables[reference_config.reference_table]
            except KeyError:
                raise Exception(
                    f"No table {reference_config.reference_table}, needed by reference {reference_config.id}"
                )

            reference = Reference(
                directions=reference_config.directions,
                id=id,
                table=table,
                columns=reference_config.columns,
                reference_table=reference_table,
                reference_columns=reference_config.reference_columns,
            )
            self._references[id] = reference
            table.references.append(reference)
            reference_table.reverse_references.append(reference)

    def get_table(self, id) -> Table:
        """
        Get table by ID
        """
        return self._tables[id]

    def tables(self):
        """
        Iterable of tables
        """
        return self._tables.values()


async def _dump_data(
    conn: asyncpg.Connection, table: Table, ids: typing.List[int], out: typing.BinaryIO
):
    """
    Dump data
    """

    logging.log(TRACE, f"Dumping %s rows from table %s", len(ids), table.id)
    start = time.perf_counter()

    await conn.execute("CREATE TEMP TABLE _slicedb (_ctid tid) ON COMMIT DROP")
    await conn.copy_records_to_table(
        "_slicedb", records=[[int_to_tid(id)] for id in ids], schema_name="pg_temp"
    )
    query = f"SELECT {sql_list(table.columns_sql)} FROM {table.sql} AS t JOIN _slicedb AS s ON t.ctid = s._ctid"
    await conn.copy_from_query(query, output=functools.partial(to_thread, out.write))
    await conn.execute("DROP TABLE pg_temp._slicedb")
    end = time.perf_counter()
    logging.debug(
        f"Dumped %s rows from table %s (%.3fs)", len(ids), table.id, end - start
    )


async def _discover_table_condition(
    conn: asyncpg.Connection, table: Table, condition: str, result: _DiscoveryResult
) -> typing.List[Tid]:
    """
    Discover, using root
    """
    logging.log(TRACE, f"Finding rows from table %s", table.id)
    start = time.perf_counter()
    query = f"SELECT ctid FROM {table.sql} WHERE {condition}"
    found_ids = [tid_to_int(id_) for id_, in await conn.fetch(query)]
    segment = result.add(table, found_ids, []) if found_ids else None
    end = time.perf_counter()
    if segment is None:
        logging.debug(
            f"Found no rows in table %s (%.3fs)",
            len(found_ids),
            table.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s (%.3fs)",
            len(found_ids),
            len(segment.row_ids),
            segment.table.id,
            segment.index,
            end - start,
        )
    end = time.perf_counter()
    return segment


async def _discover_reference(
    conn: asyncpg.Connection,
    reference: Reference,
    direction: DumpReferenceDirection,
    segment: TableSegment,
    source: typing.List[TableSegment],
    result,
) -> typing.List[Tid]:
    """
    Discover, using reference
    """
    if direction == DumpReferenceDirection.FORWARD:
        from_columns = reference.columns
        from_table = reference.table
        to_columns = reference.reference_columns
        to_table = reference.reference_table
    elif direction == DumpReferenceDirection.REVERSE:
        from_columns = reference.reference_columns
        from_table = reference.reference_table
        to_columns = reference.columns
        to_table = reference.table

    logging.log(
        TRACE,
        f"Finding rows from table %s using %s/%s via %s",
        to_table.id,
        segment.table.id,
        segment.index,
        reference.id,
    )
    start = time.perf_counter()
    from_expr = sql_list([SqlObject(SqlId("a"), SqlId(name)) for name in from_columns])
    to_expr = sql_list([SqlObject(SqlId("b"), SqlId(name)) for name in to_columns])
    query = f"""
        SELECT DISTINCT b.ctid
        FROM {from_table.sql} AS a
            JOIN {to_table.sql} AS b ON ({from_expr}) = ({to_expr})
        WHERE a.ctid = ANY($1::tid[])
    """
    tids = [int_to_tid(id) for id in segment.row_ids]
    found_ids = [tid_to_int(id_) for id_, in await conn.fetch(query, [tids])]

    new_segment = (
        result.add(to_table, found_ids, source + [segment]) if found_ids else None
    )
    end = time.perf_counter()
    if new_segment is None:
        logging.debug(
            f"Found %s rows (no new) in table %s using %s/%s via %s (%.3fs)",
            len(found_ids),
            to_table.id,
            segment.table.id,
            segment.index,
            reference.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s using %s/%s via %s (%.3fs)",
            len(found_ids),
            len(new_segment.row_ids),
            new_segment.table.id,
            new_segment.index,
            segment.table.id,
            segment.index,
            reference.id,
            end - start,
        )

    return new_segment
