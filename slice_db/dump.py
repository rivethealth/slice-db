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
from .concurrent.queue import Queue
from .formats.dump import (
    DUMP_DATA_JSON_FORMAT,
    DumpReferenceDirection,
    DumpRoot,
    DumpSchema,
    DumpTable,
)
from .formats.manifest import (
    MANIFEST_DATA_JSON_FORMAT,
    Manifest,
    ManifestSchema,
    ManifestSequence,
    ManifestTable,
    ManifestTableSegment,
)
from .formats.transform import TRANSFORM_DATA_JSON_FORMAT
from .log import TRACE
from .pg import export_snapshot, set_snapshot
from .pg.token import parse_statements
from .resource import AsyncResourceFactory, ResourceFactory
from .slice import SliceWriter
from .sql import SqlWriter
from .transform import Transforms


class OutputType(enum.Enum):
    SQL = enum.auto()
    SLICE = enum.auto()


@dataclasses.dataclass
class DumpIo:
    conn: AsyncResourceFactory[asyncpg.Connection]
    output: ResourceFactory[typing.BinaryIO]
    schema_file: ResourceFactory[typing.TextIO]
    transform_file: typing.Optional[AsyncResourceFactory[typing.TextIO]]


class DumpStrategy(typing.Protocol):
    new_transactions: bool

    def start(self, dump: Dump, roots: typing.List[Root]):
        pass


@dataclasses.dataclass
class DumpParams:
    include_schema: bool
    parallelism: int
    pepper: bytes
    output_type: OutputType
    strategy: DumpStrategy


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
        transforms = Transforms(transform.transforms, params.pepper)
        transformers = {
            id: transforms.table(transform_table.columns, schema.get_table(id).columns)
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
            row_counts = await _set_row_counts(conn, list(schema.tables()))

            if params.parallelism == 1 and not params.strategy.new_transactions:

                @contextlib.asynccontextmanager
                async def conn_factory():
                    yield conn

            else:
                await conn.execute("SET idle_in_transaction_session_timeout TO 0")
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
                result=result,
                roots=roots,
                strategy=params.strategy,
                transformers=transformers,
            )

            await _dump_sequences(
                conn_factory=conn_factory, result=result, output=output, schema=schema
            )

        if params.output_type == OutputType.SLICE:
            manifest = Manifest(
                pre_data=ManifestSchema(count=result.section_counts["pre-data"]),
                post_data=ManifestSchema(count=result.section_counts["post-data"]),
                sequences=result.sequence_manifests(),
                tables=result.table_manifests(),
            )
            MANIFEST_DATA_JSON_FORMAT.dump(slice_writer.open_manifest, manifest)
        elif params.output_type == OutputType.SQL:
            if params.include_schema:
                with sql_writer.open_postdata() as f:
                    await _pg_dump_section("post-data", f)


async def _dump_rows(
    conn_factory: ResourceFactory[asyncpg.Connection],
    include_schema: bool,
    output: _Output,
    parallelism: int,
    result,
    roots: typing.List[Root],
    strategy: DumpStrategy,
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

    queue = Queue()
    lock = LifoSemaphore(parallelism)
    dump = Dump(
        conn_factory=conn_factory,
        lock=lock,
        output=output,
        result=result,
        transformers=transformers,
        queue=queue,
    )

    strategy.start(dump, roots)

    if include_schema:
        dump.start_task(_SchemaTask(section="pre-data", output=output, result=result)())
        dump.start_task(
            _SchemaTask(section="post-data", output=output, result=result)()
        )

    await queue.finished()

    end = time.perf_counter()
    if include_schema:
        logging.info(
            "Dumped schema and %d total rows (%.3fs)", result.row_count, end - start
        )
    else:
        logging.info("Dumped %d total rows (%.3fs)", result.row_count, end - start)


async def _dump_sequences(
    conn_factory: ResourceFactory[asyncpg.Connection],
    result: _DiscoveryResult,
    output: _Output,
    schema: Schema,
):
    sequence_ids = set()
    for table_id in result.table_manifests().keys():
        for sequence in schema.get_table(table_id).sequences:
            sequence_ids.add(sequence.id)

    if not sequence_ids:
        return

    async with conn_factory() as conn:
        logging.log(TRACE, "Dumping %s sequences", len(sequence_ids))
        start = time.perf_counter()

        for sequence_id in sequence_ids:
            sequence = schema.get_sequence(sequence_id)
            result.add_sequence(sequence)
            row = await conn.fetchrow(
                f"""
                SELECT last_value
                FROM {sequence.sql}
                """,
            )
            output.write_sequence(sequence, row["last_value"])

        end = time.perf_counter()
        logging.debug("Dumped %s sequences (%.3fs)", len(sequence_ids), end - start)


class _Output(typing.Protocol):
    async def open_schema(self, section: str, index: int) -> typing.BinaryIO:
        pass

    async def open_segment(self, segment: TableSegment) -> typing.BinaryIO:
        pass

    def write_sequence(self, sequence: Sequence, value: int):
        pass


class _SliceOutput(_Output):
    """
    Concurrency-safe slice output
    """

    def __init__(self, writer: SliceWriter):
        self._lock = asyncio.Lock()
        self._writer = writer

    @contextlib.asynccontextmanager
    async def open_schema(self, section: str, index: int):
        """
        Open schema for writing
        """
        async with self._lock:
            with self._writer.open_schema(section, index) as f:
                yield f

    @contextlib.asynccontextmanager
    async def open_segment(self, segment: TableSegment):
        """
        Open segment for writing
        """
        async with self._lock:
            with self._writer.open_segment(segment.table.id, segment.index) as f:
                yield f

    def write_sequence(self, sequence: Sequence, value: int):
        self._writer.write_sequence(sequence.id, value)


class _SqlOutput(_Output):
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

    def write_sequence(self, sequence: Sequence, value: int):
        self._writer.write_sequence(sequence.id, sequence.schema, sequence.name, value)


async def _pg_dump_section(section: str, out: typing.BinaryIO) -> str:
    logging.log(TRACE, "Dumping %s schema", section)
    start = time.perf_counter()
    process = await asyncio.subprocess.create_subprocess_exec(
        "pg_dump",
        "-B",
        "--disable-dollar-quoting",
        "--no-acl",
        "--quote-all-identifiers",
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
    _sequence_manifests: typing.Dict[str, ManifestSequence]
    _table_manifests: typing.Dict[str, ManifestTable]
    section_counts: typing.DefaultDict[str, int]

    def __init__(self):
        self._id_count = 0
        self._row_ids = collections.defaultdict(lambda: IntSet(numpy.int64))
        self._sequence_manifests = {}
        self._table_manifests = {}
        self.section_counts = collections.defaultdict(lambda: 0)

    def add(
        self, table: Table, row_ids: typing.List[int]
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
        table_manifest.segments.append(ManifestTableSegment(row_count=len(new_ids)))

        return segment

    def add_sequence(self, sequence: Sequence):
        self._sequence_manifests[sequence.id] = ManifestSequence(
            name=sequence.name, schema=sequence.schema
        )

    @property
    def row_count(self):
        """
        Total rows
        """
        return self._id_count

    def sequence_manifests(self):
        """
        Dict of ManifestSequences
        """
        return self._sequence_manifests

    def table_manifests(self):
        """
        Dict of ManifestTables
        """
        return self._table_manifests


@dataclasses.dataclass
class Dump:
    conn_factory: AsyncResourceFactory[asyncpg.Connection]
    lock: typing.AsyncContextManager
    output: _SliceOutput
    result: _DiscoveryResult
    transformers: typing.Dict[str, TableTransformer]
    queue: Queue

    async def _wrap(self, fn: typing.Callable):
        async with self.lock:
            return await fn

    def start_task(self, fn):
        self.queue.add(asyncio.create_task(self._wrap(fn)))


@dataclasses.dataclass
class _SchemaTask:
    section: str
    result: _DiscoveryResult
    output: _SliceOutput

    async def __call__(self):
        with tempfile.TemporaryFile() as tmp:
            await _pg_dump_section(self.section, tmp)
            tmp.seek(0)
            text = tmp.read().decode()
        # last statement is comment-only and messes up asyncpg
        for i, statement in enumerate(list(parse_statements(text))[:-1]):
            self.result.section_counts[self.section] += 1
            async with self.output.open_schema(self.section, i) as f:
                await to_thread(f.write, statement.encode())


@dataclasses.dataclass
class Root:
    """Root"""

    table: Table
    """Table"""
    condition: str
    """Condition"""


@dataclasses.dataclass
class Sequence:
    """Sequence"""

    id: str
    schema: str
    name: str

    @property
    def sql(self):
        return SqlObject(SqlId(self.schema), SqlId(self.name))


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
    row_count: int
    """Estimated number of total rows"""
    sequences: typing.List[Sequence]
    """Sequences"""

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


class Schema:
    """
    Graph model of schema
    """

    def __init__(self, schema: DumpSchema):
        self._sequences = {}
        for id, sequence in schema.sequences.items():
            self._sequences[id] = Sequence(id, sequence.schema, sequence.name)

        self._tables = {}
        for id, table_config in schema.tables.items():
            table = Table(
                columns=table_config.columns,
                references=[],
                id=id,
                name=table_config.name,
                reverse_references=[],
                row_count=0,
                schema=table_config.schema,
                sequences=[self._sequences[id] for id in table_config.sequences],
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

    def get_sequence(self, id: str) -> Sequence:
        return self._sequences[id]

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


@dataclasses.dataclass
class TableSegment:
    index: int
    row_ids: numpy.ndarray
    table: Table


async def _set_row_counts(conn: asyncpg.Connection, tables: typing.List[Table]):
    query = """
        SELECT reltuples
        FROM unnest($1::regclass[]) WITH ORDINALITY AS i (oid, ordinality)
            JOIN pg_class AS pc ON i.oid = pc.oid
        ORDER BY i.ordinality
    """
    result = await conn.fetch(query, [str(table.sql) for table in tables])
    for row, table in zip(result, tables):
        table.row_count = int(row["reltuples"])
