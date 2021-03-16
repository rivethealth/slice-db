from __future__ import annotations

import collections
import contextlib
import dataclasses
import logging
import threading
import time
import typing

import numpy
import psycopg2.sql as sql

from .collection.set import IntSet
from .concurrent.work import Worker, WorkerRunner
from .formats.dump import (
    DUMP_DATA_JSON_FORMAT,
    DumpReferenceDirection,
    DumpRoot,
    DumpSchema,
)
from .formats.manifest import MANIFEST_DATA_JSON_FORMAT, Manifest, ManifestTable
from .formats.manifest import ManifestTableSegment as TableSegmentManifest
from .log import TRACE
from .pg import Tid, export_snapshot, freeze_transaction, tid_to_int, transaction
from .resource import ResourceFactory
from .slice import SliceWriter


@dataclasses.dataclass
class TableSegment:
    index: int
    row_ids: typing.List[Tid]
    table: Table


def dump(
    conn_fn: ResourceFactory,
    schema_file_fn: ResourceFactory[typing.TextIO],
    root_configs: typing.List[DumpRoot],
    parallelism: int,
    output_fn: ResourceFactory[typing.BinaryIO],
):
    """
    Dump
    """
    dump_config = DUMP_DATA_JSON_FORMAT.load(schema_file_fn)
    schema = Schema(dump_config)
    roots = []
    for root_config in root_configs:
        try:
            table = schema.get_table(root_config.table)
        except KeyError:
            raise Exception(f"Root table {root_config.table} does not exist")
        roots.append(Root(table=table, condition=sql.SQL(root_config.condition)))

    with output_fn() as file, SliceWriter(file) as writer:
        result = _DiscoveryResult()

        with conn_fn() as conn, transaction(conn) as cur:
            if parallelism == 1:
                freeze_transaction(cur)

                def pg_manager():
                    return contextlib.nullcontext(cur)

            else:
                snapshot = export_snapshot(cur)
                logging.info("Running at snapshot %s", snapshot)

                @contextlib.contextmanager
                def pg_manager():
                    with conn_fn() as conn, transaction(conn) as cur:
                        freeze_transaction(cur, snapshot=snapshot)
                        yield cur

            _dump_rows(roots, parallelism, pg_manager, result, writer)

        manifest = Manifest(tables=result.table_manifests())

        MANIFEST_DATA_JSON_FORMAT.dump(writer.open_manifest, manifest)


def _dump_rows(
    roots: typing.List[Root],
    parallelism: int,
    cur_resource: ResourceFactory,
    result,
    writer: SliceWriter,
):
    """
    Dump rows
    """
    output = _Output(writer)

    logging.info("Dumping rows")
    start = time.perf_counter()
    worker = _Dump(result, output)
    runner = WorkerRunner(parallelism, worker.process_item, cur_resource)
    runner.run(
        [_RootItem(table=root.table, condition=root.condition) for root in roots]
    )
    end = time.perf_counter()
    logging.info("Dumping %d rows (%.3fs)", result.row_count, end - start)


class _Output:
    """
    Thread-safe dump output
    """

    def __init__(self, writer: SliceWriter):
        self._lock = threading.Lock()
        self._writer = writer

    @contextlib.contextmanager
    def open_segment(self, table_id: str, index: int):
        """
        Open segment for writing
        """
        with self._lock, self._writer.open_segment(table_id, index) as f:
            yield f


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
        self._lock = threading.Lock()

    def add(
        self, table: Table, row_ids: typing.List[Tid]
    ) -> typing.Optional[TableSegment]:
        """
        Add IDs and return list of newly added segment
        """
        with self._lock:
            existing_ids = self._row_ids[table.id]
            new_ids = []
            new_ints = []
            for id_ in row_ids:
                id_int = tid_to_int(id_)
                if id_int in existing_ids:
                    continue
                new_ids.append(id_)
                new_ints.append(id_int)

            if not new_ids:
                return

            existing_ids.add(new_ints)

            self._id_count += len(new_ids)

            if table.id not in self._table_manifests:
                self._table_manifests[table.id] = ManifestTable(
                    columns=table.columns,
                    id=table.id,
                    name=table.name,
                    schema=table.schema,
                    segments=[],
                )
            table_manifest = self._table_manifests[table.id]
            segment = TableSegment(
                table=table, row_ids=new_ids, index=len(table_manifest.segments)
            )
            table_manifest.segments.append(TableSegmentManifest(row_count=len(new_ids)))

        return segment

    @property
    def row_count(self):
        """
        Total rows
        """
        return self._id_count

    def table_manifests(self):
        """
        Iterable of ManifestTables
        """
        return self._table_manifests.values()


@dataclasses.dataclass
class _RootItem:
    table: Table
    """Table"""
    condition: sql.SQL
    """Condition"""


@dataclasses.dataclass
class _ReferenceItem:
    direction: DumpReferenceDirection
    """Direction"""
    reference: Reference
    """Reference"""
    segment: TableSegment
    """Source segment"""


class _Dump:
    def __init__(self, result: _DiscoveryResult, output: _Output):
        self._result = result
        self._output = output

    def process_item(self, item: typing.Union[_RootItem, _ReferenceItem], cur):
        """
        Process item
        """
        if isinstance(item, _RootItem):
            segment = _discover_table_condition(
                cur, item.table, item.condition, self._result
            )
            if segment is None:
                return
            to_table = item.table

            yield from self._table_items(segment)
        elif isinstance(item, _ReferenceItem):
            segment = _discover_reference(
                cur, item.reference, item.direction, item.segment, self._result
            )
            if segment is None:
                return

            if item.direction == DumpReferenceDirection.FORWARD:
                to_table = item.reference.reference_table
            elif item.direction == DumpReferenceDirection.REVERSE:
                to_table = item.reference.table

            yield from self._table_items(segment, reference_item=item)

        with self._output.open_segment(segment.table.id, segment.index) as f:
            _dump_data(cur, to_table, segment.row_ids, f)

    def _table_items(
        self,
        segment: TableSegment,
        reference_item: _ReferenceItem = None,
    ):
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
            yield _ReferenceItem(
                segment=segment,
                reference=reference,
                direction=DumpReferenceDirection.FORWARD,
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
            yield _ReferenceItem(
                segment=segment,
                reference=reference,
                direction=DumpReferenceDirection.REVERSE,
            )


@dataclasses.dataclass
class Root:
    """Root"""

    table: Table
    """Table"""
    condition: sql.SQL
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
        self._tables = {}
        for table_config in schema.tables:
            table = Table(
                columns=table_config.columns,
                references=[],
                id=table_config.id,
                name=table_config.name,
                reverse_references=[],
                schema=table_config.schema,
            )
            if table.id in self._tables:
                raise Exception(f"Multiple definitions for table {table.id}")
            self._tables[table.id] = table

        self._references = {}
        for reference_config in schema.references:
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
                id=reference_config.id,
                table=table,
                columns=reference_config.columns,
                reference_table=reference_table,
                reference_columns=reference_config.reference_columns,
            )
            if reference.id in self._references:
                raise Exception(f"Multiple definitions for reference {reference.id}")
            self._references[reference.id] = reference
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


def _dump_data(cur, table: Table, ids: typing.List[Tid], out):
    """
    Dump data
    """

    logging.log(TRACE, f"Dumping %s rows from table %s", len(ids), table.id)
    start = time.perf_counter()
    query = sql.SQL(
        """
            COPY (
                SELECT {}
                FROM {}
                WHERE ctid = ANY({}::tid[])
            )
            TO STDOUT
        """
    ).format(
        sql.SQL(", ").join([sql.Identifier(column) for column in table.columns]),
        sql.Identifier(table.schema, table.name),
        sql.Literal(ids),
    )
    cur.copy_expert(query, out, size=1024 * 32)
    end = time.perf_counter()
    logging.debug(
        f"Dumped %s rows from table %s (%.3fs)", len(ids), table.id, end - start
    )


def _discover_table_condition(
    cur, table: Table, condition: sql.SQL, result: _DiscoveryResult
) -> typing.List[Tid]:
    """
    Discover, using root
    """
    logging.log(TRACE, f"Finding rows from table %s", table.id)
    start = time.perf_counter()
    query = sql.SQL(
        """
            SELECT ctid
            FROM {}
            WHERE {}
        """
    ).format(
        sql.Identifier(table.schema, table.name),
        condition,
    )
    cur.execute(query)
    found_ids = [id_ for id_, in cur.fetchall()]
    segment = result.add(table, found_ids) if found_ids else None
    end = time.perf_counter()
    if segment is None:
        logging.debug(
            f"Found %s rows (no new) in table %s (%.3fs)",
            len(found_ids),
            table.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s in table %s (%.3fs)",
            len(found_ids),
            len(segment.row_ids),
            segment.table.id,
            segment.index,
            end - start,
        )
    end = time.perf_counter()
    return segment


def _discover_reference(
    cur,
    reference: Reference,
    direction: DumpReferenceDirection,
    segment: TableSegment,
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
    query = sql.SQL(
        """
            SELECT DISTINCT b.ctid
            FROM {} AS a
                JOIN {} AS b ON ({}) = ({})
            WHERE a.ctid = ANY(%s::tid[])
        """
    ).format(
        sql.Identifier(from_table.schema, from_table.name),
        sql.Identifier(to_table.schema, to_table.name),
        sql.SQL(", ").join([sql.Identifier("a", name) for name in from_columns]),
        sql.SQL(", ").join([sql.Identifier("b", name) for name in to_columns]),
    )
    cur.execute(query, [segment.row_ids])
    found_ids = [id_ for id_, in cur.fetchall()]
    segment = result.add(to_table, found_ids) if found_ids else None
    end = time.perf_counter()
    if segment is None:
        logging.debug(
            f"Found %s rows (no new) in table %s using %s/%s via %s (%.3fs)",
            len(found_ids),
            to_table.id,
            reference.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s using %s/%s via %s (%.3fs)",
            len(found_ids),
            len(segment.row_ids),
            segment.table.id,
            segment.index,
            reference.id,
            end - start,
        )

    return segment
