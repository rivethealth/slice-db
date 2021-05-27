from __future__ import annotations

import asyncio
import dataclasses
import functools
import logging
import shutil
import tempfile
import time

import asyncpg
from pg_sql import SqlId, SqlObject, sql_list

from .concurrent import to_thread
from .dump import Dump, DumpReferenceDirection, DumpStrategy, Table, TableSegment
from .log import TRACE
from .transform import TableTransformer


class TempTableStrategy(DumpStrategy):
    @property
    def new_transactions(self):
        return True

    def start(self, dump: Dump, roots: typing.List[Root]):
        for root in roots:
            task = _RootTask(table=root.table, condition=root.condition, dump=dump)
            dump.start_task(task())


@dataclasses.dataclass
class _RootTask:
    table: Table
    condition: str
    dump: Dump

    async def __call__(self):
        async with self.dump.conn_factory() as conn:
            segment = await _discover_table_condition(
                conn, self.table, self.condition, self.dump.result
            )
            if segment is None:
                return

            task = _TableTask(segment=segment, dump=self.dump)
            self.dump.start_task(task())


@dataclasses.dataclass
class _TableTask:
    segment: TableSegment
    dump: Dump
    source_direction: typing.Optional[DumpReferenceDirection] = None
    source_reference: typing.Optional[Reference] = None

    async def _process_reference(
        self,
        conn: asyncpg.Connection,
        reference: Reference,
        direction: DumpReferenceDirection,
    ) -> typing.Optional[asyncio.Task]:
        if direction not in reference.directions:
            return
        if direction == DumpReferenceDirection.FORWARD:
            if (
                self.source_direction == DumpReferenceDirection.REVERSE
                and self.source_reference is reference
            ):
                return
        elif direction == DumpReferenceDirection.REVERSE:
            if (
                self.source_direction == DumpReferenceDirection.FORWARD
                and self.source_reference is reference
            ):
                return

        segment = await _discover_reference(
            conn,
            self.segment,
            reference,
            direction,
            self.dump.result,
        )

        if segment is None:
            return

        task = _TableTask(
            dump=self.dump,
            segment=segment,
            source_direction=direction,
            source_reference=reference,
        )
        self.dump.start_task(task())

    async def __call__(self):
        with tempfile.TemporaryFile() as tmp:
            async with self.dump.conn_factory() as conn:
                await _prepare_discover_reference(conn, self.segment)

                for reference in self.segment.table.references:
                    await self._process_reference(
                        conn, reference, DumpReferenceDirection.FORWARD
                    )
                for reference in self.segment.table.reverse_references:
                    await self._process_reference(
                        conn, reference, DumpReferenceDirection.REVERSE
                    )

                await _dump_data(conn, self.segment.table, self.segment.row_ids, tmp)

            tmp.seek(0)
            async with self.dump.output.open_segment(self.segment) as f:
                try:
                    transformer = self.dump.transformers[self.segment.table.id]
                except KeyError:
                    await to_thread(shutil.copyfileobj, tmp, f)
                else:
                    await to_thread(
                        TableTransformer.transform_binary,
                        transformer,
                        self.dump.pepper,
                        tmp,
                        f,
                    )


async def _dump_data(conn: asyncpg.Connection, table: Table, ids, out: typing.BinaryIO):
    """
    Dump data
    """

    logging.log(TRACE, f"Dumping %s rows from table %s", len(ids), table.id)
    start = time.perf_counter()
    query = f"""
        SELECT {sql_list(table.columns_sql)}
        FROM
            {table.sql} AS t
            JOIN pg_temp._slice_db AS i ON t.ctid = i.tid
    """
    await conn.copy_from_query(query, output=functools.partial(to_thread, out.write))
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
    found_ids = [row["ctid"] for row in await conn.fetch(query)]
    segment = result.add(table, found_ids) if found_ids else None
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


async def _prepare_discover_reference(conn: asyncpg.Connection, segment: TableSegment):
    await conn.execute(
        """
        DO $$
            BEGIN
                IF to_regclass('pg_temp._slice_db') IS NULL THEN
                    CREATE TEMP TABLE _slice_db (tid tid)
                    ON COMMIT DELETE ROWS;
                END IF;
            END;
        $$
        """
    )
    await conn.copy_records_to_table(
        "_slice_db", records=((i,) for i in segment.row_ids), schema_name="pg_temp"
    )
    await conn.execute("ANALYZE pg_temp._slice_db")


async def _discover_reference(
    conn: asyncpg.Connection,
    segment: TableSegment,
    reference: Reference,
    direction: DumpReferenceDirection,
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
    # assumption: add reference has a unique value on the reference table
    # therefore, no need to dedup child records since they will be had by only one parent
    distinct = "DISTINCT" if direction == DumpReferenceDirection.FORWARD else ""
    query = f"""
        SELECT {distinct} b.ctid
        FROM {from_table.sql} AS a
            JOIN {to_table.sql} AS b ON ({from_expr}) = ({to_expr})
            JOIN pg_temp._slice_db AS i ON a.ctid = i.tid
    """
    found_ids = [id_ for id_, in await conn.fetch(query)]

    new_segment = result.add(to_table, found_ids) if found_ids else None
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
