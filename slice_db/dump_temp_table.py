from __future__ import annotations

import asyncio
import dataclasses
import functools
import logging
import os
import shutil
import sys
import tempfile
import time
import typing

import asyncpg
from pg_sql import SqlId, SqlObject, sql_list

from .concurrent import to_thread
from .dump import (
    Dump,
    DumpReferenceDirection,
    DumpStrategy,
    Reference,
    Root,
    Table,
    TableSegment,
)
from .log import TRACE
from .transform import TableTransformer

MAX_SIZE = 1000 * 50


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
            segments = await _discover_table_condition(
                conn, self.table, self.condition, self.dump.result
            )

            for segment in segments:
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

        segments = await _discover_reference(
            conn,
            self.segment,
            reference,
            direction,
            self.dump.result,
        )

        for segment in segments:
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

                for reference in sorted(
                    self.segment.table.references,
                    key=lambda r: r.reference_table.row_count,
                ):
                    await self._process_reference(
                        conn, reference, DumpReferenceDirection.FORWARD
                    )
                for reference in sorted(
                    self.segment.table.reverse_references,
                    key=lambda r: r.table.row_count,
                ):
                    await self._process_reference(
                        conn, reference, DumpReferenceDirection.REVERSE
                    )
                await conn.execute("SET statement_timeout TO 0")
                await _dump_data(conn, self.segment.table, self.segment.row_ids, tmp)

            tmp.seek(0)
            try:
                transformer = self.dump.transformers[self.segment.table.id]
            except KeyError:
                # copy file without tranformation
                async with self.dump.output.open_segment(self.segment) as f:
                    await to_thread(shutil.copyfileobj, tmp, f)
            else:
                if hasattr(os, "fork"):
                    # in a forked process, transform to new temp file, and then copy
                    # that transformed temp file
                    with tempfile.TemporaryFile() as tmp_transformed:
                        os.set_inheritable(tmp.fileno(), True)
                        os.set_inheritable(tmp_transformed.fileno(), True)
                        pid = os.fork()
                        if not pid:
                            try:
                                TableTransformer.transform_binary(
                                    transformer, tmp, tmp_transformed
                                )
                                tmp_transformed.flush()
                            except BaseException as e:
                                print(str(e), file=sys.stderr)
                                os._exit(1)
                            os._exit(0)
                        _, exit_code = await to_thread(os.waitpid, pid, 0)
                        if exit_code:
                            raise Exception("Transform failed")
                        tmp_transformed.seek(0)
                        async with self.dump.output.open_segment(self.segment) as f:
                            await to_thread(shutil.copyfileobj, tmp_transformed, f)
                else:
                    # copy file with transformation
                    async with self.dump.output.open_segment(self.segment) as f:
                        await to_thread(
                            TableTransformer.transform_binary,
                            transformer,
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
        FROM {table.sql}
        WHERE ctid = ANY(ARRAY(SELECT tid FROM pg_temp._slice_db))
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

    await conn.execute("SET statement_timeout TO 0")
    query = f"""
        SELECT ctid
        FROM {table.sql}
        WHERE {condition}
        ORDER BY 1
    """
    found_ids = [id_ for id_, in await conn.fetch(query)]

    new_segments = []
    for i in range(0, len(found_ids), MAX_SIZE):
        await asyncio.sleep(0)
        new_segment = result.add(table, found_ids[i : i + MAX_SIZE])
        if new_segment is not None:
            new_segments.append(new_segment)

    end = time.perf_counter()
    if not new_segments:
        logging.debug(
            f"Found no rows in table %s (%.3fs)",
            table.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s (%.3fs)",
            len(found_ids),
            sum(len(segment.row_ids) for segment in new_segments),
            table.id,
            ",".join(str(segment.index) for segment in new_segments),
            end - start,
        )

    return new_segments


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
            JOIN pg_temp._slice_db AS sd ON a.ctid = sd.tid
        ORDER BY 1
    """
    found_ids = [id_ for id_, in await conn.fetch(query)]

    await conn.execute("SET statement_timeout TO 0")
    new_segments = []
    for i in range(0, len(found_ids), MAX_SIZE):
        await asyncio.sleep(0)
        new_segment = result.add(to_table, found_ids[i : i + MAX_SIZE])
        if new_segment is not None:
            new_segments.append(new_segment)

    end = time.perf_counter()
    if not new_segments:
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
            sum(len(segment.row_ids) for segment in new_segments),
            to_table.id,
            ",".join(str(segment.index) for segment in new_segments),
            segment.table.id,
            segment.index,
            reference.id,
            end - start,
        )

    return new_segments
