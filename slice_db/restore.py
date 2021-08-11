import asyncio
import contextlib
import dataclasses
import itertools
import json
import logging
import time
import typing

import asyncpg
from pg_sql import SqlId, SqlNumber, SqlObject, SqlString

from .collection.dict import groups
from .concurrent import to_thread, wait_success
from .concurrent.graph import GraphRunner
from .concurrent.lock import LifoSemaphore
from .formats.dump import DumpSchema
from .formats.manifest import (
    MANIFEST_DATA_JSON_FORMAT,
    Manifest,
    ManifestTable,
    ManifestTableSegment,
)
from .log import TRACE
from .pg import defer_constraints
from .resource import AsyncResourceFactory, ResourceFactory
from .slice import SliceReader


@dataclasses.dataclass
class RestoreParams:
    include_schema: bool
    parallelism: int
    transaction: bool


@dataclasses.dataclass
class RestoreIo:
    conn: AsyncResourceFactory[asyncpg.Connection]
    input: ResourceFactory[typing.BinaryIO]


async def restore(io, params):
    if 1 < params.parallelism and params.transaction:
        raise Exception("A single transaction must be disabled for parallelism > 1")

    lock = asyncio.Semaphore(params.parallelism)

    with io.input() as file, SliceReader(file) as reader:
        manifest = MANIFEST_DATA_JSON_FORMAT.load(reader.open_manifest)

        async with contextlib.AsyncExitStack() as stack:
            if params.transaction:
                conn = await stack.enter_async_context(io.conn())
                await stack.enter_async_context(conn.transaction())

                @contextlib.asynccontextmanager
                async def conn_factory():
                    async with lock:
                        yield conn

            else:

                @contextlib.asynccontextmanager
                async def conn_factory():
                    async with lock, io.conn() as conn:
                        yield conn

            if params.include_schema:
                async with conn_factory() as conn:
                    for i in range(manifest.pre_data.count):
                        logging.info("Running pre-data %d", i)
                        with reader.open_schema("pre-data", i) as f:
                            schema_sql = f.read().decode("utf-8")
                        await conn.execute(schema_sql)

            await _restore_sequences(
                conn_factory=conn_factory,
                manifest=manifest,
                reader=reader,
            )

            await _restore_rows(
                conn_factory=conn_factory,
                include_schema=params.include_schema,
                manifest=manifest,
                reader=reader,
                transaction=params.transaction,
            )

            if params.include_schema:
                async with conn_factory() as conn:
                    for i in range(manifest.post_data.count):
                        logging.info("Running post-data %d", i)
                        with reader.open_schema("post-data", i) as f:
                            schema_sql = f.read().decode("utf-8")
                        await conn.execute(schema_sql)


async def _restore_sequences(
    conn_factory: AsyncResourceFactory,
    manifest: Manifest,
    reader: SliceReader,
):
    async with conn_factory() as conn:
        for id, sequence in manifest.sequences.items():
            value = SqlNumber(reader.read_sequence(id))
            seq = SqlObject(SqlId(sequence.schema), SqlId(sequence.name))
            await conn.execute(
                f"""
                SELECT setval({SqlString(str(seq))}, {SqlNumber(value)})
                FROM {seq}
                WHERE last_value < {value}
                """
            )


async def _restore_rows(
    conn_factory: AsyncResourceFactory,
    include_schema: bool,
    manifest: Manifest,
    reader: SliceReader,
    transaction: bool,
):
    if include_schema:
        constraints = []
    else:
        async with conn_factory() as conn:
            constraints = await get_constaints(conn, manifest.tables)

            deferrable_constaints = [
                SqlObject(SqlId(constraint.schema), SqlId(constraint.name))
                for constraint in constraints
                if constraint.deferrable
            ]

            if deferrable_constaints:
                if not transaction:
                    raise Exception(f"Transaction required to defer {constraints[0]}")

                logging.info("Deferring %d constraints", len(deferrable_constaints))
                await defer_constraints(conn, deferrable_constaints)

    items = {
        id: RestoreItem(
            conn_factory=conn_factory,
            id=id,
            reader=reader,
            table=table,
        )
        for id, table in manifest.tables.items()
    }
    tasks = {id: asyncio.create_task(item()) for id, item in items.items()}

    for constraint in constraints:
        if constraint.deferrable:
            continue
        item = items[constraint.table]
        item.deps.append(tasks[constraint.reference_table])

    await wait_success(tasks.values())


@dataclasses.dataclass
class RestoreItem:
    conn_factory: AsyncResourceFactory[asyncpg.Connection]
    id: str
    reader: SliceReader
    table: ManifestTable
    deps: typing.List[asyncio.Task] = dataclasses.field(default_factory=list)

    async def __call__(self):
        await asyncio.gather(*self.deps)

        async with self.conn_factory() as conn:
            for i, segment in enumerate(self.table.segments):
                with self.reader.open_segment(
                    self.id,
                    i,
                ) as file:
                    await update_data(conn, self.id, self.table, i, segment, file)

    def __hash__(self):
        return id(self)


async def update_data(
    conn: asyncpg.Connection,
    id: str,
    table: ManifestTable,
    index: int,
    segment: ManifestTableSegment,
    in_: typing.BinaryIO,
):
    logging.log(TRACE, f"Restoring %s rows into table %s", segment.row_count, id)
    start = time.perf_counter()

    async def source():
        while True:
            bytes = await to_thread(in_.read, 1024 * 32)
            if not bytes:
                break
            yield bytes

    await conn.copy_to_table(
        table.name,
        source=source(),
        schema_name=table.schema,
        columns=table.columns,
    )
    end = time.perf_counter()
    logging.debug(
        f"Restored %s rows in table %s (%.3fs)",
        segment.row_count,
        id,
        end - start,
    )


@dataclasses.dataclass
class ForeignKey:
    deferrable: bool
    """Deferrable"""
    name: str
    """Name"""
    schema: str
    """Schema"""
    table: str
    """Table ID"""
    reference_table: str
    """Referenced table ID"""


async def get_constaints(
    conn: asyncpg.Connection, manifest_tables: typing.Dict[str, ManifestTable]
) -> typing.List[ForeignKey]:
    """
    Query PostgreSQL for constraints between tables
    """

    query = """
        WITH
            "table" AS (
                SELECT *
                FROM unnest($1::text[], $2::text[], $3::text[]) AS t (id, schema, name)
            )
        SELECT
            pn.nspname,
            pc.conname,
            a.id,
            b.id,
            pc.condeferrable
        FROM
            pg_constraint AS pc
            JOIN pg_class AS pc2 ON pc.conrelid = pc2.oid
            JOIN pg_namespace AS pn ON pc2.relnamespace = pn.oid
            JOIN "table" AS a ON (pn.nspname, pc2.relname) = (a.schema, a.name)
            JOIN pg_class AS pc3 ON pc.confrelid = pc3.oid
            JOIN pg_namespace AS pn2 ON pc3.relnamespace = pn2.oid
            JOIN "table" AS b ON (pn2.nspname, pc3.relname) = (b.schema, b.name)
        WHERE pc.contype = 'f'
    """
    rows = await conn.fetch(
        query,
        list(manifest_tables.keys()),
        [table.schema for table in manifest_tables.values()],
        [table.name for table in manifest_tables.values()],
    )

    foreign_keys = []
    for schema, name, table, reference_table, deferrable in rows:
        foreign_keys.append(
            ForeignKey(
                deferrable=deferrable,
                name=name,
                reference_table=reference_table,
                schema=schema,
                table=table,
            )
        )

    return foreign_keys
