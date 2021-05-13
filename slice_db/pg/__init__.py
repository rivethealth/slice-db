import asyncio
import contextlib
import typing

import asyncpg
from pg_sql import SqlObject, sql_list

Snapshot = str

Tid = [int, int]


@contextlib.asynccontextmanager
async def connection_manager(connection: asyncpg.Connection):
    try:
        yield connection
    finally:
        connection.close()


async def export_snapshot(conn: asyncpg.Connection) -> Snapshot:
    return await conn.fetchval("SELECT pg_export_snapshot()")


async def set_snapshot(conn: asyncpg.Connection, snapshot: Snapshot):
    # TODO: properly interpolate
    await conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")


async def defer_constraints(conn: asyncpg.Connection, names: typing.List[SqlObject]):
    query = f"SET CONSTRAINTS {sql_list(names)} DEFERRED"
    await conn.execute(query)


def int_to_tid(int: int) -> Tid:
    a = int // (2 ** (4 * 8))
    b = int % (2 ** (4 * 8))
    return a, b


def tid_to_int(id: Tid) -> int:
    """
    Translate TID to 48-bit integer, where 32 top significant bits are block number,
    and the other 16 bits are the position within the block
    """
    a, b = id
    return a * (2 ** (4 * 8)) + b
