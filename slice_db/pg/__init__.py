import asyncio
import contextlib
import os
import typing

import asyncpg
from pg_sql import SqlObject, sql_list

Snapshot = str


def server_settings():
    application_name = os.environ.get("PGAPPNAME", "slice_db")
    return {"application_name": application_name}


@contextlib.asynccontextmanager
async def connection_manager(connection: asyncpg.Connection):
    try:
        yield connection
    finally:
        await connection.close()


async def export_snapshot(conn: asyncpg.Connection) -> Snapshot:
    return await conn.fetchval("SELECT pg_export_snapshot()")


async def set_snapshot(conn: asyncpg.Connection, snapshot: Snapshot):
    # TODO: properly interpolate
    await conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")


async def defer_constraints(conn: asyncpg.Connection, names: typing.List[SqlObject]):
    query = f"SET CONSTRAINTS {sql_list(names)} DEFERRED"
    await conn.execute(query)


def tid_decoder(bytes: bytes) -> int:
    return int.from_bytes(bytes, "big")


def tid_encoder(int: int) -> bytes:
    return int.to_bytes(6, "big")


async def set_tid_codec(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "tid",
        schema="pg_catalog",
        encoder=tid_encoder,
        decoder=tid_decoder,
        format="binary",
    )
