import contextlib

import asyncpg

from ..common import setup_connection
from ..pg import server_settings
from ..restore import RestoreIo, RestoreParams, restore
from .common import open_bytes_read


async def restore_main(args):
    params = RestoreParams(
        include_schema=args.include_schema,
        parallelism=args.jobs,
        transaction=args.transaction,
    )

    async with asyncpg.create_pool(
        init=_init_connection,
        min_size=0,
        max_size=args.jobs,
        max_inactive_connection_lifetime=10,
        server_settings=server_settings(),
        statement_cache_size=0,
    ) as pool:
        io = RestoreIo(
            conn=lambda: pool.acquire(), input=lambda: open_bytes_read(args.input)
        )

        await restore(io, params)


async def _init_connection(conn: asyncpg.Connection):
    await setup_connection(conn)
