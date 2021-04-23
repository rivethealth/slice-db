import contextlib

import asyncpg

from ..restore import RestoreIo, RestoreParams, restore
from .common import open_bytes_read


async def restore_main(args):
    params = RestoreParams(
        include_schema=args.include_schema,
        parallelism=args.jobs,
        transaction=args.transaction,
    )

    async with asyncpg.create_pool(
        min_size=0, max_size=args.jobs, max_inactive_connection_lifetime=10
    ) as pool:
        io = RestoreIo(
            conn=lambda: pool.acquire(), input=lambda: open_bytes_read(args.input)
        )

        await restore(io, params)
