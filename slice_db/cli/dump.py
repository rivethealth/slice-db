import asyncio
import secrets

import asyncpg

from ..common import setup_connection
from ..dump import DumpIo, DumpParams, OutputType, dump
from ..dump_temp_table import TempTableStrategy
from ..formats.dump import DumpRoot
from ..pg import server_settings, set_tid_codec
from .common import open_bytes_write, open_str_read


async def dump_main(args):
    roots = [
        DumpRoot(condition=condition, table=table) for table, condition in args.roots
    ]

    if args.output_type == "slice":
        output_type = OutputType.SLICE
    elif args.output_type == "sql":
        output_type = OutputType.SQL

    if args.pepper is not None:
        pepper = args.pepper.encode("ascii")
    else:
        pepper = secrets.token_bytes(8)

    pool = await asyncpg.create_pool(
        init=_init_connection,
        max_inactive_connection_lifetime=10,
        max_size=args.jobs + 1,
        min_size=0,
        statement_cache_size=0,
        server_settings=server_settings(),
    )
    try:
        io = DumpIo(
            conn=lambda: pool.acquire(),
            output=lambda: open_bytes_write(args.output),
            schema_file=lambda: open_str_read(args.schema),
            transform_file=args.transform and (lambda: open_str_read(args.transform)),
        )
        if args.temp_tables:
            strategy = TempTableStrategy()
        else:
            raise Exception("--no-temp-tables not supported")
        params = DumpParams(
            include_schema=args.include_schema,
            parallelism=args.jobs,
            output_type=output_type,
            pepper=pepper,
            strategy=strategy,
        )

        await dump(roots, io, params)
    finally:
        try:
            await asyncio.wait_for(pool.close(), 10)
        except asyncio.TimeoutError:
            logging.error("Pool failed to close within 10s")


async def _init_connection(conn):
    await set_tid_codec(conn)
    await setup_connection(conn)
