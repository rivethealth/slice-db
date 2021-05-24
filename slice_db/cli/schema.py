import asyncpg

from ..formats.dump import DUMP_JSON_FORMAT
from ..pg import connection_manager, server_settings
from ..schema import query_schema
from .common import open_str_write


async def schema_main(args):
    conn = await asyncpg.connect(server_settings=server_settings())
    async with connection_manager(conn) as conn:
        schema_json = await query_schema(conn)

    DUMP_JSON_FORMAT.dump(lambda: open_str_write(args.output), schema_json, pretty=True)
