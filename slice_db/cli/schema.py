import asyncpg

from ..formats.dump import DUMP_JSON_FORMAT
from ..pg import connection_manager
from ..schema import query_schema
from .common import open_str_write


async def schema_main(args):
    async with connection_manager(await asyncpg.connect()) as conn:
        schema_json = await query_schema(conn)

    DUMP_JSON_FORMAT.dump(lambda: open_str_write(args.output), schema_json, pretty=True)
