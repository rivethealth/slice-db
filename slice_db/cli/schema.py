from ..formats.dump import DUMP_JSON_FORMAT
from ..pg import connection, transaction
from ..schema import query_schema
from .common import open_str_write


def schema_main(args):
    with connection("") as conn, transaction(conn) as cur:
        schema_json = query_schema(cur)

    DUMP_JSON_FORMAT.dump(lambda: open_str_write(args.output), schema_json, pretty=True)
