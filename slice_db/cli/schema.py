import json
import sys
from contextlib import contextmanager

import jsonschema
import psycopg2

from ..formats.schema import SCHEMA_VALIDATOR
from ..pg import connection, freeze_transaction, transaction
from ..schema_db import query_schema


def schema_main(args):
    with connection("") as conn, transaction(conn) as cur:
        freeze_transaction(cur)
        schema_json = query_schema(cur)
    SCHEMA_VALIDATOR.validate(schema_json)

    json.dump(schema_json, sys.stdout, sort_keys=True, indent=2)
