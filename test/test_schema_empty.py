import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE empty (
    );
"""

_SCHEMA_JSON = {
    "references": {},
    "sequences": {},
    "tables": {
        "public.empty": {
            "columns": [],
            "name": "empty",
            "schema": "public",
            "sequences": [],
        },
    },
}


def test_schema_empty(pg_database, snapshot):
    with connection("") as conn, transaction(conn) as cur:
        cur.execute(_SCHEMA_SQL)

    output = run_process(["slicedb", "schema"])

    schema = json.loads(output)

    assert schema == _SCHEMA_JSON
