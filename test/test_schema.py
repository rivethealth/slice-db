import json
import os
import subprocess

from pg import connection, transaction
from process import run_process


def test_schema(pg_database, snapshot):
    with connection("") as conn, transaction(conn) as cur:
        cur.execute(
            """
                    CREATE TABLE parent (
                        id int PRIMARY KEY
                    );

                    CREATE TABLE child (
                        id int PRIMARY KEY,
                        parent_id int REFERENCES parent (id)
                    );
                """
        )

    result = run_process(["slicedb", "schema"])
    schema_json = json.loads(result)
    snapshot.assert_match(schema_json)
