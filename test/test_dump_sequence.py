import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE datum (
        id serial PRIMARY KEY,
        color text NOT NULL
    );
"""


def test_dump_sequence(pg_database, snapshot):

    with temp_file("schema-") as schema_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

            cur.execute(
                """
                    INSERT INTO datum (color) VALUES ('blue'), ('yellow'), ('red')
                """
            )

        result = run_process(["slicedb", "schema"])
        schema_json = json.loads(result)

        with open(schema_file, "w") as f:
            json.dump(schema_json, f)

        run_process(
            [
                "slicedb",
                "dump",
                "--schema",
                schema_file,
                "--root",
                "public.datum",
                "color = 'blue'",
                "--output",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    DELETE FROM datum;

                    SELECT setval('datum_id_seq', 1, false);
                """
            )

        run_process(
            [
                "slicedb",
                "restore",
                "--input",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("TABLE datum")
            result = cur.fetchall()
            assert result == [(1, "blue")]

            cur.execute("SELECT last_value FROM datum_id_seq")
            result = cur.fetchone()
            assert result == (3,)
