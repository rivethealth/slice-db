import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
CREATE TABLE directory (
    id int PRIMARY KEY,
    parent_id int REFERENCES directory (id) DEFERRABLE
);
"""

_SCHEMA_JSON = {
    "references": {
        "public.directory.directory_parent_id_fkey": {
            "columns": ["parent_id"],
            "referenceColumns": ["id"],
            "referenceTable": "public.directory",
            "table": "public.directory",
        }
    },
    "tables": {
        "public.directory": {
            "columns": ["id", "parent_id"],
            "name": "directory",
            "schema": "public",
        },
    },
}


def test_defer(pg_database, snapshot):
    with temp_file("schema-") as schema_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

            cur.execute(
                """
                INSERT INTO directory (id, parent_id)
                VALUES (1, NULL), (2, 1), (3, 1);
                """
            )

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        run_process(
            [
                "slicedb",
                "dump",
                "--schema",
                schema_file,
                "--root",
                "public.directory",
                "id = 1",
                "--output",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                SET CONSTRAINTS ALL DEFERRED;

                DELETE FROM directory;
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
            cur.execute("SELECT * FROM directory ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, None), (2, 1), (3, 1)]


def test_defer_cycle(pg_database, snapshot):
    with temp_file("schema-") as schema_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

            cur.execute(
                """
                INSERT INTO directory (id, parent_id)
                VALUES (1, 2), (2, 3), (3, 1);
                """
            )

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        run_process(
            [
                "slicedb",
                "dump",
                "--schema",
                schema_file,
                "--root",
                "public.directory",
                "id = 1",
                "--output",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                SET CONSTRAINTS ALL DEFERRED;

                DELETE FROM directory;
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
            cur.execute("SELECT * FROM directory ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, 2), (2, 3), (3, 1)]
