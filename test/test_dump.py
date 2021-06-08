import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE parent (
        id int PRIMARY KEY
    );

    CREATE TABLE child (
        id int PRIMARY KEY,
        parent_id int REFERENCES parent (id)
    );
"""

_SCHEMA_JSON = {
    "references": {
        "public.child.child_parent_id_fkey": {
            "columns": ["parent_id"],
            "referenceColumns": ["id"],
            "referenceTable": "public.parent",
            "table": "public.child",
        }
    },
    "sequences": {},
    "tables": {
        "public.parent": {
            "columns": ["id"],
            "name": "parent",
            "schema": "public",
            "sequences": [],
        },
        "public.child": {
            "columns": ["id", "parent_id"],
            "name": "child",
            "schema": "public",
            "sequences": [],
        },
    },
}


def test_dump(pg_database, snapshot):
    with temp_file("schema-") as schema_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

            cur.execute(
                """
                    INSERT INTO parent (id)
                    VALUES (1), (2);

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
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
                "public.parent",
                "id = 1",
                "--output",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    DELETE FROM child;

                    DELETE FROM parent;
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
            cur.execute("TABLE parent")
            result = cur.fetchall()
            assert result == [(1,)]

            cur.execute("TABLE child")
            result = cur.fetchall()
            assert result == [(1, 1), (2, 1)]


def test_dump_schema(pg_database, snapshot):
    with temp_file("schema-") as schema_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

            cur.execute(
                """
                    INSERT INTO parent (id)
                    VALUES (1), (2);

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        run_process(
            [
                "slicedb",
                "dump",
                "--include-schema",
                "--schema",
                schema_file,
                "--root",
                "public.parent",
                "id = 1",
                "--output",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    DROP TABLE child;

                    DROP TABLE parent;
                """
            )

        run_process(
            [
                "slicedb",
                "restore",
                "--include-schema",
                "--input",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("TABLE parent")
            result = cur.fetchall()
            assert result == [(1,)]

            cur.execute("TABLE child")
            result = cur.fetchall()
            assert result == [(1, 1), (2, 1)]
