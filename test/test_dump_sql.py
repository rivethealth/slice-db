import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process


def test_dump_sql(pg_database, snapshot):
    schema_sql = """
        CREATE TABLE parent (
            id int PRIMARY KEY
        );

        CREATE TABLE child (
            id int PRIMARY KEY,
            parent_id int REFERENCES parent (id)
        );
    """

    with temp_file("schema-") as schema_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(schema_sql)

            cur.execute(
                """
                    INSERT INTO parent (id)
                    VALUES (1), (2);

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        with open(schema_file, "w") as f:
            schema_json = {
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
            json.dump(schema_json, f)

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
                "--output-type",
                "sql",
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    DROP TABLE child;

                    DROP TABLE parent;
                """
            )

        with open(output_file) as f:
            print(f.read())

        run_process(
            [
                "psql",
                "-f",
                output_file,
            ],
            env=dict(**os.environ, ON_ERROR_STOP="1"),
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("TABLE parent")
            result = cur.fetchall()
            assert result == [(1,)]

            cur.execute("TABLE child")
            result = cur.fetchall()
            assert result == [(1, 1), (2, 1)]
