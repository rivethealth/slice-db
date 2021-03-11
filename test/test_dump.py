import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process

def test_dump(pg_database, snapshot):
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
                "references": [
                    {
                        "check": "immediate",
                        "columns": ["parent_id"],
                        "id": "public.child.child_parent_id_fkey",
                        "name": "child_parent_id_fkey",
                        "referenceColumns": ["id"],
                        "referenceTable": "public.parent",
                        "table": "public.child",
                    }
                ],
                "tables": [
                    {
                        "columns": ["id"],
                        "id": "public.parent",
                        "name": "parent",
                        "schema": "public",
                    },
                    {
                        "columns": ["id", "parent_id"],
                        "id": "public.child",
                        "name": "child",
                        "schema": "public",
                    },
                ],
            }
            json.dump(schema_json, f)

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
                output_file
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
                "--slice",
                output_file,
            ]
        )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("TABLE parent")
            result = cur.fetchall()
            assert result == [(1,)]

            cur.execute("TABLE child")
            result = cur.fetchall()
            assert result == [(1,1), (2,1)]
