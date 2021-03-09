import json
import os
import subprocess
import tempfile

from pg import connection, pg_server, transaction
from process import run_process


def test_dump(snapshot):
    with pg_server("schema"), tempfile.NamedTemporaryFile(
        "w", prefix="slice-db-"
    ) as schema_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    CREATE TABLE parent (
                        id int PRIMARY KEY
                    );

                    INSERT INTO parent (id)
                    VALUES (1), (2);

                    CREATE TABLE child (
                        id int PRIMARY KEY,
                        parent_id int REFERENCES parent (id)
                    );

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        schema_json = {
            "references": [
                {
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
        json.dump(schema_json, schema_file)
        schema_file.flush()

        run_process(
            [
                "slicedb",
                "dump",
                "--schema",
                schema_file.name,
                "--root",
                "public.parent",
                "id = 1",
            ]
        )
