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
        parent_id int REFERENCES parent (id),
        name text NOT NULL
    );
"""

_SCHEMA_JSON = {
    "references": [
        {
            "columns": ["parent_id"],
            "id": "public.child.child_parent_id_fkey",
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
            "columns": ["id", "parent_id", "name"],
            "id": "public.child",
            "name": "child",
            "schema": "public",
        },
    ],
}

_TRANSFORM_JSON = {
    "tables": [
        {"id": "public.child", "columns": [{"name": "name", "transform": "given_name"}]}
    ]
}


def test_dump_transform(pg_database, snapshot):
    with temp_file("schema-") as schema_file, temp_file(
        "transform-"
    ) as transform_file, temp_file("output-") as output_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

            cur.execute(
                """
                    INSERT INTO parent (id)
                    VALUES (1), (2);

                    INSERT INTO child (id, parent_id, name)
                    VALUES (1, 1, 'John'), (2, 1, 'Sue'), (3, 2, 'Bill');
                """
            )

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        with open(transform_file, "w") as f:
            json.dump(_TRANSFORM_JSON, f)

        run_process(
            [
                "slicedb",
                "dump",
                "--schema",
                schema_file,
                "--transform",
                transform_file,
                "--pepper",
                "abc",
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
            cur.execute("SELECT * FROM parent ORDER BY id")
            result = cur.fetchall()
            assert result == [(1,)]

            cur.execute("SELECT * FROM child ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, 1, "Shellie"), (2, 1, "Vonda")]
