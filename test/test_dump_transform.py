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
            "columns": ["id", "parent_id", "name"],
            "name": "child",
            "schema": "public",
            "sequences": [],
        },
    },
}

_TRANSFORM_JSON = {
    "tables": {"public.child": {"columns": {"name": "given_name"}}},
    "transforms": {"given_name": {"class": "GivenNameTransform"}},
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
            assert result == [(1, 1, "Patsy"), (2, 1, "Myron")]
