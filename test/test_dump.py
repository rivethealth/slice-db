import json
import os
import subprocess

from file import temp_file
from pg import connection, transaction
from process import run_process

from slice_db.dump import _filter_psql_commands

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


def test_dump_no_temp_tables(pg_database, snapshot):
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
                "--no-table-tables",
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


def test_dump_no_temp_tables_readonly(pg_database, snapshot):
    """
    --no-table-tables must work against a read-only connection (e.g. a
    physical replica), since that's the whole reason it exists: the default
    strategy stages discovered ids in a session temp table, which a
    replica rejects.
    """
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

            cur.execute("DROP ROLE IF EXISTS test_readonly")
            cur.execute("CREATE ROLE test_readonly LOGIN")
            cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO test_readonly")
            # Server-enforced, unlike PGOPTIONS which asyncpg (not
            # libpq-based) doesn't read from the environment.
            cur.execute(
                "ALTER ROLE test_readonly SET default_transaction_read_only = on"
            )

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        env = dict(os.environ)
        env["PGUSER"] = "test_readonly"

        run_process(
            [
                "slicedb",
                "dump",
                "--no-table-tables",
                "--schema",
                schema_file,
                "--root",
                "public.parent",
                "id = 1",
                "--output",
                output_file,
            ],
            env=env,
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


def test_filter_psql_commands():
    """Test that psql meta-commands are filtered from pg_dump output."""
    text = """-- some comment
\\restrict
CREATE TABLE foo (id int);
\\unrestrict
-- another comment"""

    result = _filter_psql_commands(text)

    assert "\\restrict" not in result
    assert "\\unrestrict" not in result
    assert "CREATE TABLE foo" in result
    assert "-- some comment" in result
    assert "-- another comment" in result


def test_filter_psql_commands_empty():
    """Test filtering empty string."""
    assert _filter_psql_commands("") == ""


def test_filter_psql_commands_no_commands():
    """Test that text without psql commands is unchanged."""
    text = """CREATE TABLE foo (id int);
-- comment
ALTER TABLE foo ADD COLUMN bar text;"""

    result = _filter_psql_commands(text)

    assert result == text