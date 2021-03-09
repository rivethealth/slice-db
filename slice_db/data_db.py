import logging
import time
import typing

import psycopg2.sql as sql

from .formats.manifest import Table as TableManifest
from .model import Reference, ReferenceDirection, Table
from .pg import Tid


def get_data(cur, table: Table, ids: typing.List[Tid], out):
    logging.debug(f"Dumping %s rows from table %s", len(ids), table.id)
    start = time.perf_counter()
    query = sql.SQL(
        """
            COPY (
                SELECT {}
                FROM {}
                WHERE ctid = ANY({}::tid[])
            )
            TO STDOUT
        """
    ).format(
        sql.SQL(", ").join([sql.Identifier(column) for column in table.columns]),
        sql.Identifier(table.schema, table.name),
        sql.Literal(ids),
    )
    cur.copy_expert(query, out)
    end = time.perf_counter()
    logging.debug(
        f"Dumped %s rows from table %s (%.3fs)", len(ids), table.id, end - start
    )


def update_data(cur, table: Table, table_manifest: TableManifest, in_):
    logging.debug(
        f"Restoring %s rows into table %s", table_manifest.row_count, table.id
    )
    start = time.perf_counter()
    cur.copy_from(in_, sql.Identifier(table.schema, table.name), columns=table.columns)
    end = time.perf_counter()
    logging.debug(
        f"Restored %s rows in table %s (%.3fs)",
        table_manifest.row_count,
        table.id,
        end - start,
    )


def get_table_condition(cur, table: Table, condition: str) -> typing.List[Tid]:
    logging.debug(f"Finding rows from table %s", table.id)
    start = time.perf_counter()
    query = sql.SQL(
        """
            SELECT ctid
            FROM {}
            WHERE {}
        """
    ).format(
        sql.Identifier(table.schema, table.name),
        sql.SQL(condition),
    )
    cur.execute(query)
    ids = [id_ for id_, in cur.fetchall()]
    end = time.perf_counter()
    logging.debug(
        f"Found %s rows from table %s (%.3fs)", len(ids), table.id, end - start
    )
    return ids


def traverse_reference(
    cur, reference: Reference, direction: ReferenceDirection, ids: typing.List[Tid]
) -> typing.List[Tid]:
    if direction == ReferenceDirection.FORWARD:
        from_columns = reference.columns
        from_table = reference.table
        to_columns = reference.reference_columns
        to_table = reference.reference_table
    elif direction == ReferenceDirection.REVERSE:
        from_columns = reference.reference_columns
        from_table = reference.reference_table
        to_columns = reference.columns
        to_table = reference.table

    logging.debug(
        f"Finding rows from table %s using %s",
        to_table.id,
        reference.id,
    )
    start = time.perf_counter()
    query = sql.SQL(
        """
            SELECT b.ctid
            FROM {} AS a
                JOIN {} AS b ON ({}) = ({})
            WHERE a.ctid = ANY(%s::tid[])
        """
    ).format(
        sql.Identifier(from_table.schema, from_table.name),
        sql.Identifier(to_table.schema, to_table.name),
        sql.SQL(", ").join([sql.Identifier("a", name) for name in from_columns]),
        sql.SQL(", ").join([sql.Identifier("b", name) for name in to_columns]),
    )
    cur.execute(query, [ids])
    ids = [id_ for id_, in cur.fetchall()]
    end = time.perf_counter()
    logging.debug(
        f"Found %s rows from table %s using %s (%.3fs)",
        len(ids),
        to_table.id,
        reference.id,
        end - start,
    )
    return ids
