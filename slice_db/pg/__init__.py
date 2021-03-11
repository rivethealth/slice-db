import typing
from contextlib import contextmanager

import psycopg2
import psycopg2.sql as sql

Snapshot = str

Tid = str


@contextmanager
def connection(*args, **kwargs):
    conn = psycopg2.connect(*args, **kwargs)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn):
    with conn, conn.cursor() as cur:
        yield cur


def export_snapshot(cur) -> Snapshot:
    cur.execute("SELECT pg_export_snapshot()")
    (snapshot,) = cur.fetchone()
    return snapshot


def freeze_transaction(cur, snapshot=None):
    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    if snapshot is not None:
        cur.execute("SET TRANSACTION SNAPSHOT %s", [snapshot])


def defer_constraints(cur, names: typing.List[typing.List[str]]):
    query = sql.SQL("SET CONSTRAINTS {} DEFERRED").format(
        sql.SQL(", ").join(sql.Identifier(*name) for name in names)
    )
    cur.execute(query)
