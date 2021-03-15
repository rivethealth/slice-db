import contextlib
import dataclasses
import json
import logging
import time
import typing
import zipfile

import psycopg2.sql as sql

from .concurrent.graph import GraphRunner
from .formats.dump import DumpSchema
from .formats.manifest import Manifest
from .formats.manifest import Table as TableManifest
from .formats.manifest import TableSegment as TableSegmentManifest
from .log import TRACE
from .pg import defer_constraints, transaction
from .slice import SliceReader


@dataclasses.dataclass
class RestoreParams:
    parallelism: int
    transaction: bool


class NoArgs:
    def __init__(self, fn):
        self._fn = fn
        self._resource = None

    def __enter__(self, *args, **kwargs):
        self._resource = self._fn()
        return self._resource.__enter__(*args, **kwargs)

    def __exit__(self, *args, **kwargs):
        self._resource.__exit__(*args, **kwargs)
        self._resource = None


def restore(conn_fn, params, file_fn):
    if params.parallelism > 1 and params.transaction:
        raise Exception("A single transaction must be disabled for parallelism > 1")

    @contextlib.contextmanager
    def cur_factory():
        with conn_fn() as conn:
            if params.transaction:
                with transaction(conn) as cur:
                    yield contextlib.nullcontext(cur)
            else:

                @contextlib.contextmanager
                def pg_transaction():
                    with transaction(conn) as cur:
                        yield cur

                yield NoArgs(pg_transaction)

    with file_fn() as file, SliceReader(file) as slice:
        with slice.open_manifest() as file:
            manifest_json = json.load(file)
        manifest = Manifest.schema().load(manifest_json)
        manifest_tables = {table.id: table for table in manifest.tables}

        items = {
            id: RestoreItem(table=table, manifest=manifest_tables.get(id))
            for id, table in schema._tables.items()
        }
        restore = Restore(zip)
        runner = GraphRunner(params.parallelism, restore.process, cur_factory)

        with cur_factory() as conn:
            with conn as cur:
                constraints = []
                for table_id in manifest_tables.keys():
                    table = schema.get_table(table_id)
                    for reference in table.references:
                        if reference.check != ReferenceCheck.DEFERRABLE:
                            continue
                        if reference.reference_table.id not in manifest_tables:
                            continue
                        if reference.name is None:
                            raise Exception(
                                f"Missing name for deferrable reference {reference.id}"
                            )
                        constraints.append([reference.table.schema, reference.name])
                if constraints:
                    logging.info("Defering %d constraints", len(constraints))
                    defer_constraints(cur, constraints)

        runner.run(
            list(items.values()),
            lambda item: [
                items[reference.reference_table.id]
                for reference in item.table.references
                if 1 < params.parallelism or reference.check == ReferenceCheck.IMMEDIATE
            ],
        )


@dataclasses.dataclass
class RestoreItem:
    table: Table
    manifest: typing.Optional[TableSegmentManifest]

    def __hash__(self):
        return id(self)


class Restore:
    def __init__(self, slice_reader: SliceReader):
        self._slice_reader = slice_reader

    def process(self, item: RestoreItem, transaction):
        if item.manifest is None:
            return

        with transaction as cur, self._slice_reader.open_segment(
            item.table.id
        ) as entry:
            update_data(cur, item.table, item.manifest, entry)


_BUFFER_SIZE = 1024 * 32


def update_data(cur, table: Table, table_manifest: TableManifest, in_):
    logging.log(
        TRACE, f"Restoring %s rows into table %s", table_manifest.row_count, table.id
    )
    start = time.perf_counter()
    cur.copy_from(
        in_,
        sql.Identifier(table.schema, table.name).as_string(cur),
        columns=table.columns,
        size=_BUFFER_SIZE,
    )
    end = time.perf_counter()
    logging.debug(
        f"Restored %s rows in table %s (%.3fs)",
        table_manifest.row_count,
        table.id,
        end - start,
    )


def get_constaints(cur, schema: Schema):
    """
    Query PostgreSQL for constraints between tables
    """
    cur.execute(
        """
            WITH
                "table" AS (
                    SELECT *
                    FROM unnest(%s::string[], %s::string[], %s::string[]) AS t (id, schema, name)
                )
            SELECT
                pn.nspname,
                pc.conname,
                a.id,
                b.id,
                pc.condeferrable
            FROM
                pg_constraint AS pc
                JOIN pg_class AS pc2 ON pc.conrelid = pc2.oid
                JOIN pg_namespace AS pn ON pc2.relnamespace = pn.oid
                JOIN "table" AS a ON (pn.nspname, pc2.relname) = (a.schema, a.name)
                JOIN pg_class AS pc3 ON pc.confrelid = pc3.oid
                JOIN pg_namespace AS pn2 ON pc3.relnamespace = pn2.oid
                JOIN "table" AS b ON (pn2.nspname, pc3.relname) = (b.schema, b.name)
            WHERE pc.contype = 'f'
        """,
        [
            [table.id for table in schema.tables()],
            [table.schema for table in schema.tables()],
            [table.name for table in schema.tables()],
        ],
    )

    for schema, name, table, reference_table, deferrable in cur.fetchall():
        pass

    return schema_json
