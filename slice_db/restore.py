import contextlib
import dataclasses
import itertools
import json
import logging
import time
import typing
import zipfile

import psycopg2.sql as sql

from .collection.dict import groups
from .concurrent.graph import GraphRunner
from .formats.dump import DumpSchema
from .formats.manifest import (
    MANIFEST_DATA_JSON_FORMAT,
    Manifest,
    ManifestTable,
    ManifestTableSegment,
)
from .log import TRACE
from .pg import defer_constraints, transaction
from .resource import NoArgs
from .slice import SliceReader


@dataclasses.dataclass
class RestoreParams:
    include_schema: bool
    parallelism: int
    transaction: bool


def restore(conn_fn, params, file_fn):
    if not params.include_schema and params.parallelism > 1 and params.transaction:
        raise Exception("A single transaction must be disabled for parallelism > 1")

    with file_fn() as file, SliceReader(file) as reader:
        manifest = MANIFEST_DATA_JSON_FORMAT.load(reader.open_manifest)

        items = {
            id: RestoreItem(id=id, table=table) for id, table in manifest.tables.items()
        }
        restore = Restore(reader)

        with conn_fn() as conn, transaction(conn) as cur:
            if params.transaction:

                def cur_factory():
                    return contextlib.nullcontext(contextlib.nullcontext(cur))

            else:

                @contextlib.contextmanager
                def cur_factory():
                    with conn_fn() as conn:
                        yield NoArgs(lambda: transaction(conn))

            runner = GraphRunner(params.parallelism, restore.process, cur_factory)

            if params.include_schema:
                constraints = []
            else:
                constraints = get_constaints(cur, manifest.tables)

            deferrable_constaints = [
                [constraint.schema, constraint.name]
                for constraint in constraints
                if constraint.deferrable
            ]

            if deferrable_constaints:
                logging.info("Deferring %d constraints", len(deferrable_constaints))
                defer_constraints(cur, deferrable_constaints)

            deps = groups(
                (constraint for constraint in constraints if not constraint.deferrable),
                lambda constraint: constraint.table,
            )

            if params.include_schema:
                with cur_factory() as conn, conn as cur:
                    with reader.open_schema("pre-data") as f:
                        schema_sql = f.read()
                    cur.execute(schema_sql)

            runner.run(
                list(items.values()),
                lambda item: [
                    items[foreign_key.reference_table] for foreign_key in deps[item.id]
                ],
            )

            if params.include_schema:
                with cur_factory() as conn, conn as cur:
                    with reader.open_schema("post-data") as f:
                        schema_sql = f.read()
                    cur.execute(schema_sql)


@dataclasses.dataclass
class RestoreItem:
    id: str
    table: ManifestTable

    def __hash__(self):
        return id(self)


class Restore:
    def __init__(self, slice_reader: SliceReader):
        self._slice_reader = slice_reader

    def process(self, item: RestoreItem, transaction):
        with transaction as cur:
            for i, segment in enumerate(item.table.segments):
                with self._slice_reader.open_segment(
                    item.id,
                    i,
                ) as file:
                    update_data(cur, item.id, item.table, i, segment, file)


_BUFFER_SIZE = 1024 * 32


def update_data(
    cur, id: str, table: ManifestTable, index: int, segment: ManifestTableSegment, in_
):
    logging.log(TRACE, f"Restoring %s rows into table %s", segment.row_count, id)
    start = time.perf_counter()
    cur.copy_from(
        in_,
        sql.Identifier(table.schema, table.name).as_string(cur),
        columns=[sql.Identifier(column).as_string(cur) for column in table.columns],
        size=_BUFFER_SIZE,
    )
    end = time.perf_counter()
    logging.debug(
        f"Restored %s rows in table %s (%.3fs)",
        segment.row_count,
        id,
        end - start,
    )


@dataclasses.dataclass
class ForeignKey:
    deferrable: bool
    """Deferrable"""
    name: str
    """Name"""
    schema: str
    """Schema"""
    table: str
    """Table ID"""
    reference_table: str
    """Referenced table ID"""


def get_constaints(
    cur, manifest_tables: typing.Dict[str, ManifestTable]
) -> typing.List[ForeignKey]:
    """
    Query PostgreSQL for constraints between tables
    """
    cur.execute(
        """
            WITH
                "table" AS (
                    SELECT *
                    FROM unnest(%s::text[], %s::text[], %s::text[]) AS t (id, schema, name)
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
            list(manifest_tables.keys()),
            [table.schema for table in manifest_tables.values()],
            [table.name for table in manifest_tables.values()],
        ],
    )

    foreign_keys = []
    for schema, name, table, reference_table, deferrable in cur.fetchall():
        foreign_keys.append(
            ForeignKey(
                deferrable=deferrable,
                name=name,
                reference_table=reference_table,
                schema=schema,
                table=table,
            )
        )

    return foreign_keys
