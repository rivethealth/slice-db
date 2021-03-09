from __future__ import annotations

import codecs
import collections
import dataclasses
import enum
import json
import logging
import time
import typing
import zipfile

import psycopg2.sql as sql

from .slice import MANIFEST_NAME, SCHEMA_NAME, table_name
from .data_db import get_data, get_table_condition, traverse_reference
from .formats.manifest import Manifest
from .formats.manifest import Table as TableManifest
from .model import Reference, ReferenceDirection, Root, Schema, Table
from .pg import Tid
from .util.work import WorkerRunner, WorkerThread

UTF8_WRITER: codecs.StreamWriter = codecs.getwriter("utf-8")

def dump_all(
    schema: Schema,
    schema_json,
    roots: typing.List[Root],
    parallelism: int,
    cur_resource,
    file,
):
    with zipfile.ZipFile(file, "w", compression=zipfile.ZIP_DEFLATED) as zip:
        with zip.open(SCHEMA_NAME, "w") as entry:
            json.dump(schema_json, UTF8_WRITER(entry), sort_keys=True, indent=2)

        ids_result = find_rows(roots, parallelism, cur_resource)

        manifest = Manifest(
            tables=
                    [
                        TableManifest(id=table_id, row_count=len(ids))
                        for table_id, ids in ids_result.tables.items()
                    ]
        )
        with zip.open(MANIFEST_NAME, "w") as entry:
            json.dump(Manifest.schema().dump(manifest), UTF8_WRITER(entry))

        dump_data(ids_result, schema, parallelism, cur_resource, zip)


def find_rows(roots: typing.List[Root], parallelism: int, cur_resource):
    logging.info("Finding rows")
    start = time.perf_counter()
    result = IdsResult()
    worker = IdWorker(result)
    runner = WorkerRunner(parallelism, worker.process_item, cur_resource)
    runner.run(
        [
            TableConditionIdsItem(table=root.table, condition=root.condition)
            for root in roots
        ]
    )
    end = time.perf_counter()
    logging.info("Found rows (%.3fs)", end - start)
    return result


def dump_data(
    ids_result: IdsResult,
    schema: Schema,
    parallelism: int,
    cur_resource,
    zip: zipfile.ZipFile,
):
    logging.info("Dumping rows")
    start = time.perf_counter()
    with cur_resource() as cur:
        for table_id, ids in ids_result.tables.items():
            with zip.open(table_name(table_id), "w") as entry:
                get_data(cur, schema.get_table(table_id), list(ids), entry)
    end = time.perf_counter()
    logging.info("Dumped rows (%.3fs)", end - start)


@dataclasses.dataclass
class DataItem:
    table: Table
    ids: typing.List[Tid]


class IdsResult:
    tables: typing.DefaultDict[str, typing.Set[Tid]]

    def __init__(self):
        self.tables = collections.defaultdict(lambda: set())

    def add(self, table_id: str, ids: typing.List[Tid]):
        """
        Add IDs and return list of newly added IDs
        """
        table_ids = self.tables[table_id]
        new_ids = []
        for id_ in ids:
            if id_ in table_ids:
                continue
            table_ids.add(id_)
            new_ids.append(id_)
        return new_ids


@dataclasses.dataclass
class TableConditionIdsItem:
    table: Table
    condition: str


@dataclasses.dataclass
class ReferenceIdsItem:
    ids: typing.List[Tid]
    reference: Reference
    direction: ReferenceDirection


class IdWorker:
    def __init__(self, result: IdsResult):
        self._result = result

    def process_item(
        self, item: typing.Union[TableConditionIdsItem, ReferenceIdsItem], cur
    ):
        if isinstance(item, TableConditionIdsItem):
            ids = get_table_condition(cur, item.table, item.condition)
            if not ids:
                return
            ids = self._result.add(item.table.id, ids)
            yield from self._table_items(item.table, ids)
        elif isinstance(item, ReferenceIdsItem):
            ids = traverse_reference(cur, item.reference, item.direction, item.ids)
            if not ids:
                return
            if item.direction == ReferenceDirection.FORWARD:
                to_table = item.reference.reference_table
            elif item.direction == ReferenceDirection.REVERSE:
                to_table = item.reference.table
            ids = self._result.add(to_table.id, ids)
            yield from self._table_items(to_table, ids, reference_item=item)

    def _table_items(
        self,
        table: Table,
        ids: typing.List[Tid],
        reference_item: ReferenceIdsItem = None,
    ):
        if not ids:
            return
        for reference in table.references:
            if (
                reference_item is not None
                and reference is reference_item.reference
                and reference_item.direction == ReferenceDirection.REVERSE
            ):
                continue
            yield ReferenceIdsItem(
                ids=ids, reference=reference, direction=ReferenceDirection.FORWARD
            )
        for reference in table.reverse_references:
            if (
                reference_item is not None
                and reference is reference_item.reference
                and reference_item.direction == ReferenceDirection.FORWARD
            ):
                continue
            yield ReferenceIdsItem(
                ids=ids, reference=reference, direction=ReferenceDirection.REVERSE
            )
