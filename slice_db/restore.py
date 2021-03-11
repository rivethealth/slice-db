import dataclasses
import json
import logging
import typing
import zipfile

from .concurrent.graph import GraphRunner
from .data_db import update_data
from .formats.manifest import Manifest
from .formats.manifest import Table as TableManifest
from .formats.schema import Schema as SchemaConfig
from .model import ReferenceCheck, Schema, Table
from .pg import defer_constraints
from .slice import MANIFEST_NAME, SCHEMA_NAME, table_name


def restore(parallelism, cur_factory, file):
    with zipfile.ZipFile(file) as zip:
        with zip.open(SCHEMA_NAME) as file:
            schema_json = json.load(file)
        schema_config = SchemaConfig.schema().load(schema_json)
        schema = Schema(schema_config)

        with zip.open(MANIFEST_NAME) as file:
            manifest_json = json.load(file)
        manifest = Manifest.schema().load(manifest_json)
        manifest_tables = {table.id: table for table in manifest.tables}

        items = {
            id: RestoreItem(table=table, manifest=manifest_tables.get(id))
            for id, table in schema.tables.items()
        }
        restore = Restore(zip)
        runner = GraphRunner(parallelism, restore.process, cur_factory)

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
                if 1 < parallelism or reference.check == ReferenceCheck.IMMEDIATE
            ],
        )


@dataclasses.dataclass
class RestoreItem:
    table: Table
    manifest: typing.Optional[TableManifest]

    def __hash__(self):
        return id(self)


class Restore:
    def __init__(self, zip: zipfile.ZipFile):
        self._zip = zip

    def process(self, item: RestoreItem, transaction):
        if item.manifest is None:
            return

        with transaction as cur, self._zip.open(table_name(item.table.id)) as entry:
            update_data(cur, item.table, item.manifest, entry)
