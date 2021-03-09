import contextlib
import json
import logging
import sys

import psycopg2

from ..dump import dump_all
from ..formats.schema import SCHEMA_VALIDATOR
from ..formats.schema import Schema as SchemaConfig
from ..model import Root, Schema
from ..pg import connection, export_snapshot, freeze_transaction, transaction

def dump(args):
    with open(args.schema, "r") as f:
        schema_json = json.load(f)

    SCHEMA_VALIDATOR.validate(schema_json)

    logging.basicConfig(level=logging.DEBUG)

    schema_config = SchemaConfig.schema().load(schema_json)
    schema = Schema(schema_config)

    roots = []
    for id_, condition in args.root:
        try:
            table = schema.get_table(id_)
        except KeyError:
            raise Exception(f"Root table {id_} does not exist")
        roots.append(Root(table=table, condition=condition))

    with connection("") as conn, transaction(conn) as cur:
        freeze_transaction(cur)
        if args.parallelism == 1:
            snapshot = export_snapshot(cur)
        logging.info("Running at snapshot %s", snapshot)

        @contextlib.contextmanager
        def cur_factory():
            if snapshot is None:
                yield cur
                return
            with connection("") as conn, transaction(conn) as cur:
                freeze_transaction(cur, snapshot=snapshot)
                yield cur

        dump_all(schema, schema_json, roots, 1, cur_factory, sys.stdout.buffer)
