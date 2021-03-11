import contextlib
import json
import logging
import sys

import psycopg2

from ..dump import dump
from ..formats.schema import SCHEMA_VALIDATOR
from ..formats.schema import Root as RootConfig
from ..formats.schema import Schema as SchemaConfig
from ..pg import connection, export_snapshot, freeze_transaction, transaction
from .common import open_bytes_write, open_str_read


def dump_main(args):
    roots = [
        RootConfig(condition=condition, table=table) for table, condition in args.roots
    ]

    with open_str_read(args.schema) as schema_file, open_bytes_write(
        args.output
    ) as output, connection("") as conn, transaction(conn) as cur:
        freeze_transaction(cur)
        if args.parallelism != 1:
            snapshot = export_snapshot(cur)
            logging.info("Running at snapshot %s", snapshot)
        else:
            snapshot = None

        @contextlib.contextmanager
        def pg_manager():
            if snapshot is None:
                yield contextlib.nullcontext(cur)
                return
            with connection("") as conn, transaction(conn) as cur2:
                freeze_transaction(cur2, snapshot=snapshot)
                yield contextlib.nullcontext(cur2)

        dump(schema_file, roots, 1, pg_manager, output)
