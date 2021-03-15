import contextlib

from ..pg import connection
from ..restore import RestoreParams, restore
from .common import open_bytes_read


def restore_main(args):
    params = RestoreParams(parallelism=args.jobs, transaction=args.transaction)

    restore(lambda: connection(""), params, lambda: open_bytes_read(args.input))
