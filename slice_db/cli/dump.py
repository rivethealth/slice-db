from ..dump import dump
from ..formats.dump import DumpRoot
from ..pg import connection
from .common import open_bytes_write, open_str_read


def dump_main(args):
    roots = [
        DumpRoot(condition=condition, table=table) for table, condition in args.roots
    ]

    dump(
        lambda: connection(""),
        lambda: open_str_read(args.schema),
        roots,
        args.jobs,
        lambda: open_bytes_write(args.output),
    )
