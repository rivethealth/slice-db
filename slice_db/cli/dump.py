import secrets

from ..dump import DumpIo, DumpParams, OutputType, dump
from ..formats.dump import DumpRoot
from ..pg import connection
from .common import open_bytes_write, open_str_read


def dump_main(args):
    roots = [
        DumpRoot(condition=condition, table=table) for table, condition in args.roots
    ]

    if args.output_type == "slice":
        output_type = OutputType.SLICE
    elif args.output_type == "sql":
        output_type = OutputType.SQL

    if args.pepper is not None:
        pepper = args.pepper.encode("ascii")
    else:
        pepper = secrets.token_bytes(8)

    io = DumpIo(
        conn=lambda: connection(""),
        output=lambda: open_bytes_write(args.output),
        schema_file=lambda: open_str_read(args.schema),
        transform_file=args.transform and (lambda: open_str_read(args.transform)),
    )
    params = DumpParams(
        include_schema=args.include_schema,
        parallelism=args.jobs,
        output_type=output_type,
        pepper=pepper,
    )

    dump(roots, io, params)
