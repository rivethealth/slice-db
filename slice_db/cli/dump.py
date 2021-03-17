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

    io = DumpIo(
        conn=lambda: connection(""),
        output=lambda: open_bytes_write(args.output),
        schema_file=lambda: open_str_read(args.schema),
    )
    params = DumpParams(
        include_schema=args.include_schema,
        parallelism=args.jobs,
        output_type=output_type,
    )

    dump(roots, io, params)
