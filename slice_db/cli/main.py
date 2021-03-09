import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Slice DB",
        fromfile_prefix_chars="@",
        formatter_class=ArgumentFormatter,
    )
    parser.add_argument("--log_level", choices=["json", "yaml"], default="json")
    subparsers = parser.add_subparsers(dest="command")

    dump_parser = subparsers.add_parser("dump", description="Dump data")
    dump_parser.add_argument(
        "--parallelism", default=1, help="Number of workers", type=int
    )
    dump_parser.add_argument("--pepper")
    dump_parser.add_argument("--schema", required=True)
    dump_parser.add_argument("--transform")
    dump_parser.add_argument("--root", action="append", default=[], nargs=2)

    restore_parser = subparsers.add_parser("restore", description="Restore data")
    restore_parser.add_argument("--database", nargs=2)

    schema_parser = subparsers.add_parser("schema", description="Collect schema")

    transform_parer = subparsers.add_parser("transform", description="Transform slice")
    transform_parer.add_argument("--transform", required=True)

    args = parser.parse_args()

    if args.command == "dump":
        from .dump import dump

        dump(args)
    elif args.command == "restore":
        from .restore import restore

        restore(args)
    elif args.command == "schema":
        from .schema import schema

        schema(args)
    elif args.command == "transform":
        from .transform import schema

        transform(args)


class ArgumentFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter
):
    pass
