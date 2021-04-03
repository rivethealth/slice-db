import json

from ..dump import Schema, Table
from ..formats.dump import DUMP_DATA_JSON_FORMAT, DumpReferenceDirection, DumpSchema
from .common import open_str_read, open_str_write


def filter_main(args):
    input_config = DUMP_DATA_JSON_FORMAT.load(lambda: open_str_read(args.input))

    if args.subcommand == "children":
        output_config = children(args, input_config)

    DUMP_DATA_JSON_FORMAT.dump(
        lambda: open_str_write(args.output), output_config, pretty=True
    )


def children(args, input_config: DumpSchema):
    schema = Schema(input_config)

    child_ids = set()

    def visit(table: Table):
        if table.id in child_ids:
            return
        child_ids.add(table.id)
        for reference in table.reverse_references:
            if DumpReferenceDirection.REVERSE in reference.directions:
                visit(reference.table)

    for table_id in args.table:
        visit(schema.get_table(table_id))

    for table in schema.tables():
        if table.id in child_ids:
            continue
        for reference in table.reverse_references:
            if reference.table.id in child_ids:
                input_config.references[reference.id].directions.remove(
                    DumpReferenceDirection.REVERSE
                )

    return input_config
