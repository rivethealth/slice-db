import json

from ..formats.schema import ReferenceDirection as ReferenceDirectionConfig
from ..formats.schema import Schema as SchemaConfig
from ..model import ReferenceDirection, Schema, Table
from .common import open_str_read, open_str_write


def filter_main(args):
    with open_str_read(args.input) as f:
        schema_json = json.load(f)
    input_config = SchemaConfig.schema().load(schema_json)

    if args.subcommand == "children":
        output_config = children(args, input_config)

    output_json = SchemaConfig.schema().dump(output_config)
    with open_str_write(args.output) as f:
        json.dump(output_json, f, sort_keys=True, indent=2)


def children(args, input_config: SchemaConfig):
    schema = Schema(input_config)

    references = {reference.id: reference for reference in input_config.references}

    child_ids = set()

    def visit(table: Table):
        if table.id in child_ids:
            return
        child_ids.add(table.id)
        for reference in table.reverse_references:
            if ReferenceDirection.REVERSE in reference.directions:
                visit(reference.table)

    for table_id in args.table:
        visit(schema.get_table(table_id))

    for table in schema._tables.values():
        if table.id in child_ids:
            continue
        for reference in table.reverse_references:
            if reference.table.id in child_ids:
                references[reference.id].directions.remove(
                    ReferenceDirectionConfig.REVERSE
                )

    return input_config