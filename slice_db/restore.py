import typing
import zipfile

from .model import Schema, Table


def restore_all(
    schema: Schema,
    cur,
    zip: zipfile.ZipFile,
):
    pass


def restore_table(
    table: Table,
    zip: zipfile.ZipFile,
    restoring: typing.Set[str],
    restored: typing.Set[str],
):
    # with zip.open(table.id):
    pass
