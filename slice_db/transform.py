import codecs
import hashlib
import importlib.resources as pkg_resources
import typing

from .collection.dict import groups
from .formats.transform import TransformColumn, TransformTable
from .pg.copy import COPY_FORMAT

T = typing.TypeVar("T")


class Choice:
    def __init__(self, options: typing.List[T]):
        self._options = options

    def choose(self, input: bytes):
        b = hashlib.md5(input).digest()
        i = int.from_bytes(b[0:8], "big") % len(self._options)
        return self._options[i]


class GivenNameTransform:
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "given-name.txt") as f:
            text = f.read()
        options = [name for name in text.split("\n") if name]
        self._choice = Choice(options)

    def transform(self, text: str, pepper: bytes):
        return self._choice.choose(text.encode("utf-8") + pepper)


class SurnameTransform:
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "surname.txt") as f:
            text = f.read()
        options = [name for name in text.split("\n") if name]
        self._choice = Choice(options)

    def transform(self, text: str, pepper: bytes):
        return self._choice.choose(text.encode("utf-8") + pepper)


class GeozipTransform:
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "zip.txt") as f:
            text = f.read()
        options = [int(line) for line in text.split("\n") if line]
        g = groups(options, lambda x: str(x).zfill(5)[0:3])
        self._choices = {geozip: Choice(options) for geozip, options in g.items()}
        self._all_choices = Choice(options)

    def transform(self, zip: str, pepper: bytes):
        geo = zip[0:3]
        if geo not in self._choices:
            result = self._all_choices.choose(zip.encode("utf-8") + pepper)
        else:
            result = self._choices[geo].choose(zip.encode("utf-8") + pepper)
        return str(result).zfill(5)


def create_transform(type):
    if type == "geozip":
        return GeozipTransform()
    if type == "given_name":
        return GivenNameTransform()
    if type == "surname":
        return SurnameTransform()
    raise Exception(f"Invalid transform type {type}")


_UTF8_READ = codecs.getreader("utf-8")
_UTF8_WRITE = codecs.getwriter("utf-8")


class TableTransformer:
    class _Field:
        def __init__(self, index, transform):
            self._index = index
            self._transform = transform

        def apply(self, row, pepper):
            field = COPY_FORMAT.parse_field(row[self._index])
            field = self._transform.transform(field, pepper)
            row[self._index] = COPY_FORMAT.serialize_field(field)

    def __init__(
        self, transform_columns: typing.List[TransformColumn], columns: typing.List[str]
    ):
        self._fields = [
            TableTransformer._Field(
                columns.index(column.name), create_transform(column.transform)
            )
            for column in transform_columns
        ]

    def transform(self, pepper: bytes, input: typing.TextIO, output: typing.TextIO):
        for line in input:
            row = COPY_FORMAT.parse_raw_row(line)
            for field in self._fields:
                field.apply(row, pepper)
            output.write(COPY_FORMAT.serialize_raw_row(row))
            output.write("\n")

    @staticmethod
    def transform_binary(
        transformer, pepper: bytes, input: typing.BinaryIO, output: typing.BinaryIO
    ):
        transformer.transform(pepper, _UTF8_READ(input), _UTF8_WRITE(output))
