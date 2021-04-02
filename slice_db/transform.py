import codecs
import datetime
import hashlib
import importlib.resources as pkg_resources
import random
import typing
import unicodedata

from .collection.dict import groups
from .formats.transform import TransformColumn, TransformTable
from .pg.copy import COPY_FORMAT

T = typing.TypeVar("T")


def bytes_hash_int(input: bytes) -> int:
    b = hashlib.md5(input).digest()
    return int.from_bytes(b[0:8], "big")


class Choice:
    def __init__(self, options: typing.List[T]):
        self._options = options

    def choose(self, input: bytes):
        i = bytes_hash_int(input) % len(self._options)
        return self._options[i]


class IntRange:
    def __init__(self, min, max):
        self._min = min
        self._max = max

    def value(self, input: bytes):
        x = bytes_hash_int(input) % (self._max - self._min)
        return self._min + x


class AlphanumericTransform:
    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        rnd = random.Random(bytes_hash_int(text.encode("utf-8") + pepper))
        result = "".join(self._replace(rnd, c) for c in text)
        return result

    @staticmethod
    def _replace(rnd, c):
        category = unicodedata.category(c)
        if category in ("Lu", "Lt", "Co", "Cs", "So"):
            return chr(rnd.randint(ord("A"), ord("Z")))
        if category in ("Ll", "Lm", "Lo"):
            return chr(rnd.randint(ord("a"), ord("z")))
        if category in ("Nd", "Nl", "No"):
            return chr(rnd.randint(ord("0"), ord("9")))
        return c


class ConstTransform:
    def __init__(self, value):
        self._value = value

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        return self._value


class GivenNameTransform:
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "given-name.txt") as f:
            text = f.read()
        options = [name for name in text.split("\n") if name]
        self._choice = Choice(options)

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        return self._choice.choose(text.encode("utf-8") + pepper)


class DateYearTransform:
    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        date = datetime.date.fromisoformat(text)
        year = datetime.date(date.year + 1, 1, 1) - datetime.date(date.year, 1, 1)
        range = IntRange(0, year.days)
        days = range.value(text.encode("utf-8") + pepper)
        date = datetime.date(date.year, 1, 1) + datetime.timedelta(days=days)
        return date.isoformat()


class NullTransform:
    def transform(self, text: typing.Optional[str], pepper: bytes):
        return None


class SurnameTransform:
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "surname.txt") as f:
            text = f.read()
        options = [name for name in text.split("\n") if name]
        self._choice = Choice(options)

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        return self._choice.choose(text.encode("utf-8") + pepper)


class GeozipTransform:
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "zip.txt") as f:
            text = f.read()
        options = [int(line) for line in text.split("\n") if line]
        g = groups(options, lambda x: str(x).zfill(5)[0:3])
        self._choices = {geozip: Choice(options) for geozip, options in g.items()}
        self._all_choices = Choice(options)

    def transform(self, zip: typing.Optional[str], pepper: bytes):
        if zip is None:
            return None

        geo = zip[0:3]
        if geo not in self._choices:
            result = self._all_choices.choose(zip.encode("utf-8") + pepper)
        else:
            result = self._choices[geo].choose(zip.encode("utf-8") + pepper)
        return str(result).zfill(5)


def create_transform(type, params):
    if type == "alphanumeric":
        return AlphanumericTransform()
    if type == "const":
        return ConstTransform(params)
    if type == "date_year":
        return DateYearTransform()
    if type == "geozip":
        return GeozipTransform()
    if type == "given_name":
        return GivenNameTransform()
    if type == "null":
        return NullTransform()
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
        self,
        transform_columns: typing.Dict[str, TransformColumn],
        columns: typing.List[str],
    ):
        self._fields = [
            TableTransformer._Field(
                columns.index(name), create_transform(column.transform, column.params)
            )
            for name, column in transform_columns.items()
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
