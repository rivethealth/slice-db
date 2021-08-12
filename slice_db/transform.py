from __future__ import annotations

import codecs
import datetime
import enum
import hashlib
import importlib.resources as pkg_resources
import random
import string
import typing
import unicodedata

import pyffx

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


class CharCategory(enum.Enum):
    LOWERCASE = enum.auto()
    NUMBER = enum.auto()
    OTHER = enum.auto()
    UPPERCASE = enum.auto()


class Char:
    _LOWERCASE_CATEGORIES = set(["Ll", "Lm", "Lo"])
    _NUMERIC_CATEGORIES = set(["Nd", "Nl", "No"])
    _UPPERCASE_CATEGORIES = set(["Lu", "Lt", "Co", "Cs", "So"])

    @staticmethod
    def char_category(c: str) -> CharCategory:
        category = unicodedata.category(c)
        if category in Char._UPPERCASE_CATEGORIES:
            return CharCategory.UPPERCASE
        if category in Char._LOWERCASE_CATEGORIES:
            return CharCategory.LOWERCASE
        if category in Char._NUMERIC_CATEGORIES:
            return CharCategory.NUMBER
        return CharCategory.OTHER

    @staticmethod
    def string_categories(s: str) -> typing.Set[CharCategory]:
        return set(Char.char_category(c) for c in s)


class CaseInsensitive:
    def __init__(self, inner):
        self._inner = inner

    def transform(self, text: str):
        # normalize to uppercase
        transformed = self._inner(text.upper())
        # back to original case
        if len(text) < len(transformed):
            text += text[-1] * (len(transformed) - len(text))
        return "".join(
            c2.lower() if c1.islower() else c2.upper()
            for c1, c2 in zip(text, transformed)
        )


class AlphanumericTransform:
    def __init__(self, case_insensitive, unique):
        self._case_insensitive = case_insensitive
        self._unique = unique

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        fn = lambda text: self._transform(text, pepper)
        if self._case_insensitive:
            return CaseInsensitive(fn).transform(text)
        return fn(text)

    def _transform(self, text: str, pepper: bytes):
        if self._unique:
            return self._transform_unique(text, pepper)

        rnd = random.Random(bytes_hash_int(text.encode("utf-8") + pepper))
        result = "".join(self._replace(rnd, c) for c in text)
        return result

    def _transform_unique(self, text: str, pepper: bytes):
        categories = Char.string_categories(text)
        alphabet = ""
        if CharCategory.UPPERCASE in categories:
            alphabet += string.ascii_uppercase
        if CharCategory.LOWERCASE in categories:
            alphabet += string.ascii_lowercase
        if CharCategory.NUMBER in categories:
            alphabet += string.digits
        if not alphabet:
            alphabet = string.ascii_uppercase + string.ascii_lowercase + string.digits

        c = pyffx.String(pepper, alphabet=alphabet, length=len(text))
        text = "".join(
            c if c in alphabet else alphabet[ord(c) % len(alphabet)] for c in text
        )
        return c.encrypt(text)

    @staticmethod
    def _replace_category(rnd, category: CharCategory):
        if category is CharCategory.UPPERCASE:
            return chr(rnd.randint(ord("A"), ord("Z")))
        if category is CharCategory.LOWERCASE:
            return chr(rnd.randint(ord("a"), ord("z")))
        if category is CharCategory.NUMBER:
            return chr(rnd.randint(ord("0"), ord("9")))

    @staticmethod
    def _replace(rnd, c):
        category = Char.char_category(c)
        if category == CharCategory.OTHER:
            return c
        return AlphanumericTransform._replace_category(rnd, category)


class ConstTransform:
    def __init__(self, value):
        self._value = value

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        return self._value


class GivenNameTransform:
    def __init__(self, case_insensitive: bool):
        with pkg_resources.open_text("slice_db.data", "given-name.txt") as f:
            text = f.read()
        options = [name for name in text.split("\n") if name]
        self._case_insensitive = case_insensitive
        self._choice = Choice(options)

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        fn = lambda text: self._transform(text, pepper)
        if self._case_insensitive:
            return CaseInsensitive(fn).transform(text)
        return fn(text)

    def _transform(self, text: str, pepper: bytes):
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
    def __init__(self, case_insensitive: bool):
        with pkg_resources.open_text("slice_db.data", "surname.txt") as f:
            text = f.read()
        options = [name for name in text.split("\n") if name]
        self._case_insensitive = case_insensitive
        self._choice = Choice(options)

    def transform(self, text: typing.Optional[str], pepper: bytes):
        if text is None:
            return None

        fn = lambda text: self._transform(text, pepper)
        if self._case_insensitive:
            return CaseInsensitive(fn).transform(text)
        return fn(text)

    def _transform(self, text: str, pepper: bytes):
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
        if params is None:
            params = {}
        return AlphanumericTransform(
            case_insensitive=params.get("caseInsensitive", False),
            unique=params.get("unique", False),
        )
    if type == "const":
        return ConstTransform(params)
    if type == "date_year":
        return DateYearTransform()
    if type == "geozip":
        return GeozipTransform()
    if type == "given_name":
        if params is None:
            params = {}
        return GivenNameTransform(case_insensitive=params.get("caseInsensitive", False))
    if type == "null":
        return NullTransform()
    if type == "surname":
        if params is None:
            params = {}
        return SurnameTransform(case_insensitive=params.get("caseInsensitive", False))
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
            row = COPY_FORMAT.parse_raw_row(line[:-1])
            for field in self._fields:
                field.apply(row, pepper)
            output.write(COPY_FORMAT.serialize_raw_row(row))
            output.write("\n")

    @staticmethod
    def transform_binary(
        transformer: TableTransformer,
        pepper: bytes,
        input: typing.BinaryIO,
        output: typing.BinaryIO,
    ):
        transformer.transform(pepper, _UTF8_READ(input), _UTF8_WRITE(output))
