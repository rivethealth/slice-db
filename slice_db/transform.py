import hashlib
import importlib.resources as pkg_resources
import typing

from .collection.dict import groups

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
