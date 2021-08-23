import typing

from ..collection.dict import groups
from ..transform import Transform, TransformContext, Transformer
from .common import create_random
from .text import Char, Word, WordCase

try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources


class AddressLine1Transform(Transform):
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "street.txt") as f:
            text = f.read()
        self._streets = [line for line in text.split("\n") if line]

    def create(self, context: TransformContext, pepper: bytes, config: any):
        return _AddressLine1Transformer(self._streets, pepper)


class _AddressLine1Transformer(Transformer):
    def __init__(self, streets: typing.List[str], pepper: bytes):
        self._streets = streets
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if not text:
            return text

        random = create_random(text.encode("utf-8") + self._pepper)
        street = random.choice(self._streets)
        case = Word.case(Char.letters(text))
        if case != WordCase.TITLECASE:
            street = Word.apply_case(street, case)
        n = random.randint(1, 9999)
        return f"{n} {street}"


class AddressLine2Transform(Transform):
    def create(self, manager: TransformContext, pepper: bytes, config: any):
        return _AddressLine2Transformer(pepper)


class _AddressLine2Transformer(Transformer):
    def __init__(self, pepper: bytes):
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if not text:
            return text

        random = create_random(text.encode("utf-8") + self._pepper)
        n = random.randint(1, 999)
        return f"#{n}"


class CityTransform(Transform):
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "city.txt") as f:
            text = f.read()
        self._cities = [line for line in text.split("\n") if line]

    def create(self, manager: TransformContext, pepper: bytes, config: any):
        return _CityTransformer(self._cities, pepper)


class _CityTransformer(Transformer):
    def __init__(self, cities: typing.List[str], pepper: bytes):
        self._cities = cities
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if not text:
            return text

        random = create_random(text.encode("utf-8") + self._pepper)
        city = random.choice(self._cities)
        case = Word.case(Char.letters(text))
        return city if case == WordCase.TITLECASE else Word.apply_case(city, case)


class GeozipTransform(Transform):
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "zip.txt") as f:
            text = f.read()
        options = [line for line in text.split("\n") if line]
        self._by_geozip = groups(options, lambda x: str(x).zfill(5)[0:3])
        self._all = options

    def create(self, manager: TransformContext, pepper: bytes, config: any):
        return _GeozipTransformer(self._by_geozip, self._all, pepper)


class _GeozipTransformer(Transformer):
    def __init__(
        self,
        by_geozip: typing.Dict[str, typing.List[str]],
        all: typing.List[str],
        pepper: bytes,
    ):
        self._by_geozip = by_geozip
        self._all = all
        self._pepper = pepper

    def transform(self, zip: typing.Optional[str]):
        if zip is None:
            return None

        random = create_random(zip.encode("utf-8") + self._pepper)

        geo = zip[0:3]
        try:
            choices = self._by_geozip[geo]
        except KeyError:
            result = random.choice(self._all_choices)
        else:
            result = random.choice(choices)
        return str(result).zfill(5)


class UsStateTransform(Transform):
    def __init__(self):
        with pkg_resources.open_text("slice_db.data", "us-state.txt") as f:
            text = f.read()
        self._states = [line for line in text.split("\n") if line]

        with pkg_resources.open_text("slice_db.data", "us-state-abbr.txt") as f:
            text = f.read()
        self._abbr = [line for line in text.split("\n") if line]

    def create(self, manager: TransformContext, pepper: bytes, config: any):
        config = config or {}
        return _UsStateTransformer(
            self._abbr if config.get("abbr", False) else self._states,
            pepper,
        )


class _UsStateTransformer(Transformer):
    def __init__(self, states: typing.List[str], pepper: bytes):
        self._states = states
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if not text:
            return text

        random = create_random(text.upper().encode("utf-8") + self._pepper)
        state = random.choice(self._states)
        case = Word.case(Char.letters(state))
        return Word.apply_case(state, case)
