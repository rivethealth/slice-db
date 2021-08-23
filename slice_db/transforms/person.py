import typing

from ..transform import Transform, Transformer
from .common import create_random
from .text import Char, Word, WordCase

try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources


class GivenNameTransform(Transform):
    def create(self, manager, pepper: bytes, params):
        with pkg_resources.open_text("slice_db.data", "given-name.txt") as f:
            text = f.read()
        names = [name for name in text.split("\n") if name]
        return _NameTransformer(names, pepper)


class SurnameTransform(Transform):
    def create(self, manager, pepper: bytes, params):
        with pkg_resources.open_text("slice_db.data", "surname.txt") as f:
            text = f.read()
        names = [name for name in text.split("\n") if name]
        return _NameTransformer(names, pepper)


class _NameTransformer(Transformer):
    def __init__(self, names: typing.List[str], pepper: bytes):
        self._names = names
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if text is None:
            return None

        random = create_random(text.upper().encode("utf-8") + self._pepper)
        name = random.choice(self._names)
        case = Word.case(Char.letters(text))
        return name if case == WordCase.TITLECASE else Word.apply_case(name, case)
