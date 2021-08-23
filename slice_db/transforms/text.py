from __future__ import annotations

import enum
import re
import string
import typing
import unicodedata

import pyffx

from ..transform import Transform, TransformContext, Transformer
from .common import create_random


class AlphanumericTransform(Transform):
    def create(self, context: TransformContext, pepper: bytes, config):
        if config is None:
            config = {}
        return _AlphanumericTransformer(
            unique=config.get("unique", False), pepper=pepper
        )


class _AlphanumericTransformer(Transformer):
    def __init__(self, unique: bool, pepper: bytes):
        self._unique = unique
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if text is None:
            return None

        if self._unique:
            return self._transform_unique(text)

        return self._transform(text)

    def _transform(self, text: str):
        rnd = create_random(text.upper().encode("utf-8") + self._pepper)
        result = "".join(self._replace(rnd, c) for c in text)
        return result

    def _transform_unique(self, text: str):
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

        c = pyffx.String(self._pepper, alphabet=alphabet, length=len(text))
        text = "".join(
            c if c in alphabet else alphabet[ord(c) % len(alphabet)] for c in text
        )
        return c.encrypt(text)

    def _replace(self, rnd, c):
        category = Char.char_category(c)
        if category is CharCategory.UPPERCASE:
            return chr(rnd.randint(ord("A"), ord("Z")))
        if category is CharCategory.LOWERCASE:
            return chr(rnd.randint(ord("a"), ord("z")))
        if category is CharCategory.NUMBER:
            return chr(rnd.randint(ord("0"), ord("9")))
        return c


class Char:
    _LOWERCASE_CATEGORIES = set(["Ll", "Lm", "Lo"])
    _NUMERIC_CATEGORIES = set(["Nd", "Nl", "No"])
    _UPPERCASE_CATEGORIES = set(["Lu", "Lt", "Co", "Cs", "So"])

    _NOT_LETTERS_RE = re.compile("[^a-zA-Z]")

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

    def letters(s: str) -> str:
        return Char._NOT_LETTERS_RE.sub("", s)

    @staticmethod
    def string_categories(s: str) -> typing.Set[CharCategory]:
        return set(Char.char_category(c) for c in s)


class CharCategory(enum.Enum):
    LOWERCASE = enum.auto()
    NUMBER = enum.auto()
    OTHER = enum.auto()
    UPPERCASE = enum.auto()


class WordCase(enum.Enum):
    UPPERCASE = enum.auto()
    LOWERCASE = enum.auto()
    TITLECASE = enum.auto()
    OTHER = enum.auto()


class Word:
    @staticmethod
    def case(s: str):
        if not s:
            return WordCase.OTHER

        categories = [Char.char_category(c) for c in s]
        if all(c == CharCategory.UPPERCASE for c in categories):
            return WordCase.UPPERCASE
        if all(c == CharCategory.LOWERCASE for c in categories):
            return WordCase.LOWERCASE
        if categories[0] == WordCase.UPPERCASE and all(
            c == CharCategory.LOWERCASE for c in categories[1:]
        ):
            return WordCase.TITLECASE
        return WordCase.OTHER

    def apply_case(s: str, case: WordCase):
        if case == WordCase.UPPERCASE:
            return s.upper()
        if case == WordCase.LOWERCASE:
            return s.lower()
        if case == WordCase.TITLECASE:
            return s[0].upper() + s[1:].lower()
        return s


class WordTransform:
    def __init__(self):
        with pkg_resource.open_text("slice_db.data", "word.txt") as f:
            text = f.read()
        self._words = [word for word in text.split("\n") if word]

    def create(self, context: TransformContext, pepper: bytes, config: any):
        return _WordTransformer(self._words, max(self.words.keys()), pepper)


class _WordTransformer:
    def __init__(
        self, words: typing.Dict[str, typing.List[str]], default_length, pepper: bytes
    ):
        self._words = words
        self._default_length = default_length
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if not text:
            return text

        new_text = ""

        random = create_random(text.upper().encode("utf-8") + self._pepper)

        word = ""
        i = 0
        while True:
            try:
                c = text[i]
            except IndexError:
                c = None
            category = c and Char.char_category(c)
            if category == CharCategory.LOWERCASE or category == CharCategory.UPPERCASE:
                word += c
                continue
            if word:
                case = Word.case(word)
                try:
                    words = self._words[len(word)]
                except KeyError:
                    words = self._words[self._default_length]
                new_text += Word.apply_case(random.choice(words), case)
            if category == CharCategory.NUMBER:
                new_text += random.choice(string.ascii_digits)
            else:
                new_text += c

        return new_text
