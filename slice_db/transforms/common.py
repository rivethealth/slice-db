import hashlib
import random
import typing

from ..transform import Transform, TransformContext, Transformer


def bytes_hash_int(input: bytes) -> int:
    b = hashlib.md5(input).digest()
    return int.from_bytes(b[0:8], "big")


def create_random(bytes):
    return random.Random(bytes_hash_int(bytes))


class ComposeTransform(Transform):
    def create(self, context: TransformContext, config):
        transforms = [context.get_transform(name) for name in config]
        return ComposeTransformer(transforms)


class ComposeTransformer(Transformer):
    def __init__(self, transforms: typing.List[Transformer]):
        self._transforms = transforms

    def transform(self, text: typing.Optional[str]):
        for transform in self._transforms:
            text = transform.transform(text)
        return text


class ConstTransform(Transform):
    def create(self, context, pepper, params):
        return _ConstTransformer(params)


class _ConstTransformer:
    def __init__(self, value):
        self._value = value

    def transform(self, text: typing.Optional[str]):
        if text is None:
            return None

        return self._value

class IncrementingConstTransform(Transform):
    def create(self, context, pepper, config):
        return _IncrementingConstTransform(config)


class _IncrementingConstTransform:
    def __init__(self, config):
        self._count = 0
        self._value = config.get("value")
        self._exclude = config.get("exclude")

    def transform(self, text: typing.Optional[str]):
        if not text:
            return text

        if self._exclude is not None and self._exclude in text:
            return text

        self._count = self._count+1
        return self._value + ' ' + str(self._count)


class NullTransform(Transform):
    def create(self, context, pepper, params):
        return _NullTransformer()


class _NullTransformer(Transform):
    def transform(self, text: typing.Optional[str]):
        return None
