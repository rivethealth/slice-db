from __future__ import annotations

import codecs
import importlib
import importlib.resources as pkg_resources
import typing

from .collection.dict import groups
from .formats.transform import TransformInstance, TransformTable
from .pg.copy import COPY_FORMAT


class Transform(typing.Protocol):
    def create(
        self, manager: TransformContext, pepper: bytes, config: any
    ) -> Transform:
        pass


class Transformer(typing.Protocol):
    def transform(self, text: typing.Optional[str]):
        pass


class TransformContext:
    def __init__(self, transformers: typing.Dict[str, Transformer]):
        self._transformers = transformers

    def get_transform(self, name: str) -> Transformer:
        try:
            return self._transformers[name]
        except KeyError:
            raise Exception(f"Transformer {name} does not exist")


class TransformerProvider:
    def __init__(self, pepper: bytes):
        self._pepper = pepper

    def create(
        self, name: str, context: TransformContext, instance: TransformInstance
    ) -> Transformer:
        module = importlib.import_module(instance.module)
        transform = getattr(module, instance.class_)()
        pepper = self._pepper + name.encode("utf-8")
        return transform.create(context, pepper, instance.config)


_UTF8_READ = codecs.getreader("utf-8")
_UTF8_WRITE = codecs.getwriter("utf-8")


class DeferredTransformer(Transformer):
    def __init__(self):
        self._transformer = None

    def init(self, transformer: Transformer):
        self._transformer = transformer

    def transform(self, text: typing.Optional[str]):
        if self._transformer is None:
            raise Exception("Transformer not initialized")
        return self._transformer.transform(text)


class Transforms:
    def __init__(self, transforms: typing.Dict[str, TransformInstance], pepper: bytes):
        deferred = {name: DeferredTransformer() for name in transforms.keys()}
        context = TransformContext(deferred)

        provider = TransformerProvider(pepper)
        self._transforms = {
            name: provider.create(name, context, instance)
            for name, instance in transforms.items()
        }
        for name, transformer in self._transforms.items():
            deferred[name].init(transformer)

    def field(self, name: str):
        return TransformContext(self._transforms).get_transform(name)

    def table(
        self,
        transform_columns: typing.Dict[str, TransformColumn],
        columns: typing.List[str],
    ):
        fields = [
            _Field(
                columns.index(name),
                self._transforms[column],
            )
            for name, column in transform_columns.items()
        ]
        return TableTransformer(fields)


class _Field:
    def __init__(self, index: int, transform: Transform):
        self._index = index
        self._transform = transform

    def apply(self, row):
        field = COPY_FORMAT.parse_field(row[self._index])
        field = self._transform.transform(field)
        row[self._index] = COPY_FORMAT.serialize_field(field)


class TableTransformer:
    def __init__(self, fields: typing.List[_Field]):
        self._fields = fields

    def transform(self, input: typing.TextIO, output: typing.TextIO):
        for line in input:
            row = COPY_FORMAT.parse_raw_row(line[:-1])
            for field in self._fields:
                field.apply(row)
            output.write(COPY_FORMAT.serialize_raw_row(row))
            output.write("\n")

    @staticmethod
    def transform_binary(
        transformer: TableTransformer,
        input: typing.BinaryIO,
        output: typing.BinaryIO,
    ):
        transformer.transform(_UTF8_READ(input), _UTF8_WRITE(output))
