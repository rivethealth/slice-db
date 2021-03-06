import importlib.resources
import json
import typing

import dataclasses_json.mm
import jsonschema

from ..resource import ResourceFactory

T = typing.TypeVar("T")


class JsonSchema:
    def __init__(self, schema):
        self._schema = schema

    def validate(self, instance):
        jsonschema.validate(instance, self._schema)


class JsonFormat:
    """
    Parses and serializes JSON, validating via JSONSchema.
    """

    def __init__(self, json_schema: JsonSchema):
        self._json_schema = json_schema

    def load(self, file_fn: ResourceFactory[typing.TextIO]):
        with file_fn() as file:
            instance = json.load(file)
        self._json_schema.validate(instance)
        return instance

    def dump(self, file_fn: ResourceFactory[typing.TextIO], instance, pretty=False):
        self._json_schema.validate(instance)
        with file_fn() as file:
            json.dump(instance, file, sort_keys=True, indent=2 if pretty else None)


class DataJsonFormat(typing.Generic[T]):
    """
    Coverts to dataclasses, while also validating via JSONSchema
    """

    def __init__(self, format: JsonFormat, schema: dataclasses_json.mm.SchemaF):
        self._format = format
        self._dataclass_schema = schema

    def load(self, file_fn: ResourceFactory[typing.TextIO]) -> T:
        instance = self._format.load(file_fn)
        return self._dataclass_schema.load(instance)

    def dump(self, file_fn: ResourceFactory[typing.TextIO], instance: T, pretty=False):
        instance = self._dataclass_schema.dump(instance)
        self._format.dump(file_fn, instance, pretty=pretty)


def package_json_format(package: str, name: str):
    with importlib.resources.open_text(package, name) as f:
        json_schema_data = json.load(f)
    json_schema = JsonSchema(json_schema_data)
    return JsonFormat(json_schema)
