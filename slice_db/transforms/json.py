import dataclasses
import json
import typing

import jsonpath_ng

from ..formats.transform import TransformInstance
from ..transform import Transform, TransformContext, Transformer


class JsonPathTransform(Transform):
    def create(self, context: TransformContext, pepper: bytes, config):
        parts = [
            _JsonPathPart(
                jsonpath_ng.parse(element["path"]),
                context.get_transform(element["transform"]),
            )
            for element in config
        ]
        return _JsonPathTransformer(parts)


@dataclasses.dataclass
class _JsonPathPart:
    path: jsonpath_ng.JSONPath
    transformer: Transformer


class _JsonPathTransformer(Transformer):
    def __init__(self, parts: typing.List[_JsonPathPart]):
        self._parts = parts

    def transform(self, text: typing.Optional[str]):
        if text is None:
            return text

        value = json.loads(text)

        matches = [part.path.find(value) for part in self._parts]
        for part, ms in zip(self._parts, matches):
            for match in ms:
                if match.value is not None and type(match.value) != str:
                    raise Exception(f"{self._path} is not null or string")
                new_value = part.transformer.transform(match.value)
                match.full_path.update(value, new_value)

        return json.dumps(value, separators=(",", ":"))
