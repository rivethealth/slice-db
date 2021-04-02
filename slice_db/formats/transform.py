import dataclasses
import typing

import dataclasses_json

from ..json import DataJsonFormat, package_json_format


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class TransformColumn:
    transform: str
    params: typing.Any = None


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class TransformTable:
    columns: typing.Dict[str, TransformColumn]


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class Transform:
    tables: typing.Dict[str, TransformTable]


TRANSFORM_JSON_FORMAT = package_json_format("slice_db.formats", "transform.json")


TRANSFORM_DATA_JSON_FORMAT = DataJsonFormat(TRANSFORM_JSON_FORMAT, Transform.schema())
