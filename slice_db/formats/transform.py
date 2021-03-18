import dataclasses
import typing

import dataclasses_json

from ..json import DataJsonFormat, package_json_format


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class TransformColumn:
    name: str
    transform: str
    transformParams: typing.Any = dataclasses.field(default_factory=dict)


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class TransformTable:
    id: str
    columns: typing.List[TransformColumn]


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class Transform:
    tables: typing.List[TransformTable]


TRANSFORM_JSON_FORMAT = package_json_format("slice_db.formats", "transform.json")


TRANSFORM_DATA_JSON_FORMAT = DataJsonFormat(TRANSFORM_JSON_FORMAT, Transform.schema())
