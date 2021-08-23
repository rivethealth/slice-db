import dataclasses
import typing

import dataclasses_json

from ..json import DataJsonFormat, package_json_format


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class TransformInstance:
    class_: str = dataclasses.field(
        metadata=dataclasses_json.config(field_name="class")
    )
    module: str = "slice_db.transforms"
    config: typing.Any = None


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class TransformTable:
    columns: typing.Dict[str, str]


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class Transform:
    tables: typing.Dict[str, TransformTable]
    transforms: typing.Dict[str, TransformInstance]


TRANSFORM_JSON_FORMAT = package_json_format("slice_db.formats", "transform.json")


TRANSFORM_DATA_JSON_FORMAT = DataJsonFormat(TRANSFORM_JSON_FORMAT, Transform.schema())
