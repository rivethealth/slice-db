import dataclasses

import dataclasses_json

from ..json import DataJsonFormat, package_json_format


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class Transform:
    pass


TRANSFORM_JSON_FORMAT = package_json_format("slice_db.formats", "manifest.json")


TRANSFORM_DATA_JSON_FORMAT = DataJsonFormat(TRANSFORM_JSON_FORMAT, Transform.schema())
