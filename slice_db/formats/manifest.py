import dataclasses
import typing

import dataclasses_json

from ..json import DataJsonFormat, package_json_format


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass()
class ManifestTableSegmentId:
    table_id: str
    index: int


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass()
class ManifestTableSegment:
    row_count: int
    """Number of rows"""


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass()
class ManifestTable:
    columns: typing.List[str]
    """Columns"""
    name: str
    """Name"""
    schema: str
    """Schema"""
    segments: typing.List[ManifestTableSegment]
    """Segments"""


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass()
class Manifest:
    tables: typing.Dict[str, ManifestTable]


MANIFEST_JSON_FORMAT = package_json_format("slice_db.formats", "manifest.json")


MANIFEST_DATA_JSON_FORMAT = DataJsonFormat(MANIFEST_JSON_FORMAT, Manifest.schema())
