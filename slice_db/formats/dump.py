import dataclasses
import enum
import typing

import dataclasses_json

from ..json import DataJsonFormat, package_json_format


class DumpReferenceDirection(enum.Enum):
    FORWARD = "forward"
    REVERSE = "reverse"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class DumpReference:
    columns: typing.List[str]
    reference_columns: typing.List[str]
    reference_table: str
    table: str
    directions: typing.List[DumpReferenceDirection] = (
        DumpReferenceDirection.FORWARD,
        DumpReferenceDirection.REVERSE,
    )
    deferrable: bool = False


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class DumpSequence:
    schema: str
    name: str


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class DumpTable:
    columns: typing.List[str]
    name: str
    schema: typing.Optional[str]
    sequences: typing.List[str]


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class DumpSchema:
    references: typing.Dict[str, DumpReference]
    sequences: typing.Dict[str, DumpSequence]
    tables: typing.Dict[str, DumpTable]


@dataclasses.dataclass
class DumpRoot:
    table: str
    condition: str


DUMP_JSON_FORMAT = package_json_format("slice_db.formats", "dump.json")


DUMP_DATA_JSON_FORMAT = DataJsonFormat(DUMP_JSON_FORMAT, DumpSchema.schema())
