import dataclasses
import enum
import typing

import dataclasses_json

from ..format import PackageJsonValidator

SCHEMA_VALIDATOR = PackageJsonValidator("slice_db.formats", "schema.json")


class ReferenceCheck(enum.Enum):
    IMMEDIATE = "immediate"
    DEFERRABLE = "deferrable"


class ReferenceDirection(enum.Enum):
    FORWARD = "forward"
    REVERSE = "reverse"


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass(frozen=True)
class Reference:
    columns: typing.List[str]
    id: str
    reference_columns: typing.List[str]
    reference_table: str
    table: str
    check: ReferenceCheck = ReferenceCheck.DEFERRABLE
    name: typing.Optional[str] = None
    directions: typing.List[ReferenceDirection] = (
        ReferenceDirection.FORWARD,
        ReferenceDirection.REVERSE,
    )
    deferrable: bool = False


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass(frozen=True)
class Table:
    columns: typing.List[str]
    id: str
    name: str
    schema: typing.Optional[str]


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass(frozen=True)
class Schema:
    references: typing.List[Reference]
    tables: typing.List[Table]


@dataclasses.dataclass(frozen=True)
class Root:
    condition: str
    table: str
