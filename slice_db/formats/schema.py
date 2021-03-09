import dataclasses
import typing

import dataclasses_json

from ..format import PackageJsonValidator

SCHEMA_VALIDATOR = PackageJsonValidator("slice_db.formats", "schema.json")


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass(frozen=True)
class Reference:
    columns: typing.List[str]
    id: str
    name: str
    reference_columns: typing.List[str]
    reference_table: str
    table: str
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
