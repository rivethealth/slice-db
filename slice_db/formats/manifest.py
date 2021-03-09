import dataclasses
import typing

import dataclasses_json

from ..format import PackageJsonValidator

MANIFEST_VALIDATOR = PackageJsonValidator("slice_db.formats", "manifest.json")


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass(frozen=True)
class Table:
    row_count: int
    id: str


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass(frozen=True)
class Manifest:
    tables: typing.List[Table]
