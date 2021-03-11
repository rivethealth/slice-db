from __future__ import annotations

import dataclasses
import enum
import itertools
import typing

from .formats.schema import ReferenceCheck as ReferenceCheckConfig
from .formats.schema import ReferenceDirection as ReferenceDirectionConfig
from .formats.schema import Schema as ConfigSchema
from .formats.schema import Table as ConfigTable


class ReferenceDirection(enum.Enum):
    FORWARD = enum.auto()
    REVERSE = enum.auto()


class ReferenceCheck(enum.Enum):
    DEFERRABLE = enum.auto()
    IMMEDIATE = enum.auto()


@dataclasses.dataclass
class Root:
    table: Table
    condition: str


@dataclasses.dataclass
class Table:
    id: str
    name: str
    schema: str
    columns: str
    references: typing.List[Reference]
    """References to parent tables"""
    reverse_references: typing.List[Reference]
    """References to child tables"""


@dataclasses.dataclass
class Reference:
    check: typing.Optional[ReferenceCheck]
    directions: typing.List[ReferenceDirection]
    id: str
    table: Table
    columns: typing.List[str]
    reference_table: Table
    reference_columns: typing.List[str]
    deferred: bool = False


class Schema:
    """
    Graph model of schema
    """

    def __init__(self, schema: ConfigSchema):
        self.tables = {}
        for table_config in schema.tables:
            table = Table(
                columns=table_config.columns,
                references=[],
                id=table_config.id,
                name=table_config.name,
                reverse_references=[],
                schema=table_config.schema,
            )
            if table.id in self.tables:
                raise Exception(f"Multiple definitions for table {table.id}")
            self.tables[table.id] = table

        self.references = {}
        for reference_config in schema.references:
            try:
                table = self.tables[reference_config.table]
            except KeyError:
                raise Exception(
                    f"No table {reference_config.table}, needed by reference {reference_config.id}"
                )
            try:
                reference_table = self.tables[reference_config.reference_table]
            except KeyError:
                raise Exception(
                    f"No table {reference_config.reference_table}, needed by reference {reference_config.id}"
                )

            directions = []
            for direction_config in reference_config.directions:
                if direction_config == ReferenceDirectionConfig.FORWARD:
                    directions.append(ReferenceDirection.FORWARD)
                elif direction_config == ReferenceDirectionConfig.REVERSE:
                    directions.append(ReferenceDirection.REVERSE)

            if reference_config.check == ReferenceCheckConfig.DEFERRABLE:
                check = ReferenceCheck.DEFERRABLE
            elif reference_config.check == ReferenceCheckConfig.IMMEDIATE:
                check = ReferenceCheck.IMMEDIATE
            else:
                check = None

            reference = Reference(
                check=check,
                directions=directions,
                id=reference_config.id,
                table=table,
                columns=reference_config.columns,
                reference_table=reference_table,
                reference_columns=reference_config.reference_columns,
            )
            if reference.id in self.references:
                raise Exception(f"Multiple definitions for reference {reference.id}")
            self.references[reference.id] = reference
            table.references.append(reference)
            reference_table.reverse_references.append(reference)

    def get_table(self, id) -> Table:
        return self.tables[id]
