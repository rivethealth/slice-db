from .dump import Dump, DumpReferenceDirection, DumpStrategy, Table, TableSegment


def root_items(self, roots: typing.List[Root]):
    return [
        RootItem(table=root.table, condition=root.condition, dump=self)
        for root in roots
    ]


async def next(
    self,
    segment: TableSegment,
    source: typing.List[TableSegment],
    reference_item: ReferenceItem = None,
):
    items: typing.List[ReferenceItem] = []

    """
    Create items for table
    """
    for reference in segment.table.references:
        if DumpReferenceDirection.FORWARD not in reference.directions:
            continue
        if (
            reference_item is not None
            and reference is reference_item.reference
            and reference_item.direction == DumpReferenceDirection.REVERSE
        ):
            continue
        items.append(
            ReferenceItem(
                dump=self,
                segment=segment,
                reference=reference,
                direction=DumpReferenceDirection.FORWARD,
                source=source,
            )
        )
    for reference in segment.table.reverse_references:
        if DumpReferenceDirection.REVERSE not in reference.directions:
            continue
        if (
            reference_item is not None
            and reference is reference_item.reference
            and reference_item.direction == DumpReferenceDirection.FORWARD
        ):
            continue
        items.append(
            ReferenceItem(
                dump=self,
                segment=segment,
                reference=reference,
                direction=DumpReferenceDirection.REVERSE,
                source=source,
            )
        )


@dataclasses.dataclass
class RootItem:
    table: Table
    condition: str
    dump: _Dump

    async def __call__(self):
        task: asyncio.Task = None

        try:
            async with self.dump.lock:
                async with self.dump.conn_factory() as conn:
                    segment = await _discover_table_condition(
                        conn, self.table, self.condition, self.dump.result
                    )
                    if segment is None:
                        return

                task = asyncio.create_task(self.dump.next(segment, []))

                await self.dump.dump_segment(self.table, segment)
        except:
            if task is not None:
                task.cancel()
                asyncio.wait([task])
            raise

        if task is not None:
            await task


@dataclasses.dataclass
class ReferenceItem:
    direction: DumpReferenceDirection
    reference: Reference
    segment: TableSegment
    dump: _Dump
    source: typing.List[TableSegment]

    async def __call__(self):
        task: asyncio.Task = None

        try:
            async with self.dump.lock:
                async with self.dump.conn_factory() as conn:
                    segment = await _discover_reference(
                        conn,
                        self.reference,
                        self.direction,
                        self.segment,
                        self.source,
                        self.dump.result,
                    )
                    if segment is None:
                        return

                if self.direction == DumpReferenceDirection.FORWARD:
                    to_table = self.reference.reference_table
                elif self.direction == DumpReferenceDirection.REVERSE:
                    to_table = self.reference.table

                task = asyncio.create_task(
                    self.dump.next(segment, self.source, reference_item=self)
                )

                await self.dump.dump_segment(to_table, segment)
        except:
            if task is not None:
                task.cancel()
                asyncio.wait([task])
            raise

        if task is not None:
            await task


@dataclasses.dataclass
class SchemaItem:
    section: str
    output: _SliceOutput

    async def __call__(self):
        with tempfile.TemporaryFile() as tmp:
            await _pg_dump_section(self.section, tmp)
            tmp.seek(0)
            async with self.output.open_schema(self.section) as f:
                await to_thread(shutil.copyfileobj, tmp, f)


@dataclasses.dataclass
class Root:
    """Root"""

    table: Table
    """Table"""
    condition: str
    """Condition"""


@dataclasses.dataclass
class Table:
    """Table"""

    id: str
    """ID"""
    name: str
    """Name"""
    schema: str
    """Schema"""
    columns: typing.List[str]
    """Columns"""
    references: typing.List[Reference]
    """References to parent tables"""
    reverse_references: typing.List[Reference]
    """References to child tables"""

    @property
    def columns_sql(self):
        return [SqlId(column) for column in self.columns]

    @property
    def sql(self):
        return SqlObject(SqlId(self.schema), SqlId(self.name))


@dataclasses.dataclass
class Reference:
    """Reference"""

    directions: typing.List[DumpReferenceDirection]
    """Directions"""
    id: str
    """ID"""
    table: Table
    """Table"""
    columns: typing.List[str]
    """Columns"""
    reference_table: Table
    """Reference columns"""
    reference_columns: typing.List[str]


@dataclasses.dataclass
class TableSegment:
    index: int
    row_ids: numpy.ndarray
    table: Table


class Schema:
    """
    Graph model of schema
    """

    def __init__(self, schema: DumpSchema):
        self._tables = {}
        for id, table_config in schema.tables.items():
            table = Table(
                columns=table_config.columns,
                references=[],
                id=id,
                name=table_config.name,
                reverse_references=[],
                schema=table_config.schema,
            )
            self._tables[table.id] = table

        self._references = {}
        for id, reference_config in schema.references.items():
            try:
                table = self._tables[reference_config.table]
            except KeyError:
                raise Exception(
                    f"No table {reference_config.table}, needed by reference {reference_config.id}"
                )
            try:
                reference_table = self._tables[reference_config.reference_table]
            except KeyError:
                raise Exception(
                    f"No table {reference_config.reference_table}, needed by reference {reference_config.id}"
                )

            reference = Reference(
                directions=reference_config.directions,
                id=id,
                table=table,
                columns=reference_config.columns,
                reference_table=reference_table,
                reference_columns=reference_config.reference_columns,
            )
            self._references[id] = reference
            table.references.append(reference)
            reference_table.reverse_references.append(reference)

    def get_table(self, id) -> Table:
        """
        Get table by ID
        """
        return self._tables[id]

    def tables(self):
        """
        Iterable of tables
        """
        return self._tables.values()


async def _dump_data(
    conn: asyncpg.Connection, table: Table, ids: typing.List[int], out: typing.BinaryIO
):
    """
    Dump data
    """

    logging.log(TRACE, f"Dumping %s rows from table %s", len(ids), table.id)
    start = time.perf_counter()

    await conn.execute("CREATE TEMP TABLE _slicedb (_ctid tid) ON COMMIT DROP")
    await conn.copy_records_to_table(
        "_slicedb", records=[[int_to_tid(id)] for id in ids], schema_name="pg_temp"
    )
    query = f"SELECT {sql_list(table.columns_sql)} FROM {table.sql} AS t JOIN _slicedb AS s ON t.ctid = s._ctid"
    await conn.copy_from_query(query, output=functools.partial(to_thread, out.write))
    await conn.execute("DROP TABLE pg_temp._slicedb")
    end = time.perf_counter()
    logging.debug(
        f"Dumped %s rows from table %s (%.3fs)", len(ids), table.id, end - start
    )


async def _discover_table_condition(
    conn: asyncpg.Connection, table: Table, condition: str, result: _DiscoveryResult
) -> typing.List[Tid]:
    """
    Discover, using root
    """
    logging.log(TRACE, f"Finding rows from table %s", table.id)
    start = time.perf_counter()
    query = f"SELECT ctid FROM {table.sql} WHERE {condition}"
    found_ids = [tid_to_int(id_) for id_, in await conn.fetch(query)]
    segment = result.add(table, found_ids) if found_ids else None
    end = time.perf_counter()
    if segment is None:
        logging.debug(
            f"Found no rows in table %s (%.3fs)",
            table.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s (%.3fs)",
            len(found_ids),
            len(segment.row_ids),
            segment.table.id,
            segment.index,
            end - start,
        )
    end = time.perf_counter()
    return segment


async def _discover_reference(
    conn: asyncpg.Connection,
    reference: Reference,
    direction: DumpReferenceDirection,
    segment: TableSegment,
    source: typing.List[TableSegment],
    result,
) -> typing.List[Tid]:
    """
    Discover, using reference
    """
    if direction == DumpReferenceDirection.FORWARD:
        from_columns = reference.columns
        from_table = reference.table
        to_columns = reference.reference_columns
        to_table = reference.reference_table
    elif direction == DumpReferenceDirection.REVERSE:
        from_columns = reference.reference_columns
        from_table = reference.reference_table
        to_columns = reference.columns
        to_table = reference.table

    logging.log(
        TRACE,
        f"Finding rows from table %s using %s/%s via %s",
        to_table.id,
        segment.table.id,
        segment.index,
        reference.id,
    )
    start = time.perf_counter()
    from_expr = sql_list([SqlObject(SqlId("a"), SqlId(name)) for name in from_columns])
    to_expr = sql_list([SqlObject(SqlId("b"), SqlId(name)) for name in to_columns])
    # assumption: add reference has a unique value on the reference table
    # therefore, no need to dedup child records since they will be had by only one parent
    distinct = "DISTINCT" if direction == DumpReferenceDirection.FORWARD else ""
    query = f"""
        SELECT {distinct} b.ctid
        FROM {from_table.sql} AS a
            JOIN {to_table.sql} AS b ON ({from_expr}) = ({to_expr})
        WHERE a.ctid = ANY($1::tid[])
    """
    tids = [int_to_tid(id) for id in segment.row_ids]
    found_ids = [tid_to_int(id_) for id_, in await conn.fetch(query, [tids])]

    new_segment = (
        result.add(to_table, found_ids, source + [segment]) if found_ids else None
    )
    end = time.perf_counter()
    if new_segment is None:
        logging.debug(
            f"Found %s rows (no new) in table %s using %s/%s via %s (%.3fs)",
            len(found_ids),
            to_table.id,
            segment.table.id,
            segment.index,
            reference.id,
            end - start,
        )
    else:
        logging.debug(
            f"Found %s rows (%s new) as %s/%s using %s/%s via %s (%.3fs)",
            len(found_ids),
            len(new_segment.row_ids),
            new_segment.table.id,
            new_segment.index,
            segment.table.id,
            segment.index,
            reference.id,
            end - start,
        )

    return new_segment
