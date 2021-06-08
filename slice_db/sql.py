import codecs
import contextlib
import typing

from pg_sql import SqlId, SqlNumber, SqlObject, SqlString, sql_list

_UTF_8_WRITER = codecs.getwriter("utf-8")


class SqlWriter:
    def __init__(self, file):
        self._file = file

    def open_predata(self):
        return contextlib.nullcontext(self._file)

    def open_postdata(self):
        return contextlib.nullcontext(self._file)

    @contextlib.contextmanager
    def open_data(
        self, id: str, index: int, schema: str, table: str, columns: typing.List[str]
    ):
        text_writer = _UTF_8_WRITER(self._file)

        text_writer.write(f"--\n-- Data for {id}/{index}\n--\n")

        table_sql = SqlObject(SqlId(schema), SqlId(table))
        columns_sql = sql_list(SqlId(column) for column in columns)
        query = f"COPY {table_sql} ({columns_sql}) FROM stdin;\n"
        text_writer.write(query)

        yield self._file

        text_writer.write("\\.\n\n\n")

    def write_sequence(self, id: str, schema: str, name: str, value: int):
        text_writer = _UTF_8_WRITER(self._file)

        text_writer.write(f"--\n-- Sequence {id}\n--\n")

        seq_name = SqlObject(SqlId(schema), SqlId(name))
        seq_value = SqlNumber(value)
        query = f"SELECT setval({SqlString(str(seq_name))}, {seq_value}) FROM {seq_name} WHERE last_value < {seq_value};\n"
        text_writer.write(query)

        text_writer.write("\n")
