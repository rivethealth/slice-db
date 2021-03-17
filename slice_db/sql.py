import codecs
import contextlib
import typing

import psycopg2.sql as sql

_UTF_8_WRITER = codecs.getwriter("utf-8")


class SqlWriter:
    def __init__(self, context, file):
        self._context = context
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

        query = sql.SQL("COPY {} ({}) FROM stdin;\n").format(
            sql.Identifier(schema, table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        )
        text_writer.write(query.as_string(self._context))

        yield self._file

        text_writer.write("\\.\n\n\n")
