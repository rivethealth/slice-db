import contextlib
import typing


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
        self._file.write("--\n".encode("utf-8"))
        self._file.write(f"-- Data for {id}/{index}\n".encode("utf-8"))
        self._file.write("--\n".encode("utf-8"))
        self._file.write(
            f"COPY {schema}.{table} ({', '.join(columns)}) FROM stdin;\n".encode(
                "utf-8"
            )
        )
        yield self._file
        self._file.write("\\.\n\n\n".encode("utf-8"))
