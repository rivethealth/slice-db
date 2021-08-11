"""
Slice definitions
"""

import codecs
import typing
import zipfile

_MANIFEST_PATH = "manifest.json"


_UTF8_READER: codecs.StreamWriter = codecs.getreader("utf-8")


_UTF8_WRITER: codecs.StreamWriter = codecs.getwriter("utf-8")


def _schema_path(section: str, index: int):
    return f"{section}/{index + 1}.sql"


def _sequence_path(seq_id: str) -> str:
    return f"{seq_id}.txt"


def _segment_path(table_id: str, index: int) -> str:
    return f"{table_id}/{index + 1}.tsv"


class SliceReader:
    """
    Read slice
    """

    def __init__(self, file: typing.BinaryIO):
        self._zip = zipfile.ZipFile(file)

    def __enter__(self, *args, **kwargs):
        self._zip.__enter__(*args, **kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self._zip.__exit__(*args, **kwargs)

    def open_manifest(self) -> typing.ContextManager[typing.TextIO]:
        """
        Open manifest
        """
        file = self._zip.open(_MANIFEST_PATH)
        return _UTF8_READER(file)

    def open_schema(self, section: str, index: int):
        return self._zip.open(_schema_path(section, index))

    def open_segment(
        self, table_id: str, index: int
    ) -> typing.ContextManager[typing.BinaryIO]:
        """
        Open segment
        """
        return self._zip.open(_segment_path(table_id, index))

    def read_sequence(self, id: str):
        with self._zip.open(_sequence_path(id), "r") as f:
            reader = _UTF8_READER(f)
            return int(f.read())


class SliceWriter:
    """
    Write slice
    """

    def __init__(self, file: typing.BinaryIO):
        self._zip = zipfile.ZipFile(file, "w", compression=zipfile.ZIP_DEFLATED)

    def __enter__(self, *args, **kwargs):
        self._zip.__enter__(*args, **kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self._zip.__exit__(*args, **kwargs)

    def open_manifest(self) -> typing.ContextManager[typing.TextIO]:
        """
        Open manifest
        """
        file = self._zip.open(_MANIFEST_PATH, "w")
        return _UTF8_WRITER(file)

    def open_schema(self, section: str, index: int):
        return self._zip.open(_schema_path(section, index), "w")

    def open_segment(
        self, table_id: str, index: int
    ) -> typing.ContextManager[typing.BinaryIO]:
        """
        Open segment
        """
        return self._zip.open(_segment_path(table_id, index), "w", force_zip64=True)

    def write_sequence(self, id: str, value: int):
        with self._zip.open(_sequence_path(id), "w") as f:
            writer = _UTF8_WRITER(f)
            writer.write(str(value))
