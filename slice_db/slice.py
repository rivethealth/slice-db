"""
Slice definitions
"""

import codecs
import typing
import zipfile

_MANIFEST_PATH = "manifest.json"


_UTF8_READER: codecs.StreamWriter = codecs.getreader("utf-8")


_UTF8_WRITER: codecs.StreamWriter = codecs.getwriter("utf-8")


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

    def open_schema(self, section: str):
        return self._zip.open(f"{section}.sql")

    def open_segment(
        self, table_id: str, index: int
    ) -> typing.ContextManager[typing.BinaryIO]:
        """
        Open segment
        """
        return self._zip.open(_segment_path(table_id, index))


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

    def open_schema(self, section: str):
        return self._zip.open(f"{section}.sql", "w")

    def open_segment(
        self, table_id: str, index: int
    ) -> typing.ContextManager[typing.BinaryIO]:
        """
        Open segment
        """
        return self._zip.open(_segment_path(table_id, index), "w", force_zip64=True)
