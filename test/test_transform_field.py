import subprocess

from process import run_process


def test_transform_field_geozip():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "geozip",
            "94356",
        ]
    )
    assert result.decode("utf-8") == "94303\n"


def test_transform_field_given_name():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "given_name",
            "Jane",
        ]
    )
    assert result.decode("utf-8") == "Jeramy\n"


def test_transform_field_surname():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "surname",
            "Baker",
        ]
    )
    assert result.decode("utf-8") == "Bertrand\n"
