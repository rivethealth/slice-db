import subprocess

from process import run_process


def test_transform_field_alphanumeric():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "alphanumeric",
            "123 Main St $9.99",
        ]
    )
    assert result.decode("utf-8") == "262 Eimu Yg $7.96\n"


def test_transform_field_date_year():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "date_year",
            "2005-03-09",
        ]
    )
    assert result.decode("utf-8") == "2005-04-01\n"


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
