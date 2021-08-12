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


def test_transform_field_alphanumeric_case_insensitive():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "alphanumeric",
            "--params",
            '{"caseInsensitive":true}',
            "abc",
        ]
    )
    assert result.decode("utf-8") == "vfj\n"

    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "alphanumeric",
            "--params",
            '{"caseInsensitive":true}',
            "aBc",
        ]
    )
    assert result.decode("utf-8") == "vFj\n"


def test_transform_field_alphanumeric_unique():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "alphanumeric",
            "--params",
            '{"unique":true}',
            "abc",
        ]
    )
    assert result.decode("utf-8") == "grk\n"


def test_transform_field_const():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "const",
            "--params",
            '"X"',
            "example",
        ]
    )
    assert result.decode("utf-8") == "X\n"


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


def test_transform_field_given_name_case_insenstive():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "given_name",
            "--params",
            '{"caseInsensitive":true}',
            "JANE",
        ]
    )
    assert result.decode("utf-8") == "KIMBERLEY\n"

    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "given_name",
            "--params",
            '{"caseInsensitive":true}',
            "Jane",
        ]
    )
    assert result.decode("utf-8") == "Kimberley\n"


def test_transform_field_json():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "json_object",
            "--params",
            '{"properties":{"example1":{"type":"json_string", "params":{"type": "alphanumeric"}}}}',
            '{"example1":"foo","example2":"bar"}',
        ]
    )
    assert result.decode("utf-8") == '{"example1":"cys","example2":"bar"}\n'


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


def test_transform_field_null():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transform",
            "const",
            "example",
        ]
    )
    assert result.decode("utf-8") == "\n"
