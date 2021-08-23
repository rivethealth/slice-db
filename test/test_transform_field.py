import subprocess

from process import run_process


def test_transform_field_address_line_1():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"AddressLine1Transform"}}',
            "123 Main St",
        ]
    )
    assert result.decode("utf-8") == "8901 Sherrin Avenue South\n"


def test_transform_field_address_line_2():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"AddressLine2Transform"}}',
            "Suite 101",
        ]
    )
    assert result.decode("utf-8") == "#897\n"


def test_transform_field_alphanumeric():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"AlphanumericTransform"}}',
            "123 Main St $9.99",
        ]
    )
    assert result.decode("utf-8") == "850 Xxqy Wh $0.97\n"


def test_transform_field_alphanumeric_unique():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"AlphanumericTransform","config":{"unique":true}}}',
            "abc",
        ]
    )
    assert result.decode("utf-8") == "grk\n"


def test_transform_field_city():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"CityTransform"}}',
            "New York City",
        ]
    )
    assert result.decode("utf-8") == "Woodland\n"


def test_transform_field_const():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"ConstTransform","config":"X"}}',
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
            "--transforms",
            '{"":{"class":"DateYearTransform"}}',
            "2005-03-09",
        ]
    )
    assert result.decode("utf-8") == "2005-08-19\n"


def test_transform_field_geozip():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"GeozipTransform"}}',
            "94356",
        ]
    )
    assert result.decode("utf-8") == "94304\n"


def test_transform_field_given_name():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"GivenNameTransform"}}',
            "Jane",
        ]
    )
    assert result.decode("utf-8") == "Joe\n"


def test_transform_field_json_path():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"JsonPathTransform","config":[{"path":"example1","transform":"alphanumeric"},{"path":"example2","transform":"alphanumeric"}]},"alphanumeric":{"class":"AlphanumericTransform"}}',
            '{"example1":"foo","example2":"bar"}',
        ]
    )
    assert result.decode("utf-8") == '{"example1":"axl","example2":"fwe"}\n'


def test_transform_field_null():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"NullTransform"}}',
            "example",
        ]
    )
    assert result.decode("utf-8") == "\n"


def test_transform_field_surname():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"SurnameTransform"}}',
            "Baker",
        ]
    )
    assert result.decode("utf-8") == "Kemper\n"


def test_transform_field_us_state():
    result = run_process(
        [
            "slicedb",
            "transform-field",
            "--pepper",
            "abc",
            "--transforms",
            '{"":{"class":"UsStateTransform"}}',
            "New York",
        ]
    )
    assert result.decode("utf-8") == "Tennessee\n"
