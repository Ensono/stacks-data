import pytest

from requests.exceptions import RequestException
from unittest.mock import Mock, patch

from stacks_data.utils import (
    filter_files_by_extension,
    find_placeholders,
    substitute_env_vars,
    camel_to_snake,
    config_uniqueness_check,
    get_latest_package_version,
)

TEST_ENV_VARS = {"TEST_VAR1": "value1", "TEST_VAR2": "value2", "ADLS_ACCOUNT": "value3"}


@pytest.mark.parametrize(
    "input_str,expected",
    [
        (
            "abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net/table_name",
            ["ADLS_ACCOUNT"],
        ),
        (
            "abcd{TEST_VAR1}{TEST_VAR2}",
            ["TEST_VAR1", "TEST_VAR2"],
        ),
        (
            "somestring{TEST_VAR3}{NONEXISTENT_VAR}123",
            ["TEST_VAR3", "NONEXISTENT_VAR"],
        ),
    ],
)
@patch.dict("os.environ", TEST_ENV_VARS, clear=True)
def test_find_placeholders(input_str, expected):
    assert find_placeholders(input_str) == expected


@patch.dict("os.environ", TEST_ENV_VARS, clear=True)
def test_substitute_env_vars():
    input_str = "{TEST_VAR1}_{TEST_VAR2}_{ADLS_ACCOUNT}_{NONEXISTENT_VAR}"

    assert substitute_env_vars(input_str) == "value1_value2_value3_{NONEXISTENT_VAR}"


@pytest.mark.parametrize(
    "extension,expected",
    [
        ("csv", ["test1.csv", "test3.csv"]),
        ("txt", ["test2.txt"]),
        ("doc", ["test4.doc"]),
        ("pdf", ["test5.pdf"]),
        (".pdf", ["test5.pdf"]),
    ],
)
def test_filter_files_by_extension(extension, expected):
    paths = ["test1.csv", "test2.txt", "test3.csv", "test4.doc", "test5.pdf", "test6", "test7/csv"]
    assert filter_files_by_extension(paths, extension) == expected


@pytest.mark.parametrize(
    "input_str, expected_output",
    [
        ("camelCase", "camel_case"),
        ("CamelCase", "camel_case"),
        ("CamelCamelCase", "camel_camel_case"),
        ("Camel2Camel2Case", "camel2_camel2_case"),
        ("getHTTPResponseCode", "get_http_response_code"),
        ("get2HTTP", "get2_http"),
        ("HTTPResponseCode", "http_response_code"),
        ("noChange", "no_change"),
        ("", ""),
    ],
)
def test_camel_to_snake(input_str, expected_output):
    result = camel_to_snake(input_str)
    assert result == expected_output


@pytest.mark.parametrize(
    "config_list, unique_key, expected_output",
    [
        (
            [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"},
                {"id": 3, "name": "test3"},
            ],
            "id",
            True,
        ),
        (
            [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"},
                {"id": 2, "name": "test3"},
            ],
            "id",
            False,
        ),
    ],
)
def test_config_uniqueness_check(config_list, unique_key, expected_output):
    result = config_uniqueness_check(config_list, unique_key)
    assert result == expected_output


def test_get_latest_package_version():
    package_name = "test-package"
    package_json = {"info": {"name": "test-package", "version": "3.2.1"}}
    mock_response = Mock()
    mock_response.json.return_value = package_json
    mock_response.raise_for_status.return_value = None
    with patch("requests.get", return_value=mock_response):
        latest_version = get_latest_package_version(package_name)
    assert latest_version == "3.2.1"


def test_get_latest_package_version_invalid_package():
    package_name = "non-existent-package"
    with patch("requests.get", side_effect=RequestException("RequestException")):
        with pytest.raises(RequestException):
            get_latest_package_version(package_name)
