import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from tests.unit.pyspark.conftest import TEST_CSV_DIR

from datastacks.pyspark.storage_utils import (
    ENV_NAME_ADLS_ACCOUNT,
    ENV_NAME_APPLICATION_ID,
    ENV_NAME_BLOB_ACCOUNT,
    ENV_NAME_DIRECTORY_ID,
    ENV_NAME_SERVICE_PRINCIPAL_SECRET,
    check_env,
    get_adls_directory_contents,
    get_adls_file_url,
    get_blob_url,
    load_json_from_blob,
    set_spark_properties,
)

TEST_ENV_VARS = {
    ENV_NAME_SERVICE_PRINCIPAL_SECRET: "secret",
    ENV_NAME_APPLICATION_ID: "app_id",
    ENV_NAME_DIRECTORY_ID: "dir_id",
    ENV_NAME_ADLS_ACCOUNT: "myadlsaccount",
    ENV_NAME_BLOB_ACCOUNT: "myblobaccount",
}


@patch.dict("os.environ", TEST_ENV_VARS, clear=True)
def test_check_env():
    check_env()


@patch.dict("os.environ", {}, clear=True)
def test_check_env_raises():
    with pytest.raises(EnvironmentError):
        check_env()


@patch.dict("os.environ", {ENV_NAME_SERVICE_PRINCIPAL_SECRET: "secret", ENV_NAME_APPLICATION_ID: "app_id"}, clear=True)
def test_check_env_raises_with_partial_vars():
    with pytest.raises(EnvironmentError) as excinfo:
        check_env()
    assert ENV_NAME_DIRECTORY_ID in str(excinfo.value)
    assert ENV_NAME_ADLS_ACCOUNT in str(excinfo.value)
    assert ENV_NAME_BLOB_ACCOUNT in str(excinfo.value)


@patch.dict("os.environ", TEST_ENV_VARS, clear=True)
def test_set_spark_properties(spark):
    set_spark_properties(spark)
    adls_account = os.getenv(ENV_NAME_ADLS_ACCOUNT)
    assert spark.conf.get(f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net") == "OAuth"
    assert (
        spark.conf.get(f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net")
        == "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    assert spark.conf.get(f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net") == os.getenv(
        ENV_NAME_APPLICATION_ID
    )
    assert spark.conf.get(f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net") == os.getenv(
        ENV_NAME_SERVICE_PRINCIPAL_SECRET
    )
    assert (
        spark.conf.get(f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net")
        == f"https://login.microsoftonline.com/{os.getenv(ENV_NAME_DIRECTORY_ID)}/oauth2/token"
    )


@pytest.mark.parametrize("recursive", [True, False])
def test_get_adls_directory_contents(mock_adls_client, recursive):
    paths = get_adls_directory_contents("test_container", "test_path", recursive=recursive)

    test_path = Path(TEST_CSV_DIR)
    if recursive:
        expected_paths = [str(item.relative_to(test_path)) for item in test_path.rglob("*")]
    else:
        expected_paths = [str(item.name) for item in test_path.iterdir()]

    assert sorted(paths) == sorted(expected_paths)


def test_load_json_from_blob(mock_blob_client, json_contents):
    json_as_dict = load_json_from_blob("test_container", "test_path")
    assert json_as_dict == json.loads(json_contents)


@patch.dict("os.environ", {ENV_NAME_ADLS_ACCOUNT: "myadlsaccount"}, clear=True)
def test_get_adls_file_url():
    container = "mycontainer"
    file_name = "myfolder/myfile.txt"

    expected_url = "abfss://mycontainer@myadlsaccount.dfs.core.windows.net/myfolder/myfile.txt"

    assert get_adls_file_url(container, file_name) == expected_url


def test_get_blob_url(monkeypatch):
    monkeypatch.setenv(ENV_NAME_BLOB_ACCOUNT, "testaccount")

    expected_url = "https://testaccount.blob.core.windows.net"
    assert get_blob_url() == expected_url
