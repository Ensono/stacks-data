import json
import pytest

from unittest.mock import patch

from stacks.data.pyspark.storage_utils import (
    check_env,
    load_json_from_blob,
    get_blob_url,
    set_spark_properties,
)


test_azure_tenant_id = "dir_id"
test_azure_client_id = "app_id"
test_azure_client_secret = "secret"
test_azure_storage_account_name = "myadlsaccount"
test_azure_config_account_name = "myblobaccount"


@patch.dict("os.environ", clear=True)
def test_check_env_all_variables_set(monkeypatch):
    monkeypatch.setenv("AZURE_TENANT_ID", test_azure_tenant_id)
    monkeypatch.setenv("AZURE_CLIENT_ID", test_azure_client_id)
    monkeypatch.setenv("AZURE_CLIENT_SECRET", test_azure_client_secret)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", test_azure_storage_account_name)
    monkeypatch.setenv("AZURE_CONFIG_ACCOUNT_NAME", test_azure_config_account_name)

    check_env()


@patch.dict("os.environ", clear=True)
def test_check_env_missing_variables(monkeypatch):
    monkeypatch.delenv("AZURE_TENANT_ID", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_NAME", raising=False)
    monkeypatch.delenv("AZURE_CONFIG_ACCOUNT_NAME", raising=False)

    with pytest.raises(EnvironmentError):
        check_env()


@patch.dict("os.environ", clear=True)
def test_check_env_raises_with_partial_vars(monkeypatch):
    monkeypatch.setenv("AZURE_TENANT_ID", test_azure_tenant_id)
    monkeypatch.setenv("AZURE_CLIENT_ID", test_azure_client_id)
    monkeypatch.setenv("AZURE_CLIENT_SECRET", test_azure_client_id)
    with pytest.raises(EnvironmentError) as excinfo:
        check_env()
    assert "AZURE_STORAGE_ACCOUNT_NAME" in str(excinfo.value)
    assert "AZURE_CONFIG_ACCOUNT_NAME" in str(excinfo.value)


def test_set_spark_properties(spark):
    with patch.multiple(
        "stacks.data.pyspark.storage_utils",
        AZURE_TENANT_ID=test_azure_tenant_id,
        AZURE_CLIENT_ID=test_azure_client_id,
        AZURE_CLIENT_SECRET=test_azure_client_secret,
        AZURE_STORAGE_ACCOUNT_NAME=test_azure_storage_account_name,
    ):
        set_spark_properties(spark)
        assert (
            spark.conf.get(f"fs.azure.account.auth.type.{test_azure_storage_account_name}.dfs.core.windows.net")
            == "OAuth"
        )
        assert (
            spark.conf.get(
                f"fs.azure.account.oauth.provider.type.{test_azure_storage_account_name}.dfs.core.windows.net"
            )
            == "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        assert (
            spark.conf.get(f"fs.azure.account.oauth2.client.id.{test_azure_storage_account_name}.dfs.core.windows.net")
            == test_azure_client_id
        )

        assert (
            spark.conf.get(
                f"fs.azure.account.oauth2.client.secret.{test_azure_storage_account_name}.dfs.core.windows.net"
            )
            == test_azure_client_secret
        )

        assert (
            spark.conf.get(
                f"fs.azure.account.oauth2.client.endpoint.{test_azure_storage_account_name}.dfs.core.windows.net"
            )
            == f"https://login.microsoftonline.com/{test_azure_tenant_id}/oauth2/token"
        )


def test_load_json_from_blob(mock_blob_client, json_contents):
    json_as_dict = load_json_from_blob("test_container", "test_path")
    assert json_as_dict == json.loads(json_contents)


def test_get_blob_url():
    with patch("stacks.data.pyspark.storage_utils.AZURE_CONFIG_ACCOUNT_NAME", new=test_azure_config_account_name):
        assert get_blob_url() == f"https://{test_azure_config_account_name}.blob.core.windows.net"
