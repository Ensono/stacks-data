import json

from unittest.mock import patch

from stacks.data.pyspark.storage_utils import (
    load_json_from_blob,
    get_blob_url,
)

test_azure_tenant_id = "dir_id"
test_azure_client_id = "app_id"
test_azure_client_secret = "secret"
test_azure_storage_account_name = "myadlsaccount"
test_azure_config_account_name = "myblobaccount"


def test_load_json_from_blob(mock_blob_client, json_contents):
    json_as_dict = load_json_from_blob("test_container", "test_path")
    assert json_as_dict == json.loads(json_contents)


def test_get_blob_url():
    with patch("stacks.data.pyspark.storage_utils.AZURE_CONFIG_ACCOUNT_NAME", new=test_azure_config_account_name):
        assert get_blob_url() == f"https://{test_azure_config_account_name}.blob.core.windows.net"
