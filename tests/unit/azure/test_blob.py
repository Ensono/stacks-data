import json
import pytest
from unittest.mock import Mock, patch, call, mock_open
from azure.storage.blob import ContainerClient
from stacks.data.azure.blob import BlobStorageClient

TEST_CONTAINER = "test_container"
TEST_DIRECTORY = "test_directory"
TEST_BLOB_PREFIX = f"{TEST_DIRECTORY}/"


@pytest.fixture
def mock_blob_client():
    with patch("stacks.data.azure.blob.BlobServiceClient", autospec=True) as mock_service_client:
        mock_client = BlobStorageClient("teststorageaccount")
        mock_container_client = Mock(spec=ContainerClient)
        mock_blob_client = Mock(spec=BlobStorageClient)
        mock_service_client.return_value.get_container_client.return_value = mock_container_client
        mock_container_client.return_value.get_blob_client.return_value = mock_blob_client
        yield mock_client


@pytest.mark.parametrize("overwrite", [True, False])
def test_upload_file_to_blob(mock_blob_client, overwrite):
    local_file_path = "tests/data/ingest_sources/test_config.json"
    expected_target_blob = f"{TEST_DIRECTORY}/test_config.json"

    with patch("builtins.open", mock_open()) as mock_file:
        mock_blob_client.upload_file_to_blob(TEST_CONTAINER, TEST_DIRECTORY, local_file_path, overwrite=overwrite)

        mock_get_container_client = mock_blob_client.blob_service_client.get_container_client

        mock_get_container_client.assert_called_once_with(TEST_CONTAINER)
        mock_get_container_client.return_value.get_blob_client.assert_called_once_with(expected_target_blob)
        mock_get_container_client.return_value.get_blob_client.return_value.upload_blob.assert_called_once_with(
            mock_file(), overwrite=overwrite
        )


def test_delete_blob_prefix_success(mock_blob_client):
    container_name = TEST_CONTAINER
    blob_prefix = TEST_BLOB_PREFIX

    container_client = mock_blob_client.blob_service_client.get_container_client.return_value
    blob1 = Mock(name=f"{blob_prefix}blob1.txt")
    blob2 = Mock(name=f"{blob_prefix}blob2.txt")
    blob_list = [blob1, blob2]
    container_client.list_blobs.return_value = blob_list

    with patch.object(container_client, "delete_blob") as mock_delete_blob:
        result = mock_blob_client.delete_blob_prefix(container_name, blob_prefix)

    container_client.list_blobs.assert_called_once_with(name_starts_with=blob_prefix)
    mock_delete_blob.assert_has_calls([call(blob1.name), call(blob2.name)])
    assert result is True


def test_delete_blob_prefix_no_blobs(mock_blob_client):
    container_name = TEST_CONTAINER
    blob_prefix = TEST_BLOB_PREFIX

    container_client = mock_blob_client.blob_service_client.get_container_client.return_value
    container_client.list_blobs.return_value = []

    result = mock_blob_client.delete_blob_prefix(container_name, blob_prefix)

    container_client.list_blobs.assert_called_once_with(name_starts_with=blob_prefix)
    assert result is True


def test_delete_blob_prefix_failure(mock_blob_client):
    container_name = TEST_CONTAINER
    blob_prefix = TEST_BLOB_PREFIX

    container_client = mock_blob_client.blob_service_client.get_container_client.return_value
    blob1 = Mock(name=f"{blob_prefix}blob1.txt")
    blob_list = [blob1]
    container_client.list_blobs.return_value = blob_list
    container_client.delete_blob.side_effect = Exception("Delete failed")

    result = mock_blob_client.delete_blob_prefix(container_name, blob_prefix)

    container_client.list_blobs.assert_called_once_with(name_starts_with=blob_prefix)
    container_client.delete_blob.assert_called_with(blob1.name)
    assert result is False


def test_load_json_from_blob(mock_blob_client):
    container_name = "test_container"
    file_path = "test_dir/test_data.json"
    expected_json = {"key": "value"}
    mock_blob_client.blob_service_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = json.dumps(  # noqa: E501
        expected_json
    )

    result = mock_blob_client.load_json_from_blob(container_name, file_path)

    assert mock_blob_client.blob_service_client.get_blob_client.call_count == 1
    assert mock_blob_client.blob_service_client.get_blob_client.call_args == call(container_name, file_path)
    assert result == expected_json
