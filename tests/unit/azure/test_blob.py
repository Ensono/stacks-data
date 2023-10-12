import pytest
from unittest.mock import Mock, patch, mock_open, call
from azure.storage.blob import BlobServiceClient
from datastacks.azure.blob import upload_file_to_blob, delete_blob_prefix

TEST_CONTAINER = "test_container"
TEST_DIRECTORY = "test_directory"
TEST_BLOB_PREFIX = f"{TEST_DIRECTORY}/"


@pytest.fixture
def mock_azure_blob_service_client():
    return Mock(spec=BlobServiceClient)


@pytest.fixture
def azure_blob_service_client():
    return Mock(spec=BlobServiceClient)


def test_upload_file_to_blob(mock_azure_blob_service_client):
    container_name = TEST_CONTAINER
    target_dir = TEST_DIRECTORY
    config_file_path = "datastacks/tests/data/ingest_sources/test_config.json"
    expected_target_blob = f"{TEST_DIRECTORY}/test_config.json"
    overwrite = False

    mock_blob_service_client = mock_azure_blob_service_client.return_value
    mock_container_client = mock_blob_service_client.get_container_client.return_value
    mock_blob_client = mock_container_client.get_blob_client.return_value

    with patch("builtins.open", mock_open()) as mock_file:
        upload_file_to_blob(mock_blob_service_client, container_name, target_dir, config_file_path, overwrite)
        mock_container_client.get_blob_client.assert_called_once_with(expected_target_blob)
        mock_blob_client.upload_blob.assert_called_once_with(mock_file(), overwrite=overwrite)


def test_delete_blob_prefix_success(mock_azure_blob_service_client):
    container_name = TEST_CONTAINER
    blob_prefix = TEST_BLOB_PREFIX

    container_client = mock_azure_blob_service_client.get_container_client.return_value
    blob1 = Mock(name=f"{blob_prefix}blob1.txt")
    blob2 = Mock(name=f"{blob_prefix}blob2.txt")
    blob_list = [blob1, blob2]
    container_client.list_blobs.return_value = blob_list

    with patch.object(container_client, "delete_blob") as mock_delete_blob:
        result = delete_blob_prefix(mock_azure_blob_service_client, container_name, blob_prefix)

    container_client.list_blobs.assert_called_once_with(name_starts_with=blob_prefix)
    mock_delete_blob.assert_has_calls([call(blob1.name), call(blob2.name)])
    assert result is True


def test_delete_blob_prefix_no_blobs(mock_azure_blob_service_client):
    container_name = TEST_CONTAINER
    blob_prefix = TEST_BLOB_PREFIX

    container_client = mock_azure_blob_service_client.get_container_client.return_value
    container_client.list_blobs.return_value = []

    result = delete_blob_prefix(mock_azure_blob_service_client, container_name, blob_prefix)

    container_client.list_blobs.assert_called_once_with(name_starts_with=blob_prefix)
    assert result is True


def test_delete_blob_prefix_failure(mock_azure_blob_service_client):
    container_name = TEST_CONTAINER
    blob_prefix = TEST_BLOB_PREFIX

    container_client = mock_azure_blob_service_client.get_container_client.return_value
    blob1 = Mock(name=f"{blob_prefix}blob1.txt")
    blob_list = [blob1]
    container_client.list_blobs.return_value = blob_list
    container_client.delete_blob.side_effect = Exception("Delete failed")

    result = delete_blob_prefix(mock_azure_blob_service_client, container_name, blob_prefix)

    container_client.list_blobs.assert_called_once_with(name_starts_with=blob_prefix)
    container_client.delete_blob.assert_called_with(blob1.name)
    assert result is False
