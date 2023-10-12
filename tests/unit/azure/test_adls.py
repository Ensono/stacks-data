import pytest
from azure.storage.filedatalake import DataLakeServiceClient
from unittest.mock import Mock, patch, call
from datastacks.azure.adls import (
    delete_directories_adls,
    delete_directory_adls,
)


@pytest.fixture
def adls_client_mock():
    return Mock(spec=DataLakeServiceClient)


def test_delete_directory_adls(adls_client_mock):
    container_name = "test_container"
    directory_path = "test_directory"

    adls_directory_client_mock = adls_client_mock.get_directory_client.return_value
    adls_directory_client_mock.exists.return_value = True

    delete_directory_adls(adls_client_mock, container_name, directory_path)

    adls_client_mock.get_directory_client.assert_called_once_with(container_name, directory_path)
    adls_directory_client_mock.exists.assert_called_once()
    adls_directory_client_mock.delete_directory.assert_called_once()


@patch("datastacks.azure.adls.delete_directory_adls")
def test_delete_directories_adls(delete_directory_adls_mock, adls_client_mock):
    container_name = "test_container"
    directory_paths = ["test_directory1", "test_directory2"]

    delete_directories_adls(adls_client_mock, container_name, directory_paths)

    delete_directory_adls_mock.assert_has_calls(
        [
            call(adls_client_mock, container_name, "test_directory1"),
            call(adls_client_mock, container_name, "test_directory2"),
        ]
    )
