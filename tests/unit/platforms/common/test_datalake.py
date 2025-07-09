import pytest
from unittest.mock import Mock, patch
from azure.storage.filedatalake import FileSystemClient, PathProperties
from stacks.data.platforms.common.datalake import DatalakeClient
from pathlib import Path

TEST_CSV_DIR = "tests/data/movies_dataset"


@pytest.fixture
def mock_datalake_client():
    with patch("stacks.data.platforms.common.datalake.DataLakeServiceClient", autospec=True) as mock_service_client:

        def get_paths_side_effect(path, recursive=True):
            test_path = Path(TEST_CSV_DIR)
            files_and_dirs = list(test_path.rglob("*")) if recursive else list(test_path.glob("*"))

            mock_paths = []
            for item in files_and_dirs:
                mock_path = Mock(spec=PathProperties)
                mock_path.name = str(item.relative_to(test_path))
                mock_path.is_directory = item.is_dir()
                mock_paths.append(mock_path)

            return mock_paths

        mock_client = DatalakeClient("https://teststorageaccount.dfs.core.windows.net")
        mock_file_system_client = Mock(spec=FileSystemClient)
        mock_file_system_client.get_paths.side_effect = get_paths_side_effect
        mock_service_client.return_value.get_file_system_client.return_value = mock_file_system_client

        yield mock_client


def test_filter_directory_paths(mock_datalake_client):
    result = mock_datalake_client.filter_directory_paths("container_name", "directory_path", "sub")
    assert result == ["subfolder"]


def test_delete_directory(mock_datalake_client):
    adls_directory_client_mock = mock_datalake_client.datalake_client.get_directory_client.return_value
    adls_directory_client_mock.exists.return_value = True

    mock_datalake_client.delete_directory("container_name", "directory_path")

    mock_datalake_client.datalake_client.get_directory_client.assert_called_once()
    adls_directory_client_mock.exists.assert_called_once()
    adls_directory_client_mock.delete_directory.assert_called_once()


@pytest.mark.parametrize("files", [["links.csv"], ["keywords.csv", "links.csv", "movies_metadata.csv", "ratings.csv"]])
def test_all_files_present(mock_datalake_client, files):
    result = mock_datalake_client.all_files_present("container_name", "directory_name", files)
    assert result is True


@pytest.mark.parametrize("files", [["missing.csv"], ["links.csv", "missing.csv"]])
def test_all_files_present_error(mock_datalake_client, files):
    with pytest.raises(AssertionError):
        mock_datalake_client.all_files_present("container_name", "directory_name", files)


@pytest.mark.parametrize("recursive", [True, False])
def test_get_directory_contents(mock_datalake_client, recursive):
    paths = mock_datalake_client.get_directory_contents("test_container", "test_path", recursive=recursive)

    test_path = Path(TEST_CSV_DIR)
    if recursive:
        expected_paths = [str(item.relative_to(test_path)) for item in test_path.rglob("*")]
    else:
        expected_paths = [str(item.name) for item in test_path.iterdir()]

    assert sorted(paths) == sorted(expected_paths)


def test_upload_file(mock_datalake_client):
    file_system = "test_container"
    directory_path = "test_directory"
    local_path = TEST_CSV_DIR
    file_name = "links.csv"

    directory_client_mock = mock_datalake_client.datalake_client.get_directory_client.return_value
    file_client_mock = directory_client_mock.get_file_client.return_value

    mock_datalake_client.upload_file(file_system, directory_path, local_path, file_name)

    directory_client_mock.get_file_client.assert_called_once_with(file_name)
    file_client_mock.upload_data.assert_called_once()
