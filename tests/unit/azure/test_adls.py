import pytest
from unittest.mock import Mock, patch
from azure.storage.filedatalake import FileSystemClient, PathProperties
from stacks.data.azure.adls import AdlsClient
from pathlib import Path

TEST_CSV_DIR = "tests/data/movies_dataset"


@pytest.fixture
def mock_adls_client():
    with patch("stacks.data.azure.adls.DataLakeServiceClient", autospec=True) as mock_service_client:

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

        mock_client = AdlsClient("teststorageaccount")
        mock_file_system_client = Mock(spec=FileSystemClient)
        mock_file_system_client.get_paths.side_effect = get_paths_side_effect
        mock_service_client.return_value.get_file_system_client.return_value = mock_file_system_client

        yield mock_client


def test_filter_directory_paths_adls(mock_adls_client):
    result = mock_adls_client.filter_directory_paths_adls("container_name", "directory_path", "sub")
    assert result == ["subfolder"]


def test_delete_directory_adls(mock_adls_client):
    adls_directory_client_mock = mock_adls_client.adls_client.get_directory_client.return_value
    adls_directory_client_mock.exists.return_value = True

    mock_adls_client.delete_directory_adls("container_name", "directory_path")

    mock_adls_client.adls_client.get_directory_client.assert_called_once()
    adls_directory_client_mock.exists.assert_called_once()
    adls_directory_client_mock.delete_directory.assert_called_once()


@pytest.mark.parametrize("files", [["links.csv"], ["keywords.csv", "links.csv", "movies_metadata.csv", "ratings.csv"]])
def test_all_files_present_in_adls(mock_adls_client, files):
    result = mock_adls_client.all_files_present_in_adls("container_name", "directory_name", files)
    assert result is True


@pytest.mark.parametrize("files", [["missing.csv"], ["links.csv", "missing.csv"]])
def test_all_files_present_in_adls_error(mock_adls_client, files):
    with pytest.raises(AssertionError):
        mock_adls_client.all_files_present_in_adls("container_name", "directory_name", files)


@pytest.mark.parametrize("recursive", [True, False])
def test_get_adls_directory_contents(mock_adls_client, recursive):
    paths = mock_adls_client.get_adls_directory_contents("test_container", "test_path", recursive=recursive)

    test_path = Path(TEST_CSV_DIR)
    if recursive:
        expected_paths = [str(item.relative_to(test_path)) for item in test_path.rglob("*")]
    else:
        expected_paths = [str(item.name) for item in test_path.iterdir()]

    assert sorted(paths) == sorted(expected_paths)


def test_get_adls_file_url(mock_adls_client):
    container = "mycontainer"
    file_name = "myfolder/myfile.txt"
    expected_url = "abfss://mycontainer@teststorageaccount.dfs.core.windows.net/myfolder/myfile.txt"

    assert mock_adls_client.get_adls_file_url(container, file_name) == expected_url
