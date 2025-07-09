"""Azure utilities for Lakehouse.

This module provides a client for interacting with Microsoft Fabric Lakehouse, extending the base DatalakeClient.
"""
import logging
import sys
from pathlib import Path
from typing import Optional

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient

from stacks.data.platforms.common.datalake import DatalakeClient

logger = logging.getLogger(__name__)


def get_credential() -> TokenCredential | DefaultAzureCredential:
    """Returns a credential for accessing Lakehouse.

    If running in a Microsoft Fabric, it uses `mssparkutils` to get the token. Otherwise, it defaults to
    `DefaultAzureCredential`.

    Returns:
        A TokenCredential instance if running in Microsoft Fabric, otherwise None.

    """
    try:
        from notebookutils import mssparkutils
        from azure.core.credentials import TokenCredential, AccessToken

        class FabricTokenCredential(TokenCredential):
            def get_token(self, *args, **kwargs) -> AccessToken:
                token = mssparkutils.credentials.getToken("https://storage.azure.com/")
                return AccessToken(token, sys.maxsize)

        return FabricTokenCredential()

    except ImportError:
        return DefaultAzureCredential()


class LakehouseClient:
    def __init__(self, workspace_id: str, lakehouse_id: str):
        """Instantiate a new Lakehouse Client.

        Args:
            workspace_id: Workspace ID.
            lakehouse_id: Lakehouse ID
        """
        self.storage_account_name = "onelake"
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.datalake_client = DatalakeClient(self.get_account_url(), get_credential())

    def get_account_url(self) -> str:
        """Returns the account URL for the Microsoft Fabric Lakehouse service."""
        return f"https://{self.storage_account_name}.dfs.fabric.microsoft.com"

    def get_file_url(self, file_name: str) -> str:
        """Returns a Lakehouse URL for a specific file.

        Args:
            file_name: The name of the file (including any subdirectories within the lakehouse).

        Returns:
            Full Lakehouse URL for the specified file.
        """
        return (
            f"abfss://{self.workspace_id}@{self.storage_account_name}.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_id}/{file_name}"
        )

    def get_table_url(self, table_name: str, schema: Optional[str] = "dbo") -> str:
        """Returns a Lakehouse URL for a specific table."""
        if schema:
            table_fqdn = str(Path("Tables") / schema / table_name)
        else:
            table_fqdn = str(Path("Tables") / table_name)
        return self.get_file_url(table_fqdn)

    def get_file_system_client(self) -> FileSystemClient:
        """Returns a FileSystemClient for the Lakehouse."""
        return self.datalake_client.get_file_system_client(self.workspace_id)

    def _get_full_directory_path(self, directory_path: str) -> str:
        """Returns the full path for a directory within the current lakehouse."""
        # remove any leading slashes
        clean_path = directory_path.lstrip("/")
        return str(Path(self.lakehouse_id) / clean_path)

    def get_directory_client(self, directory_path: str) -> DataLakeDirectoryClient:
        """Returns a directory client for a given directory path.

        Args:
            directory_path: Path to the directory within the lakehouse, e.g. "Files".
        """
        full_directory_path = self._get_full_directory_path(directory_path)
        return self.datalake_client.get_directory_client(self.workspace_id, full_directory_path)

    def filter_directory_paths(self, directory_path: str, directory_substring: str) -> list:
        """Filters a directory for directories containing a given substring.

        Args:
            directory_path: Path of the directory to filter, e.g. "Files".
            directory_substring: String to be found in directory.

        Returns:
            List of directory paths containing the specified substring.
        """
        return self.datalake_client.filter_directory_paths(
            self.workspace_id, self._get_full_directory_path(directory_path), directory_substring
        )

    def delete_directories(self, directory_paths: list) -> None:
        """Deletes a list of directories from the Lakehouse.

        Args:
            directory_paths: List of directories to delete within a lakehouse. These are typically paths to the files
                or tables, e.g., ["Files/Folder1", "Files/Folder2", "Tables/dbo/diabetes"].
        """
        full_directory_paths = [self._get_full_directory_path(path) for path in directory_paths]
        self.datalake_client.delete_directories(self.workspace_id, full_directory_paths)

    def delete_directory(self, directory_path: str) -> None:
        """Deletes a directory from the Lakehouse.

        Args:
            directory_path: Path of the directory to delete within a lakehouse, e.g. "Files/Folder1".
        """
        full_directory_path = self._get_full_directory_path(directory_path)
        self.datalake_client.delete_directory(self.workspace_id, full_directory_path)

    def all_files_present(self, directory_path: str, expected_files: list[str]) -> bool:
        """Asserts all files in a given list are present in the specified container and directory.

        Args:
            directory_path: Path of the directory to check, e.g. "Files/Folder1".
            expected_files: List of expected file names.

        Returns:
            Boolean reflecting whether all files are present.
        """
        return self.datalake_client.all_files_present(
            self.workspace_id, self._get_full_directory_path(directory_path), expected_files
        )

    def get_directory_contents(self, directory_path: str, recursive: bool = True) -> list[str]:
        """Returns the contents of a directory in the Lakehouse.

        Args:
            directory_path: Path of the directory to read, e.g. "Files/Folder1".
            recursive: Whether to return contents recursively. Defaults to True.

        Returns:
            List of file paths within the specified directory.
        """
        return self.datalake_client.get_directory_contents(
            self.workspace_id, self._get_full_directory_path(directory_path), recursive
        )

    def upload_file(self, target_directory_path: str, local_path: str, file_name: str) -> None:
        """Uploads a file to a specified directory in Lakehouse.

        Args:
            target_directory_path: Path of the directory to upload to, e.g., "Files/Folder".
            local_path: Local path of the directory to upload from.
            file_name: Name of the file to be uploaded, e.g. "myfile.csv".
        """
        self.datalake_client.upload_file(
            self.workspace_id, self._get_full_directory_path(target_directory_path), local_path, file_name
        )
