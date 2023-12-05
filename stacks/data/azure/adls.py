"""Azure utilities - ADLS.

This module provides a collection of helper functions related to Azure Data Lake Storage (ADLS).
"""
import logging
from typing import Optional
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


logger = logging.getLogger(__name__)


class AdlsClient:
    def __init__(self, storage_account_name: str):
        """Instantiate a new ADLS Client.

        Args:
            storage_account_name: Name of the storage account
        """
        self.storage_account_name = storage_account_name
        self.account_url = f"https://{storage_account_name}.dfs.core.windows.net"
        self.credential = DefaultAzureCredential()
        self.adls_client = DataLakeServiceClient(account_url=self.account_url, credential=self.credential)

    def filter_directory_paths_adls(
        self,
        container_name: str,
        directory_path: str,
        directory_substring: str,
    ) -> list:
        """Filters an ADLS container directory for directories containing a given substring.

        Args:
            container_name: Container / file system
            directory_path: Directory
            directory_substring: String to be found in directory

        Returns:
            List of directory paths containing the sub_directory prefix
        """
        adls_fs_client = self.adls_client.get_file_system_client(container_name)
        output_directory_paths = []
        if adls_fs_client.get_directory_client(directory_path).exists():
            paths = adls_fs_client.get_paths(directory_path)
            for path in paths:
                if path.is_directory and directory_substring in path.name:
                    output_directory_paths.append(path.name)
        return output_directory_paths

    def delete_directories_adls(self, container_name: str, directory_paths: list):
        """Deletes a list of directories from ADLS.

        Args:
            container_name: Container / file system
            directory_paths: List of directories to delete
        """
        for directory_path in directory_paths:
            logger.info(f"ATTEMPTING TO DELETE DIRECTORY: {directory_path}")
            self.delete_directory_adls(container_name, directory_path)

    def delete_directory_adls(self, container_name: str, directory_path: str):
        """Deletes an ADLS directory.

        Args:
            container_name: Container / File System
            directory_path: A directory path
        """
        adls_directory_client = self.adls_client.get_directory_client(container_name, directory_path)
        if adls_directory_client.exists():
            adls_directory_client.delete_directory()
        else:
            logger.info(f"The Following Directory Was Not Found: {directory_path}")

    def all_files_present_in_adls(
        self,
        container_name: str,
        directory_name: str,
        expected_files: list,
    ) -> bool:
        """Asserts all files in a given list are present in the specified container and directory.

        Args:
            container_name: Container / File System
            directory_name: Directory Name
            expected_files: List of Expected Files

        Returns:
            Boolean reflecting whether all files are present
        """
        adls_fs_client = self.adls_client.get_file_system_client(container_name)
        for expected_file in expected_files:
            assert any(
                expected_file in actual_output_file.name
                for actual_output_file in adls_fs_client.get_paths(directory_name)
            )
        return True

    def get_adls_directory_contents(self, container: str, path: str, recursive: Optional[bool] = True) -> list[str]:
        """Gets the contents of a specified directory in an Azure Data Lake Storage container.

        Args:
            container: The name of the container in the ADLS account.
            path: The directory path within the container for which to list contents.
            recursive: If True, lists contents of all subdirectories recursively.

        Returns:
            A list of paths for the files and subdirectories within the specified container.
        """
        file_system_client = self.adls_client.get_file_system_client(file_system=container)

        paths = file_system_client.get_paths(path=path, recursive=recursive)
        paths = [path.name for path in paths]
        logger.debug(f"ADLS directory contents: {paths}")
        return paths

    def get_adls_file_url(self, container: str, file_name: str) -> str:
        """Constructs an Azure Data Lake Storage (ADLS) URL for a specific file.

        Args:
            container: The name of the ADLS container.
            file_name: The name of the file (including any subdirectories within the container).

        Returns:
            Full ADLS URL for the specified file.
        """
        return f"abfss://{container}@{self.storage_account_name}.dfs.core.windows.net/{file_name}"
