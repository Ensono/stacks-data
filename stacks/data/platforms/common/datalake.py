"""Azure utilities for ADLS and Lakehouse.

This module provides a base client for Azure Data Lake Storage (ADLS) and Microsoft Fabric Lakehouse.
"""
import logging
import os
from typing import Optional

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeDirectoryClient, DataLakeServiceClient, FileSystemClient

logger = logging.getLogger(__name__)


class DatalakeClient:
    def __init__(self, account_url: str, credential: Optional[TokenCredential] = None):
        """Instantiate a new Data Lake Client.

        Args:
            account_url: The URL of the Azure or Fabric Data Lake Storage account.
            credential: The credentials to authenticate with, e.g. ManagedIdentityCredential or DefaultAzureCredential.
        """
        self.credential = credential or DefaultAzureCredential()
        self.datalake_client = DataLakeServiceClient(account_url=account_url, credential=self.credential)

    def get_file_system_client(self, file_system: str) -> FileSystemClient:
        """Returns a filesystem client based on the given file system name.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
        """
        return self.datalake_client.get_file_system_client(file_system)

    def get_directory_client(self, file_system: str, directory_path: str) -> DataLakeDirectoryClient:
        """Returns a directory client based on the given file system and directory path.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            directory_path: Path of the directory to return the client for:
                - In ADLS, this is the full path within the container, e.g., "path/to/directory".
                - In Lakehouse, this is typically the path to the files, e.g., "<lakehouse_id>/Files/".

        Examples:
            >>> adls_client.get_directory_client("gold_container", "path/to/directory")
            >>> lakehouse_client.get_directory_client("<WORKSPACE_ID>", "<LAKEHOUSE_ID>/Files/")
        """
        return self.datalake_client.get_directory_client(file_system, directory_path)

    def filter_directory_paths(self, file_system: str, directory_path: str, directory_substring: str) -> list:
        """Filters a directory for directories containing a given substring.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            directory_path: Path of the directory to return:
                - In ADLS, this is the full path within the container, e.g., "path/to/directory".
                - In Lakehouse, this is typically the path to the files, e.g., "<lakehouse_id>/Files/".
            directory_substring: String to be found in directory.

        Returns:
            List of directory paths containing the specified substring.
        """
        fs_client = self.get_file_system_client(file_system)
        output_directory_paths = []
        if self.get_directory_client(file_system, directory_path).exists():
            paths = fs_client.get_paths(directory_path)
            for path in paths:
                if path.is_directory and directory_substring in path.name:
                    output_directory_paths.append(path.name)
        return output_directory_paths

    def delete_directories(self, file_system: str, directory_paths: list) -> None:
        """Deletes a list of directories from a data lake.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            directory_paths: List of directories to delete:
                - In ADLS, these are the full paths within the container, e.g., "path/to/directory".
                - In Lakehouse, these are typically the paths to the files, e.g., "<lakehouse_id>/Files/Folder/".
        """
        for directory_path in directory_paths:
            logger.info(f"DELETING DIRECTORY: {directory_path}...")
            self.delete_directory(file_system, directory_path)

    def delete_directory(self, file_system: str, directory_path: str):
        """Deletes an ADLS directory.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            directory_path: Path of the directory to delete:
                - In ADLS, this is the full path within the container, e.g., "path/to/directory".
                - In Lakehouse, this is typically the path to the files, e.g., "<lakehouse_id>/Files/".
        """
        directory_client = self.datalake_client.get_directory_client(file_system, directory_path)
        if directory_client.exists():
            directory_client.delete_directory()
        else:
            logger.info(f"The Following Directory Was Not Found: {directory_path}.")

    def all_files_present(self, file_system: str, directory_path: str, expected_files: list[str]) -> bool:
        """Asserts all files in a given list are present in the specified container and directory.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            directory_path: Path of the directory to return:
                - In ADLS, this is the full path within the container, e.g., "path/to/directory".
                - In Lakehouse, this is typically the path to the files, e.g., "<lakehouse_id>/Files/Folder".
            expected_files: List of expected file names.

        Returns:
            Boolean reflecting whether all files are present.
        """
        fs_client = self.datalake_client.get_file_system_client(file_system)
        for expected_file in expected_files:
            assert any(
                expected_file in actual_output_file.name for actual_output_file in fs_client.get_paths(directory_path)
            )
        return True

    def get_directory_contents(
        self, file_system: str, directory_path: str, recursive: Optional[bool] = True
    ) -> list[str]:
        """Gets the contents of a specified directory in an Azure Data Lake Storage container or Lakehouse.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            directory_path: Path of the directory to return:
                - In ADLS, this is the full path within the container, e.g., "path/to/directory".
                - In Lakehouse, this is typically the path to the files, e.g., "<lakehouse_id>/Files/Folder".
            recursive: If True, lists contents of all subdirectories recursively.

        Returns:
            A list of paths for the files and subdirectories within the specified file system.
        """
        file_system_client = self.datalake_client.get_file_system_client(file_system)

        paths = file_system_client.get_paths(path=directory_path, recursive=recursive)
        paths = [path.name for path in paths]
        logger.debug(f"Directory contents: {paths}")
        return paths

    def upload_file(self, file_system: str, target_directory_path: str, local_path: str, file_name: str) -> None:
        """Uploads a file to a specified directory in Azure Data Lake Storage or Lakehouse.

        Args:
            file_system: Name of the file system (container name in ADLS, workspace ID in Lakehouse).
            target_directory_path: Path of the directory to upload to:
                - In ADLS, this is the full path within the container, e.g., "path/to/directory".
                - In Lakehouse, this is typically the path to the files, e.g., "<lakehouse_id>/Files/Folder".
            local_path: Local path of the directory to upload from.
            file_name: Name of the file to be uploaded, e.g. "myfile.csv".
        """
        directory_client = self.get_directory_client(file_system, target_directory_path)
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            file_client.upload_data(data, overwrite=True)
