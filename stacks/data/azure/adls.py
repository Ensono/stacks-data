"""Azure utilities - ADLS.

This module provides a collection of helper functions related to Azure Data Lake Storage (ADLS).
"""
import logging
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)


def filter_directory_paths_adls(
    adls_client: DataLakeServiceClient,
    container_name: str,
    directory_path: str,
    directory_substring: str,
) -> list:
    """Filters an ADLS container directory for directories containing a given substring.

    Args:
        adls_client: DataLakeServiceClient
        container_name: Container / file system
        directory_path: Directory
        directory_substring: String to be found in directory

    Returns:
        List of directory paths containing the sub_directory prefix
    """
    adls_fs_client = adls_client.get_file_system_client(container_name)
    output_directory_paths = []
    if adls_fs_client.get_directory_client(directory_path).exists():
        paths = adls_fs_client.get_paths(directory_path)
        for path in paths:
            if path.is_directory and directory_substring in path.name:
                output_directory_paths.append(path.name)
    return output_directory_paths


def delete_directories_adls(adls_client: DataLakeServiceClient, container_name: str, directory_paths: list):
    """Deletes a list of directories from ADLS.

    Args:
        adls_client: DataLakeServiceClient
        container_name: Container / file system
        directory_paths: List of directories to delete
    """
    for directory_path in directory_paths:
        logger.info(f"ATTEMPTING TO DELETE DIRECTORY: {directory_path}")
        delete_directory_adls(adls_client, container_name, directory_path)


def delete_directory_adls(adls_client: DataLakeServiceClient, container_name, directory_path: str):
    """Deletes an ADLS directory.

    Args:
        adls_client: DataLakeServiceClient
        container_name: Container / File System
        directory_path: A directory path
    """
    adls_directory_client = adls_client.get_directory_client(container_name, directory_path)
    if adls_directory_client.exists():
        adls_directory_client.delete_directory()
    else:
        logger.info(f"The Following Directory Was Not Found: {directory_path}")


def all_files_present_in_adls(
    adls_client: DataLakeServiceClient,
    container_name: str,
    directory_name: str,
    expected_files: list,
) -> bool:
    """Asserts all files in a given list are present in the specified container and directory.

    Args:
        adls_client: DataLakeServiceClient
        container_name: Container / File System
        directory_name: Directory Name
        expected_files: List of Expected Files

    Returns:
        Boolean reflecting whether all files are present
    """
    adls_fs_client = adls_client.get_file_system_client(container_name)
    for expected_file in expected_files:
        assert any(
            expected_file in actual_output_file.name for actual_output_file in adls_fs_client.get_paths(directory_name)
        )
    return True
