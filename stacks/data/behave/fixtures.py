"""Behave Fixtures.

This module contains a range of standard fixtures to be used as part of Behave tests running against Azure data
workloads.
"""
import logging
from os import listdir
from os.path import isfile, join
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from behave import fixture
from behave.runner import Context
from stacks.data.constants import (
    ADLS_URL,
    CONFIG_CONTAINER_NAME,
    CONFIG_BLOB_URL,
    AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
)
from stacks.data.azure.adls import filter_directory_paths_adls, delete_directories_adls
from stacks.data.azure.blob import delete_blob_prefix, upload_file_to_blob

logger = logging.getLogger(__name__)


@fixture
def azure_adls_clean_up(context: Context, container_name: str, ingest_directory_name: str):
    """Delete test directories in ADLS.

    Args:
        context: Behave context object.
        container_name: Name of the ADLS storage container.
        ingest_directory_name: Name of the ADLS directory to delete.

    """
    credential = DefaultAzureCredential()
    adls_client = DataLakeServiceClient(account_url=ADLS_URL, credential=credential)
    logger.info("BEFORE SCENARIO: Deleting any existing test output data.")
    automated_test_output_directory_paths = filter_directory_paths_adls(
        adls_client,
        container_name,
        ingest_directory_name,
        AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
    )

    delete_directories_adls(adls_client, container_name, automated_test_output_directory_paths)

    yield context

    logger.info("AFTER SCENARIO: Deleting automated test output data.")

    automated_test_output_directory_paths = filter_directory_paths_adls(
        adls_client,
        container_name,
        ingest_directory_name,
        AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
    )

    delete_directories_adls(adls_client, container_name, automated_test_output_directory_paths)


@fixture
def azure_blob_config_prepare(context: Context, data_target_directory: str, data_local_directory: str):
    """Delete any existing files in the test directory of config blob storage, and upload test config files.

    Args:
        context: Behave context object
        data_target_directory: The test directory prefix to clear out and upload to
        data_local_directory: Directory where the test config files are stored.
    """
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=CONFIG_BLOB_URL, credential=credential)

    target_directory = f"{AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX}/{data_target_directory}"

    logger.info(f"BEFORE SCENARIO: Deleting existing test config from {CONFIG_CONTAINER_NAME}/{target_directory}*.")
    delete_blob_prefix(blob_service_client, CONFIG_CONTAINER_NAME, target_directory)

    logger.info(f"BEFORE SCENARIO: Uploading test config to {CONFIG_CONTAINER_NAME}/{target_directory}.")
    config_filepaths = [f for f in listdir(data_local_directory) if isfile(join(data_local_directory, f))]

    for file in config_filepaths:
        upload_file_to_blob(
            blob_service_client, CONFIG_CONTAINER_NAME, target_directory, f"{data_local_directory}/{file}"
        )

    yield context

    logger.info("AFTER SCENARIO: Deleting test config.")
    delete_blob_prefix(blob_service_client, CONFIG_CONTAINER_NAME, AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX)
