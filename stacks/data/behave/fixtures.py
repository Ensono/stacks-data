"""Behave Fixtures.

This module contains a range of standard fixtures to be used as part of Behave tests running against Azure data
workloads.
"""
import logging
from os import listdir
from os.path import isfile, join

from stacks.data.platforms.azure.constants import (
    ADLS_ACCOUNT,
    CONFIG_BLOB_ACCOUNT,
    CONFIG_CONTAINER_NAME,
    AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
)
from stacks.data.platforms.azure.adls import AdlsClient
from stacks.data.platforms.azure.blob import BlobStorageClient

try:
    from behave import fixture
    from behave.runner import Context
except ImportError:
    raise ImportError(
        "Required dependencies for Stacks Data Behave testing are not installed. "
        "Please install them using: pip install stacks-data[behave]"
    )

logger = logging.getLogger(__name__)


@fixture
def azure_adls_clean_up(context: Context, container_name: str, ingest_directory_name: str):
    """Delete test directories in ADLS.

    Args:
        context: Behave context object.
        container_name: Name of the ADLS storage container.
        ingest_directory_name: Name of the ADLS directory to delete.

    """
    adls_client = AdlsClient(ADLS_ACCOUNT)
    logger.info("BEFORE SCENARIO: Deleting any existing test output data.")
    automated_test_output_directory_paths = adls_client.filter_directory_paths(
        container_name,
        ingest_directory_name,
        AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
    )

    adls_client.delete_directories(container_name, automated_test_output_directory_paths)

    yield context

    logger.info("AFTER SCENARIO: Deleting automated test output data.")

    automated_test_output_directory_paths = adls_client.filter_directory_paths(
        container_name,
        ingest_directory_name,
        AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
    )

    adls_client.delete_directories(container_name, automated_test_output_directory_paths)


@fixture
def azure_blob_config_prepare(context: Context, data_target_directory: str, data_local_directory: str):
    """Delete any existing files in the test directory of config blob storage, and upload test config files.

    Args:
        context: Behave context object
        data_target_directory: The test directory prefix to clear out and upload to
        data_local_directory: Directory where the test config files are stored.
    """
    blob_storage_client = BlobStorageClient(CONFIG_BLOB_ACCOUNT)

    target_directory = f"{AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX}/{data_target_directory}"

    logger.info(f"BEFORE SCENARIO: Deleting existing test config from {CONFIG_CONTAINER_NAME}/{target_directory}*.")
    blob_storage_client.delete_blob_prefix(CONFIG_CONTAINER_NAME, target_directory)

    logger.info(f"BEFORE SCENARIO: Uploading test config to {CONFIG_CONTAINER_NAME}/{target_directory}.")
    config_filepaths = [f for f in listdir(data_local_directory) if isfile(join(data_local_directory, f))]

    for file in config_filepaths:
        blob_storage_client.upload_file_to_blob(
            CONFIG_CONTAINER_NAME, target_directory, f"{data_local_directory}/{file}"
        )

    yield context

    logger.info("AFTER SCENARIO: Deleting test config.")
    blob_storage_client.delete_blob_prefix(CONFIG_CONTAINER_NAME, AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX)
