"""Behave Shared Steps.

This module contains a collection of reusable Behave steps, designed to be used in tests running against Azure data
workloads.
"""
import json
import uuid
from datetime import datetime

import polling2
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.filedatalake import DataLakeServiceClient
from behave import given, step, then
from behave.runner import Context

from stacks.data.constants import (
    ADLS_URL,
    AZURE_SUBSCRIPTION_ID,
    AZURE_DATA_FACTORY_NAME,
    AZURE_RESOURCE_GROUP_NAME,
    AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX,
)
from stacks.data.azure.data_factory import (
    check_adf_pipeline_in_complete_state,
    get_adf_pipeline_run,
    create_adf_pipeline_run,
)
from stacks.data.azure.adls import all_files_present_in_adls

credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, AZURE_SUBSCRIPTION_ID)
adls_client = DataLakeServiceClient(account_url=ADLS_URL, credential=credential)


@given("the ADF pipeline {pipeline_name} has been triggered with {parameters}")
def trigger_adf_pipeline(context: Context, pipeline_name: str, parameters: str):
    """Trigger an Azure Data Factory pipeline with the provided parameters.

    Args:
        context: Behave context object.
        pipeline_name: The name of the Azure Data Factory pipeline.
        parameters: JSON-encoded string containing the parameters for the pipeline.
    """
    context.start_time = datetime.now()
    parameters = json.loads(parameters)
    test_run_id = f"{AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX}_{uuid.uuid4()}"
    context.test_run_id = test_run_id
    parameters.update({"run_id": test_run_id})

    run_response = create_adf_pipeline_run(
        adf_client,
        AZURE_RESOURCE_GROUP_NAME,
        AZURE_DATA_FACTORY_NAME,
        pipeline_name,
        parameters=parameters,
    )
    context.adf_run_id = run_response.run_id


@step("I poll the pipeline every {seconds} seconds until it has completed")
def poll_adf_pipeline(context: Context, seconds: str):
    """Poll the Azure Data Factory pipeline until it completes or times out.

    Args:
        context: Behave context object.
        seconds: The polling interval in seconds.
    """
    polling2.poll(
        lambda: check_adf_pipeline_in_complete_state(
            adf_client,
            AZURE_RESOURCE_GROUP_NAME,
            AZURE_DATA_FACTORY_NAME,
            context.adf_run_id,
        ),
        step=int(seconds),
        timeout=900,
    )


@step("the ADF pipeline {pipeline_name} has finished with state {state}")
def pipeline_has_finished_with_state(context: Context, pipeline_name: str, state: str):
    """Check if the Azure Data Factory pipeline has finished with the expected state.

    Args:
        context: Behave context object.
        pipeline_name: Name of the pipeline.
        state: The expected state of the pipeline.
    """
    pipeline_run = get_adf_pipeline_run(
        adf_client, AZURE_RESOURCE_GROUP_NAME, AZURE_DATA_FACTORY_NAME, context.adf_run_id
    )
    assert pipeline_run.status == state


@then(
    "the files {output_files} are present in the ADLS container {container_name} in the directory " "{directory_name}"
)
def check_all_files_present_in_adls(context: Context, output_files: str, container_name: str, directory_name: str):
    """Check if all the specified files are present in the specified ADLS container and directory.

    Args:
        context: Behave context object.
        output_files: JSON-encoded list of expected file names.
        container_name: The name of the ADLS container.
        directory_name: The directory path in the container.
    """
    expected_files_list = json.loads(output_files)
    test_directory_name = f"{directory_name}/automated_tests/{context.test_run_id}"
    assert all_files_present_in_adls(adls_client, container_name, test_directory_name, expected_files_list)


@step("the ADF pipeline {pipeline_name} completed in less than {seconds} seconds")
def check_adf_pipeline_completion_time(context: Context, pipeline_name: str, seconds: str):
    """Check if the Azure Data Factory pipeline completed within the specified time.

    Args:
        context: Behave context object.
        pipeline_name: Name of the pipeline.
        seconds: The maximum allowed duration in seconds.
    """
    end_time = datetime.now()
    time_diff = (end_time - context.start_time).total_seconds()
    assert time_diff < float(seconds)
