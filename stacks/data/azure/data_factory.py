"""Azure utilities - Data Factory.

This module provides a collection of helper functions related to Azure Data Factory.
"""
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineRun, CreateRunResponse


def create_adf_pipeline_run(
    adf_client: DataFactoryManagementClient,
    resource_group_name: str,
    data_factory_name: str,
    pipeline_name: str,
    parameters: dict,
) -> CreateRunResponse:
    """Triggers an ADF pipeline.

    Args:
        adf_client: DataFactoryManagementClient
        resource_group_name: Resource Group Name
        data_factory_name: Data Factory Name
        pipeline_name: Pipeline Name
        parameters:Dictionary of parameters to pass in

    Returns:
        ADF run response
    """
    response = adf_client.pipelines.create_run(
        resource_group_name, data_factory_name, pipeline_name, parameters=parameters
    )
    return response


def get_adf_pipeline_run(
    adf_client: DataFactoryManagementClient,
    resource_group_name: str,
    data_factory_name: str,
    run_id: str,
) -> PipelineRun:
    """Gets information on a Data Factory pipeline run.

    Args:
        adf_client: DataFactoryManagementClient
        resource_group_name: Resource Group Name
        data_factory_name: Data Factory Name
        run_id: Data Factory pipeline run ID

    Returns:
        Data Factory pipeline run information
    """
    return adf_client.pipeline_runs.get(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        run_id=run_id,
    )


def check_adf_pipeline_in_complete_state(
    adf_client: DataFactoryManagementClient,
    resource_group_name: str,
    data_factory_name: str,
    run_id: str,
) -> bool:
    """Gets the pipeline run. Returns True if pipeline in completed state or False if pipeline not in complete state.

    Args:
        adf_client: DataFactoryManagementClient
        resource_group_name: Resource Group Name
        data_factory_name: Data Factory Name
        run_id: Data Factory pipeline run ID

    Returns:
        Boolean reflecting completion state
    """
    pipeline_run = adf_client.pipeline_runs.get(
        resource_group_name=resource_group_name, factory_name=data_factory_name, run_id=run_id
    )
    return pipeline_run.status in ["Succeeded", "Failed"]
