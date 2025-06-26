"""Azure utilities - Data Factory.

This module provides a collection of helper functions related to Azure Data Factory.
"""
import logging
import sys

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineRun, CreateRunResponse

logger = logging.getLogger(__name__)


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


def get_data_factory_param(param_position: int, default_value: str | bool = None, convert_bool: bool = False):
    """Gets parameters passed from Data Factory Python activities.

    The values held in the parameters are expected to be strings - the convert_bool argument can be used to convert
    strings back to bool type (where the string "True" will return True).

    Args:
        param_position: The ordinal position of the parameter passed to the Python activity.
        default_value: Default value to return if the parameter is not found.
        convert_bool: Convert the parameter to a bool based on string value of "True".

    Returns:
        The parameter value passed from Data Factory (via sys.argv) or the default parameter value.

    """
    if len(sys.argv) <= param_position:
        logger.warning("Excepted arguments from Data Factory not found, using default value.")
        return default_value
    else:
        data_factory_param = sys.argv[param_position]
        if convert_bool:
            return data_factory_param == "True"
        else:
            return data_factory_param
