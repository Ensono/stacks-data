import pytest
import sys
from unittest.mock import Mock, patch
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import CreateRunResponse, PipelineRun
from stacks.azure.data_factory import create_adf_pipeline_run, get_adf_pipeline_run, get_data_factory_param


@pytest.fixture
def mock_adf_client():
    mock_adf_client = Mock(spec=DataFactoryManagementClient)
    mock_adf_client.pipelines = Mock()
    mock_adf_client.pipeline_runs = Mock()
    return mock_adf_client


def test_create_adf_pipeline_run(mock_adf_client):
    mock_create_run = Mock(return_value=CreateRunResponse(run_id="12345678"))
    mock_adf_client.pipelines.create_run = mock_create_run

    resource_group_name = "test_resource_group"
    data_factory_name = "test_data_factory"
    pipeline_name = "test_pipeline"
    parameters = {"param1": "value1", "param2": "value2"}

    result = create_adf_pipeline_run(
        mock_adf_client,
        resource_group_name,
        data_factory_name,
        pipeline_name,
        parameters,
    )

    assert result.run_id == "12345678"
    mock_create_run.assert_called_once_with(
        resource_group_name, data_factory_name, pipeline_name, parameters=parameters
    )


def test_get_adf_pipeline_run(mock_adf_client):
    mock_pipeline_runs_get = Mock(return_value=PipelineRun())
    mock_adf_client.pipeline_runs.get = mock_pipeline_runs_get

    resource_group_name = "test_resource_group"
    data_factory_name = "test_data_factory"
    run_id = "12345678"

    get_adf_pipeline_run(
        mock_adf_client,
        resource_group_name,
        data_factory_name,
        run_id,
    )

    mock_pipeline_runs_get.assert_called_once_with(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        run_id=run_id,
    )


def test_get_data_factory_param():
    with patch.object(sys, "argv", ["script.py", "test_value"]):
        result = get_data_factory_param(1, "default_value")
        assert result == "test_value"


def test_get_data_factory_param_default():
    with patch.object(sys, "argv", ["script.py"]):
        result = get_data_factory_param(1, "default_value")
        assert result == "default_value"


def test_get_data_factory_param_bool():
    with patch.object(sys, "argv", ["script.py", "True"]):
        result = get_data_factory_param(1, default_value=False, convert_bool=True)
        assert result is True


def test_get_data_factory_param_bool_false():
    with patch.object(sys, "argv", ["script.py", "False"]):
        result = get_data_factory_param(1, default_value=True, convert_bool=True)
        assert result is False
