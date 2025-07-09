from unittest.mock import patch

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame

from stacks.data.platforms.fabric.etl import (
    LocalSparkSession,
    transform_and_save_as_delta,
)
from stacks.data.platforms.fabric.lakehouse import LakehouseClient

TEST_ENV_VARS_FULL = {
    "FABRIC_TENANT_ID": "tenant_id",
    "FABRIC_CLIENT_ID": "app_id",
    "FABRIC_CLIENT_SECRET": "secret",
}

TEST_ENV_VARS_PARTIAL = {
    "FABRIC_TENANT_ID": "tenant_id",
    "FABRIC_CLIENT_ID": "app_id",
}


@pytest.fixture
def mock_spark_session(spark):
    with patch("stacks.data.platforms.fabric.etl.LocalSparkSession.get_spark_session", return_value=spark):
        spark_session = LocalSparkSession("pyspark-test")
        yield spark_session


@patch.dict("os.environ", TEST_ENV_VARS_FULL, clear=True)
def test_check_env(mock_spark_session):
    mock_spark_session.check_env()


@patch.dict("os.environ", {}, clear=True)
def test_check_env_missing_vars(mock_spark_session):
    with pytest.raises(EnvironmentError):
        mock_spark_session.check_env()


@patch.dict("os.environ", TEST_ENV_VARS_PARTIAL, clear=True)
def test_check_env_raises_partial_vars(mock_spark_session):
    with pytest.raises(EnvironmentError) as excinfo:
        mock_spark_session.check_env()
    assert "FABRIC_CLIENT_SECRET" in str(excinfo.value)


def test_transform_and_save_as_delta(spark, tmp_path):
    output_path = str(tmp_path)
    workspace_id = "test_workspace"
    lakehouse_id = "test_lakehouse"
    lakehouse_client = LakehouseClient(workspace_id, lakehouse_id)
    input_data = [(1, "Alice"), (2, "Bob")]
    input_df = spark.createDataFrame(input_data, ["Id", "Name"])

    def mock_transform(df: DataFrame) -> DataFrame:
        return df.withColumn("NewCol", df.Id + 1)

    expected_data = [(1, "Alice", 2), (2, "Bob", 3)]
    expected_df = spark.createDataFrame(expected_data, ["Id", "Name", "NewCol"])

    with patch("stacks.data.platforms.fabric.lakehouse.LakehouseClient.get_table_url", return_value=output_path):
        transform_and_save_as_delta(spark, lakehouse_client, input_df, mock_transform, "table_name")

    actual_df = spark.read.format("delta").load(output_path)

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, underline_cells=True)
