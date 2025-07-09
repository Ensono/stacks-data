"""ETL Transformation Utilities for Data Pipelines.

This module provides a collection of helper functions tailored for various ETL tasks in data pipelines. Specifically
designed to simplify complex operations, these functions streamline the transformation process between different data
layers, such as Bronze-to-Silver or Silver-to-Gold.
"""
import logging
import os
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame, SparkSession

from stacks.data.platforms.fabric.lakehouse import LakehouseClient
from stacks.data.pyspark.pyspark_utils import get_spark_session, save_dataframe_as_delta

logger = logging.getLogger(__name__)


class LocalSparkSession:
    def __init__(self, app_name: str, spark_config: dict[str, Any] = None):
        """Initiate a Spark session with a configured ADLS Client.

        Args:
            app_name: Name of the Spark application.
            spark_config: A dictionary with additional Spark configuration options to set.
        """
        self.app_name = app_name
        self.spark_config = spark_config or {}
        self.spark_session = self.get_spark_session()

    def set_spark_config(self) -> None:
        """Sets Spark properties to access Fabric Lakehouse from a local Spark session."""
        local_config = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": os.getenv("FABRIC_CLIENT_ID"),
            "fs.azure.account.oauth2.client.secret": os.getenv("FABRIC_CLIENT_SECRET"),
            "fs.azure.account.oauth2.client.endpoint": (
                f"https://login.microsoftonline.com/{os.getenv('FABRIC_TENANT_ID')}/oauth2/token"
            ),
        }
        self.spark_config.update(local_config)

    def get_spark_session(self) -> SparkSession:
        """Retrieve a SparkSession configured for Fabric Lakehouse access.

        This function first checks that the required environment variables for Lakehouse are set. If they are,
        it then gets (or creates) a SparkSession and configures its properties to allow for Lakehouse access using
        the set environment variables.

        Returns:
            Configured Spark session for Lakehouse access.

        Raises:
            EnvironmentError: If any of the required environment variables for Lakehouse access are not set.

        """
        self.check_env()
        self.set_spark_config()
        return get_spark_session(self.app_name, self.spark_config)

    def check_env(self) -> None:
        """Checks if the environment variables for Lakehouse are set.

        Raises:
            EnvironmentError: If any of the required environment variables are not set.
        """
        required_variables = [
            "FABRIC_TENANT_ID",
            "FABRIC_CLIENT_ID",
            "FABRIC_CLIENT_SECRET",
        ]

        missing_variables = [var_name for var_name in required_variables if not os.environ.get(var_name)]

        if missing_variables:
            raise EnvironmentError("The following environment variables are not set: " + ", ".join(missing_variables))


def transform_and_save_as_delta(
    spark: SparkSession,
    lakehouse_client: LakehouseClient,
    input_df: DataFrame,
    transform_func: Callable[[DataFrame], DataFrame],
    target_table: str,
    target_schema: Optional[str] = "dbo",
    overwrite: bool = True,
    merge_keys: Optional[list[str]] = None,
) -> None:
    """Transforms an input dataframe using a provided transformation function and saves the result as a Delta table.

    Args:
        spark: Spark session.
        lakehouse_client: Lakehouse Client.
        input_df: Data frame to be transformed.
        transform_func: Transformation function.
        target_table: Name of the destination table in Lakehouse.
        target_schema: Optional schema for the destination table.
        overwrite: Flag to determine whether to overwrite the entire table or perform an upsert.
        merge_keys: List of keys based on which upsert will be performed.

    """
    transformed_df = transform_func(input_df)
    output_filepath = lakehouse_client.get_table_url(target_table, target_schema)
    save_dataframe_as_delta(spark, transformed_df, output_filepath, overwrite, merge_keys)
