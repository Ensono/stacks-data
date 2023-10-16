"""ETL Transformation Utilities for Data Pipelines.

This module provides a collection of helper functions tailored for various ETL tasks in data pipelines. Specifically
designed to simplify complex operations, these functions streamline the transformation process between different data
layers, such as Bronze-to-Silver or Silver-to-Gold.
"""
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from dateutil.parser import isoparse
from pyspark.sql import DataFrame, SparkSession

from stacks.data.pyspark.pyspark_utils import get_spark_session, read_datasource, save_dataframe_as_delta
from stacks.data.pyspark.storage_utils import (
    check_env,
    get_adls_directory_contents,
    get_adls_file_url,
    set_spark_properties,
)

logger = logging.getLogger(__name__)


@dataclass
class TableTransformation:
    table_name: str
    transformation_function: Callable[[DataFrame], DataFrame]


def save_files_as_delta_tables(
    spark: SparkSession,
    input_files: list[str],
    datasource_type: str,
    source_container: str,
    target_container: str,
    spark_read_options: Optional[dict[str, Any]] = None,
) -> None:
    """Saves multiple data files as Delta tables.

    This function reads multiple data files of a specified format from a source container and writes them as Delta
    tables into a target container. The name of each Delta table is derived from its corresponding input file name,
    excluding the file extension.

    Args:
        spark: Spark session.
        input_files: List of file paths within the bronze container to be converted into Delta tables.
        datasource_type: Source format that Spark can read from, e.g. delta, table, parquet, json, csv.
        source_container: Name of the source container in ADLS.
        target_container: Name of the destination container in ADLS.
        spark_read_options: Options to pass to the DataFrameReader.
    """
    logger.info("Saving input files as delta tables...")
    for file in input_files:
        filepath = get_adls_file_url(source_container, file)
        df = read_datasource(spark, filepath, datasource_type, spark_read_options)
        filename_with_no_extension = Path(filepath).stem
        output_filepath = get_adls_file_url(target_container, filename_with_no_extension)
        save_dataframe_as_delta(spark, df, output_filepath)


def get_spark_session_for_adls(app_name: str, spark_config: dict[str, Any] = None) -> SparkSession:
    """Retrieve a SparkSession configured for Azure Data Lake Storage access.

    This function first checks that the required environment variables for ADLS are set. If they are, it then gets
    (or creates) a SparkSession and configures its properties to allow for ADLS access using the set environment
    variables.

    Args:
        app_name: Name of the Spark application.
        spark_config: A dictionary with additional Spark configuration options to set.

    Returns:
        Configured Spark session for ADLS access.

    Raises:
        EnvironmentError: If any of the required environment variables for ADLS access are not set.

    """
    check_env()
    spark = get_spark_session(app_name, spark_config)
    set_spark_properties(spark)
    return spark


def read_latest_rundate_data(
    spark: SparkSession,
    container_name: str,
    datasource_path: str,
    datasource_type: str,
    spark_read_options: Optional[dict[str, Any]] = None,
) -> DataFrame:
    """Reads the most recent data, based on rundate, from an ADLS location and returns it as a dataframe.

    The function identifies the latest rundate by parsing directory names in the given path and then reads the
    corresponding data. The rundate directories should follow the format: "rundate=<date_string_in_ISO-8601>",
    e.g. "rundate=YYYY-MM-DDTHH:MM:SS" or "rundate=YYYY-MM-DDTHHMMSS.FFFFFFZ".

    Args:
        spark: Spark session.
        container_name: Name of the ADLS container.
        datasource_path: Directory path within the ADLS container where rundate directories are located.
        datasource_type: Source system type that Spark can read from, e.g. delta, table, parquet, json, csv.
        spark_read_options: Options to pass to the DataFrameReader.

    Returns:
        The dataframe loaded from the datasource with the most recent rundate, with metadata columns dropped.

    """
    logger.info(f"Reading dataset: {datasource_path}")
    dirname_prefix = "rundate="
    metadata_columns = ["meta_ingestion_datetime", "meta_ingestion_pipeline", "meta_ingestion_run_id"]
    directories = get_adls_directory_contents(container_name, datasource_path, recursive=False)
    rundates = [directory.split(dirname_prefix)[1] for directory in directories]
    most_recent_rundate = max(rundates, key=isoparse)
    logger.info(f"Latest rundate: {most_recent_rundate}")
    latest_path = Path(datasource_path) / (dirname_prefix + most_recent_rundate)
    dataset_url = get_adls_file_url(container_name, str(latest_path))
    return read_datasource(spark, dataset_url, datasource_type, spark_read_options).drop(*metadata_columns)


def transform_and_save_as_delta(
    spark: SparkSession,
    input_df: DataFrame,
    transform_func: Callable[[DataFrame], DataFrame],
    target_container: str,
    output_file_name: str,
    overwrite: bool = True,
    merge_keys: Optional[list[str]] = None,
) -> None:
    """Transforms an input dataframe using the provided transformation function and saves the result as a Delta table.

    Args:
        spark: Spark session.
        input_df: Data frame to be transformed.
        transform_func: Transformation function.
        target_container: Name of the destination container in ADLS.
        output_file_name: The name of the file (including any subdirectories within the container).
        overwrite: Flag to determine whether to overwrite the entire table or perform an upsert.
        merge_keys: List of keys based on which upsert will be performed.

    """
    transformed_df = transform_func(input_df)
    output_filepath = get_adls_file_url(target_container, output_file_name)
    save_dataframe_as_delta(spark, transformed_df, output_filepath, overwrite, merge_keys)


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
