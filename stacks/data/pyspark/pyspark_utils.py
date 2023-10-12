"""Spark common utilities."""
import logging
import os
from typing import Any, Optional
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException

from pyspark.sql import DataFrame, SparkSession

from stacks.data.utils import camel_to_snake, substitute_env_vars

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str, spark_config: dict[str, Any] = None) -> SparkSession:
    """Creates a SparkSession with the specified application name or retrieves one that already exists.

    The SparkSession is configured to include Hadoop Azure and Delta Lake packages. If a SparkSession is already active,
    it will be returned instead of creating a new one.

    Args:
        app_name: Name of the Spark application.
        spark_config: A dictionary with additional Spark configuration options to set.

    Returns:
        Spark session with Delta Lake as the catalog implementation.

    """
    spark = SparkSession.getActiveSession()
    if not spark:
        logger.info("Creating Spark session...")

        config = {
            "spark.jars.packages": "org.apache.hadoop:hadoop-azure:3.3.4,io.delta:delta-core_2.12:2.4.0",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }

        if spark_config:
            config.update(spark_config)

        spark_builder = SparkSession.builder.appName(app_name)

        for key, value in config.items():
            spark_builder = spark_builder.config(key, value)

        spark = spark_builder.getOrCreate()

        logger.info("Spark session created.")

    return spark


def read_datasource(
    spark: SparkSession, data_location: str, datasource_type: str, options: Optional[dict[str, Any]] = None
) -> DataFrame:
    """Reads the data from the given location and returns it as a dataframe.

    Args:
        spark: Spark session.
        data_location: Location of the given data asset. It can either be a path to the data file or a fully qualified
            table name, depending on the datasource type.
        datasource_type: Source system type that Spark can read from, e.g. delta, table, parquet, json, csv.
        options: Optional dictionary of options to pass to the DataFrameReader.

    Returns:
        The dataframe loaded from the datasource.

    Examples:
        If the data is stored in a file, you should provide the complete path to the file, e.g.:

        >>> read_datasource(spark, "abfss://silver_movies_example@{ADLS_ACCOUNT}.dfs.core.windows.net/myfolder",
       ...     "delta")
        >>> read_datasource(spark, "abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net/myfolder/mysubfolder/*",
        ...     "parquet", {"mergeSchema": "true"})
        >>> read_datasource(spark, "abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net/myfolder",
        ...     "csv", {"inferSchema": "true", "delimiter": ","})

        For tables with metadata managed by a data catalog, you should provide the database schema and the table name:

        >>> read_datasource(spark, "staging.movies_metadata", "table")

    """
    data_location = substitute_env_vars(data_location)

    if options is None:
        options = {}

    if datasource_type.lower() == "delta":
        return spark.read.options(**options).format("delta").load(data_location)
    else:
        source_type = getattr(spark.read.options(**options), datasource_type)
        return source_type(data_location)


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Checks if the Delta table exists at the specified path."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except AnalysisException:
        return False


def save_dataframe_as_delta(
    spark: SparkSession,
    dataframe: DataFrame,
    output_filepath: str,
    overwrite: bool = True,
    merge_keys: Optional[list[str]] = None,
) -> None:
    """Saves a Spark DataFrame as a Delta table at a specified location.

    This function can either overwrite the entire table or perform an upsert based on specified merge keys.

    Args:
        spark: Spark session.
        dataframe: Spark DataFrame to save as the Delta table.
        output_filepath: The location to write the Delta table.
        overwrite: Flag to determine whether to overwrite the entire table or perform an upsert.
        merge_keys: List of keys based on which upsert will be performed.

    Example:
        >>> output_url = "abfss://silver_movies_example@{ADLS_ACCOUNT}.dfs.core.windows.net/mytable"
        >>> save_dataframe_as_delta(dataframe, output_url, overwrite=True)
        >>> save_dataframe_as_delta(dataframe, output_url, overwrite=False, merge_keys=["id", "timestamp"])

    """
    table_name = os.path.basename(output_filepath)
    logger.info(f"Saving delta table {table_name}...")

    if overwrite or not delta_table_exists(spark, output_filepath):
        dataframe.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_filepath)
    else:
        delta_table = DeltaTable.forPath(SparkSession.builder.getOrCreate(), output_filepath)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        delta_table.alias("target").merge(
            dataframe.alias("source"), merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"Saved: {output_filepath}.")


def ensure_database_exists(spark: SparkSession, schema: str) -> None:
    """Ensures that a specific database exists in the Spark session, creating it if necessary.

    Args:
        spark: Spark session.
        schema: The name of the database/schema to ensure existence of.
    """
    if not spark.catalog.databaseExists(schema):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        logger.info(f"Database {schema} has been created.")


def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    """Rename all columns of a DataFrame from camelCase to snake_case.

    Args:
        df: A PySpark DataFrame.

    Returns:
        The DataFrame with columns renamed to snake_case.
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, camel_to_snake(col))
    return df
