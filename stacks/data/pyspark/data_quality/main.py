"""Data Quality Main.

This module is the entrypoint for executing Data Quality processing against datasets, based upon provided configuration.
"""
import logging

from stacks.data.platforms.azure.constants import CONFIG_CONTAINER_NAME
from stacks.data.pyspark.data_quality.config import Config
from stacks.data.pyspark.data_quality.utils import (
    add_expectation_suite,
    create_datasource_context,
    execute_validations,
    publish_quality_results_table,
    replace_adls_data_location,
)

from stacks.data.platforms.azure.etl import EtlSession
from stacks.data.pyspark.pyspark_utils import read_datasource
from stacks.data.utils import substitute_env_vars

logger = logging.getLogger(__name__)


def data_quality_main(
    workload_name: str,
    config_path: str,
    container_name: str = CONFIG_CONTAINER_NAME,
    test_flag: bool = False,
    test_run_id: str = "default_run_id",
    test_data_adls_path: str = None,
):
    """Executes data quality checks based on the provided configuration.

    Args:
        workload_name: Name of the workload, for naming the Spark application.
        config_path: Path to a JSON config inside an Azure Blob container.
        container_name: Name of the container for storing configurations.
        test_flag: Flag if the process is being run as part of automated tests.
        test_run_id: Used to name the output folder if the process is being run as part of automated tests.
        test_data_adls_path: Override the ADLS input path of the data being tested if required for automated tests.

    Raises:
        EnvironmentError: if any of the required environment variables for ADLS access are not set.

    """
    etl_session = EtlSession(f"DataQuality-{workload_name}")
    spark = etl_session.spark_session
    blob_storage_client = etl_session.blob_storage_client

    dq_conf_dict = blob_storage_client.load_json_from_blob(container_name, config_path)
    dq_conf = Config.parse_obj(dq_conf_dict)
    logger.info(f"Running Data Quality processing for dataset: {dq_conf.dataset_name}...")

    if test_flag and test_data_adls_path:
        dq_input_path = replace_adls_data_location(dq_conf.dq_input_path, test_data_adls_path)
    else:
        dq_input_path = dq_conf.dq_input_path

    dq_output_path = substitute_env_vars(dq_conf.dq_output_path)

    for datasource in dq_conf.datasource_config:
        logger.info(f"Checking DQ for datasource: {datasource.datasource_name}...")
        if datasource.datasource_type == "table":
            data_location = datasource.data_location
        else:
            data_location = dq_input_path + datasource.data_location

        df = read_datasource(spark, data_location, datasource.datasource_type)

        gx_context = create_datasource_context(datasource.datasource_name, dq_conf.gx_directory_path)
        gx_context = add_expectation_suite(gx_context, datasource)

        validation_result = execute_validations(gx_context, datasource, df)
        results = validation_result.results

        data_quality_run_date = validation_result.meta["run_id"].run_time

        if test_flag:
            full_dq_output_path = f"{dq_output_path}automated_tests/{test_run_id}/{datasource.datasource_name}_dq/"
        else:
            full_dq_output_path = f"{dq_output_path}{datasource.datasource_name}_dq/"

        logger.info(f"DQ check completed for {datasource.datasource_name}. Results:")
        logger.info(results)

        failed_validations = publish_quality_results_table(
            spark, full_dq_output_path, datasource.datasource_name, results, data_quality_run_date
        ).cache()

        if not failed_validations.rdd.isEmpty():
            logger.info(
                f"Checking {datasource.datasource_name}, {failed_validations.count()} validations failed. "
                f"See {full_dq_output_path} for details."
            )
        else:
            logger.info(f"Checking {datasource.datasource_name}, All validations passed.")

    logger.info("Finished: Data Quality processing.")
