from pytest import fixture

from datastacks.pyspark.data_quality.config import Config
from datastacks.pyspark.data_quality.utils import create_datasource_context


@fixture
def dq_config(tmp_path) -> Config:
    gx_directory_path = str(tmp_path)

    json_config = {
        "gx_directory_path": gx_directory_path,
        "dataset_name": "movies_dataset",
        "dq_input_path": "abfss://test_db@{ADLS_ACCOUNT}.dfs.core.windows.net/ingest_azure_sql_example/",
        "dq_output_path": "abfss://test_db@{ADLS_ACCOUNT}.dfs.core.windows.net/ingest_azure_sql_example/",
        "datasource_config": [
            {
                "datasource_name": "movies_metadata",
                "datasource_type": "table",
                "data_location": "test_db.test_table",
                "expectation_suite_name": "movies_metadata_suite",
                "validation_config": [
                    {
                        "column_name": "test_column_1",
                        "expectations": [
                            {
                                "expectation_type": "expect_column_values_to_not_be_null",
                                "expectation_kwargs": {},
                            },
                            {
                                "expectation_type": "expect_column_values_to_be_of_type",
                                "expectation_kwargs": {"type_": "StringType"},
                            },
                        ],
                    },
                    {
                        "column_name": "test_column_2",
                        "expectations": [
                            {
                                "expectation_type": "expect_column_values_to_be_in_set",
                                "expectation_kwargs": {"value_set": [1, 2, 3]},
                            }
                        ],
                    },
                ],
            }
        ],
    }

    return Config.parse_obj(json_config)


@fixture
def datasource_context(spark, dq_config):
    # Note: Spark fixture is used to ensure proper Spark session settings. Otherwise, SparkSession
    # gets created when calling `create_datasource_context`, which doesn't meet test requirements.
    datasource_name = dq_config.datasource_config[0].datasource_name
    gx_directory_path = dq_config.gx_directory_path
    context = create_datasource_context(datasource_name, gx_directory_path)
    return context
