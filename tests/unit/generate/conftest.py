TEST_CONFIG_DIRECTORY = "tests/data/template_config/"
TEST_CONFIG_INGEST = TEST_CONFIG_DIRECTORY + "test_config_ingest.yml"
TEST_CONFIG_INGEST_OVERWRITE = TEST_CONFIG_DIRECTORY + "test_config_ingest_overwrite.yml"

EXPECTED_FILE_LIST = [
    "config/ingest_sources/ingest_config.json",
    "config/schema/ingest_config_schema.json",
    "data_factory/pipelines/arm_template.json",
    "data_factory/adf_datasets.tf",
    "data_factory/adf_linked_services.tf",
    "data_factory/adf_pipelines.tf",
    "data_factory/constraints.tf",
    "data_factory/data.tf",
    "data_factory/provider.tf",
    "data_factory/vars.tf",
    "tests/end_to_end/features/steps/__init__.py",
    "tests/end_to_end/features/steps/azure_data_ingest_steps.py",
    "tests/end_to_end/features/__init__.py",
    "tests/end_to_end/features/azure_data_ingest.feature",
    "tests/end_to_end/features/environment.py",
    "tests/end_to_end/__init__.py",
    "tests/unit/__init__.py",
    "de-ingest-ado-pipeline.yml",
    "README.md",
]

EXPECTED_DQ_FILE_LIST = [
    "config/data_quality/data_quality_config.json",
    "data_factory/pipelines/arm_template.json",
    "spark_jobs/data_quality.py",
    "de-ingest-ado-pipeline.yml",
    "README.md",
]
