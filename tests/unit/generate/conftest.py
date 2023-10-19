TEST_CONFIG_DIRECTORY = "tests/data/template_config/"

INGEST_EXPECTED_FILES = [
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

INGEST_DQ_FILES = [
    "config/data_quality/data_quality_config.json",
    "data_factory/pipelines/arm_template.json",
    "spark_jobs/data_quality.py",
    "de-ingest-ado-pipeline.yml",
    "README.md",
]

PROCESS_EXPECTED_FILES = [
    "data_factory/pipelines/arm_template.json",
    "data_factory/adf_pipelines.tf",
    "data_factory/constraints.tf",
    "data_factory/data.tf",
    "data_factory/provider.tf",
    "data_factory/vars.tf",
    "spark_jobs/__init__.py",
    "spark_jobs/process.py",
    "tests/end_to_end/__init__.py",
    "tests/unit/__init__.py",
    "tests/unit/conftest.py",
    "tests/unit/test_processing.py",
    "de-process-ado-pipeline.yml",
    "README.md",
]

PROCESS_DQ_FILES = [
    "config/data_quality/data_quality_config.json",
    "data_factory/pipelines/arm_template.json",
    "spark_jobs/data_quality.py",
    "de-process-ado-pipeline.yml",
    "README.md",
]
