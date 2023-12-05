import os

# Azure environment variables
AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID")
AZURE_SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID")
AZURE_RESOURCE_GROUP_NAME = os.environ.get("AZURE_RESOURCE_GROUP_NAME")
AZURE_DATA_FACTORY_NAME = os.environ.get("AZURE_DATA_FACTORY_NAME")
AZURE_REGION_NAME = os.environ.get("AZURE_REGION_NAME")
ADLS_ACCOUNT = os.environ.get("ADLS_ACCOUNT")
CONFIG_BLOB_ACCOUNT = os.environ.get("CONFIG_BLOB_ACCOUNT")

# Azure client auth
AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")

# ADLS constants
BRONZE_CONTAINER_NAME = "raw"
SILVER_CONTAINER_NAME = "staging"
GOLD_CONTAINER_NAME = "curated"

# Config storage constants
CONFIG_CONTAINER_NAME = "config"

# Automated test config
AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX = "automated_test"

# Default Spark config
DEFAULT_SPARK_CONFIG = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-azure:3.3.4,io.delta:delta-core_2.12:2.4.0",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}
