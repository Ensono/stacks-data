import os

# Azure environment variables
AZURE_SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID")
AZURE_RESOURCE_GROUP_NAME = os.environ.get("AZURE_RESOURCE_GROUP_NAME")
AZURE_DATA_FACTORY_NAME = os.environ.get("AZURE_DATA_FACTORY_NAME")
AZURE_REGION_NAME = os.environ.get("AZURE_REGION_NAME")
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONFIG_ACCOUNT_NAME = os.environ.get("AZURE_CONFIG_ACCOUNT_NAME")

# ADLS constants
BRONZE_CONTAINER_NAME = "raw"
SILVER_CONTAINER_NAME = "staging"
GOLD_CONTAINER_NAME = "curated"
ADLS_URL = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

# Config storage constants
CONFIG_CONTAINER_NAME = "config"
CONFIG_BLOB_URL = f"https://{AZURE_CONFIG_ACCOUNT_NAME}.blob.core.windows.net"

# Automated test config
AUTOMATED_TEST_OUTPUT_DIRECTORY_PREFIX = "automated_test"
