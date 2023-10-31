"""Helper functions for interacting with Azure Data Lake Storage and Azure Blob Storage."""

import json
import logging
import os

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from pyspark.sql import SparkSession

from stacks.data.constants import (
    AZURE_TENANT_ID,
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_CONFIG_ACCOUNT_NAME,
)


logger = logging.getLogger(__name__)


def check_env() -> None:
    """Checks if the environment variables for ADLS and Blob access are set.

    Raises:
        EnvironmentError: If any of the required environment variables are not set.
    """
    required_variables = [
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_CONFIG_ACCOUNT_NAME",
    ]

    missing_variables = [var_name for var_name in required_variables if os.environ.get(var_name) is None]

    if missing_variables:
        raise EnvironmentError("The following environment variables are not set: " + ", ".join(missing_variables))


def set_spark_properties(spark: SparkSession) -> None:
    """Sets Spark properties to configure Azure credentials to access Data Lake storage.

    Args:
        spark: Spark session.
    """
    spark.conf.set(f"fs.azure.account.auth.type.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        AZURE_CLIENT_ID,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        AZURE_CLIENT_SECRET,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/token",
    )


def load_json_from_blob(container: str, file_path: str) -> dict:
    """Load a JSON file from an Azure blob storage.

    Args:
        container: The name of the Azure blob storage container.
        file_path: Path to the JSON file in a given container.

    Returns:
        The contents of the JSON file as a dictionary.

    Example:
        >>> load_json_from_blob("mycontainer", "mydirectory/mydata.json")

    """
    blob_url = get_blob_url()
    blob_service_client = BlobServiceClient(account_url=blob_url, credential=DefaultAzureCredential())
    blob_client = blob_service_client.get_blob_client(container, file_path)

    blob_content = blob_client.download_blob().readall()
    return json.loads(blob_content)


def get_blob_url() -> str:
    """Constructs the URL for a Blob storage account on Azure.

    The name of the Blob storage account is acquired from an environment variable.

    Returns:
        The URL for the Blob service.
    """
    return f"https://{AZURE_CONFIG_ACCOUNT_NAME}.blob.core.windows.net"
