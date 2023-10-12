"""Helper functions for interacting with Azure Data Lake Storage and Azure Blob Storage."""
import json
import logging
import os
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

ENV_NAME_SERVICE_PRINCIPAL_SECRET = "AZURE_CLIENT_SECRET"
ENV_NAME_DIRECTORY_ID = "AZURE_TENANT_ID"
ENV_NAME_APPLICATION_ID = "AZURE_CLIENT_ID"
ENV_NAME_ADLS_ACCOUNT = "ADLS_ACCOUNT"
ENV_NAME_BLOB_ACCOUNT = "BLOB_ACCOUNT"


def check_env() -> None:
    """Checks if the environment variables for ADLS and Blob access are set.

    Raises:
        EnvironmentError: If any of the required environment variables are not set.
    """
    required_variables = [
        ENV_NAME_SERVICE_PRINCIPAL_SECRET,
        ENV_NAME_DIRECTORY_ID,
        ENV_NAME_APPLICATION_ID,
        ENV_NAME_ADLS_ACCOUNT,
        ENV_NAME_BLOB_ACCOUNT,
    ]

    missing_variables = [var for var in required_variables if var not in os.environ]

    if missing_variables:
        raise EnvironmentError("The following environment variables are not set: " + ", ".join(missing_variables))


def set_spark_properties(spark: SparkSession) -> None:
    """Sets Spark properties to configure Azure credentials to access Data Lake storage.

    Args:
        spark: Spark session.
    """
    adls_account = os.getenv(ENV_NAME_ADLS_ACCOUNT)
    spark.conf.set(f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net",
        os.getenv(ENV_NAME_APPLICATION_ID),
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net",
        os.getenv(ENV_NAME_SERVICE_PRINCIPAL_SECRET),
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{os.getenv(ENV_NAME_DIRECTORY_ID)}/oauth2/token",
    )


def get_adls_directory_contents(container: str, path: str, recursive: Optional[bool] = True) -> list[str]:
    """Gets the contents of a specified directory in an Azure Data Lake Storage container.

    Args:
        container: The name of the container in the ADLS account.
        path: The directory path within the container for which to list contents.
        recursive: If True, lists contents of all subdirectories recursively.

    Returns:
        A list of paths for the files and subdirectories within the specified container.
    """
    adls_account = os.getenv(ENV_NAME_ADLS_ACCOUNT)
    adls_url = f"https://{adls_account}.dfs.core.windows.net"
    adls_client = DataLakeServiceClient(account_url=adls_url, credential=DefaultAzureCredential())
    file_system_client = adls_client.get_file_system_client(file_system=container)

    paths = file_system_client.get_paths(path=path, recursive=recursive)
    paths = [path.name for path in paths]
    logger.debug(f"ADLS directory contents: {paths}")
    return paths


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


def get_adls_file_url(container: str, file_name: str) -> str:
    """Constructs an Azure Data Lake Storage (ADLS) URL for a specific file.

    Args:
        container: The name of the ADLS container.
        file_name: The name of the file (including any subdirectories within the container).

    Returns:
        Full ADLS URL for the specified file.
    """
    adls_account = os.getenv(ENV_NAME_ADLS_ACCOUNT)
    return f"abfss://{container}@{adls_account}.dfs.core.windows.net/{file_name}"


def get_blob_url() -> str:
    """Constructs the URL for a Blob storage account on Azure.

    The name of the Blob storage account is acquired from an environment variable.

    Returns:
        The URL for the Blob service.
    """
    blob_account = os.getenv(ENV_NAME_BLOB_ACCOUNT)
    return f"https://{blob_account}.blob.core.windows.net"
