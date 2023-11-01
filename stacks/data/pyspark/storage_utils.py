"""Helper functions for interacting with Azure Data Lake Storage and Azure Blob Storage."""

import json
import logging

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

from stacks.data.constants import AZURE_CONFIG_ACCOUNT_NAME


logger = logging.getLogger(__name__)


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
