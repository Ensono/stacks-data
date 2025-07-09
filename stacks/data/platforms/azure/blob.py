"""Azure utilities - Blob Storage.

This module provides a collection of helper functions related to Azure Blob Storage.
"""
import json
import logging

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


class BlobStorageClient:
    def __init__(self, storage_account_name: str):
        """Instantiate a new Blob Storage Client.

        Args:
            storage_account_name: Name of the storage account
        """
        self.storage_account_name = storage_account_name
        self.account_blob_url = f"https://{storage_account_name}.blob.core.windows.net"
        self.credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(account_url=self.account_blob_url, credential=self.credential)

    def upload_file_to_blob(
        self,
        container_name: str,
        target_dir: str,
        local_file_path: str,
        overwrite: bool = True,
    ):
        """Upload a file to blob storage.

        Args:
            container_name: Container name
            target_dir: Directory to load into
            local_file_path: Path to the file to upload
            overwrite: Overwrite the file if it already exists
        """
        file_name = local_file_path.rsplit("/", 1)[-1]
        target_blob_path = f"{target_dir}/{file_name}"
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(target_blob_path)

        with open(local_file_path, "rb") as file:
            blob_client.upload_blob(file, overwrite=overwrite)
        logger.info(f"Uploaded {local_file_path} to {container_name}/{target_blob_path}.")

    def delete_blob_prefix(self, container_name: str, blob_prefix: str) -> bool:
        """Delete files with a given prefix from blob storage.

        Args:
            container_name: Container name
            blob_prefix: Blob prefix in the container to delete
        """
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=blob_prefix)
        if not blob_list:
            logger.info(f"No blobs exist with prefix {blob_prefix} in container {container_name}")
        else:
            try:
                for blob in blob_list:
                    logger.info(f"Deleting {blob.name}")
                    container_client.delete_blob(blob.name)
                logger.info(f"All blobs with prefix {blob_prefix} deleted successfully from container {container_name}")
            except Exception as e:
                logger.warning(f"Error deleting directory '{blob_prefix}': {str(e)}")
                return False
        return True

    def load_json_from_blob(self, container: str, file_path: str) -> dict:
        """Load a JSON file from an Azure blob storage.

        Args:
            container: The name of the Azure blob storage container.
            file_path: Path to the JSON file in a given container.

        Returns:
            The contents of the JSON file as a dictionary.

        Example:
            >>> load_json_from_blob("mycontainer", "mydirectory/mydata.json")

        """
        blob_client = self.blob_service_client.get_blob_client(container, file_path)
        blob_content = blob_client.download_blob().readall()
        return json.loads(blob_content)
