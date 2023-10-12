"""Azure utilities - Blob.

This module provides a collection of helper functions related to Azure Blob Storage.
"""
import logging
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def upload_file_to_blob(
    blob_service_client: BlobServiceClient,
    container_name: str,
    target_dir: str,
    local_file_path: str,
    overwrite: bool = True,
):
    """Upload a file to blob storage.

    Args:
        blob_service_client: BlobServiceClient
        container_name: Container name
        target_dir: Directory to load into
        local_file_path: Path to the file to upload
        overwrite: Overwrite the file if it already exists
    """
    file_name = local_file_path.rsplit("/", 1)[-1]
    target_blob_path = f"{target_dir}/{file_name}"
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(target_blob_path)

    with open(local_file_path, "rb") as file:
        blob_client.upload_blob(file, overwrite=overwrite)
    logger.info(f"Uploaded {local_file_path} to {container_name}/{target_blob_path}.")


def delete_blob_prefix(blob_service_client: BlobServiceClient, container_name: str, blob_prefix: str) -> bool:
    """Delete files with a given prefix from blob storage.

    Args:
        blob_service_client: BlobServiceClient
        container_name: Container name
        blob_prefix: Blob prefix in the container to delete
    """
    container_client = blob_service_client.get_container_client(container_name)
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
