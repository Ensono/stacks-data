"""Azure utilities for ADLS.

This module provides a client for interacting with Azure Data Lake Storage (ADLS) Gen2,
extending the base DatalakeClient.
"""
import logging

from stacks.data.azure.datalake.base import DatalakeClient

logger = logging.getLogger(__name__)


class AdlsClient(DatalakeClient):
    def __init__(self, storage_account_name: str):
        """Instantiate a new ADLS Client.

        Args:
            storage_account_name: Name of the storage account
        """
        super().__init__(storage_account_name)

    def get_account_url(self) -> str:
        """Returns the account URL for the Azure Data Lake Storage (ADLS) service."""
        return f"https://{self.storage_account_name}.dfs.core.windows.net"

    def get_file_url(self, file_system: str, file_name: str) -> str:
        """Returns an Azure Data Lake Storage (ADLS) URL for a specific file.

        Args:
            file_system: Container name.
            file_name: The name of the file (including any subdirectories within the container).

        Returns:
            Full ADLS URL for the specified file.
        """
        return f"abfss://{file_system}@{self.storage_account_name}.dfs.core.windows.net/{file_name}"
