"""Azure utilities for Lakehouse.

This module provides a client for interacting with Microsoft Fabric Lakehouse, extending the base DatalakeClient.
"""
import logging
import sys

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from stacks.data.azure.datalake.base import DatalakeClient

logger = logging.getLogger(__name__)


def get_credential() -> TokenCredential | DefaultAzureCredential:
    """Returns a credential for accessing Lakehouse.

    If running in a Microsoft Fabric, it uses `mssparkutils` to get the token. Otherwise, it defaults to
    `DefaultAzureCredential`.

    Returns:
        A TokenCredential instance if running in Microsoft Fabric, otherwise None.

    """
    try:
        from notebookutils import mssparkutils
        from azure.core.credentials import TokenCredential, AccessToken

        class FabricTokenCredential(TokenCredential):
            def get_token(self, *args, **kwargs) -> AccessToken:
                token = mssparkutils.credentials.getToken("https://storage.azure.com/")
                return AccessToken(token, sys.maxsize)

        return FabricTokenCredential()

    except ImportError:
        return DefaultAzureCredential()


class LakehouseClient(DatalakeClient):
    def __init__(self, storage_account_name: str = "onelake"):
        """Instantiate a new Lakehouse Client.

        Args:
            storage_account_name: Name of the storage account
        """
        super().__init__(storage_account_name, get_credential())

    def get_account_url(self) -> str:
        """Returns the account URL for the Microsoft Fabric Lakehouse service."""
        return f"https://{self.storage_account_name}.dfs.fabric.microsoft.com"

    def get_file_url(self, file_system: str, lakehouse_id: str, file_name: str) -> str:
        """Returns a Lakehouse URL for a specific file.

        Args:
            file_system: Workspace ID.
            lakehouse_id: Lakehouse ID.
            file_name: The name of the file (including any subdirectories within the lakehouse).

        Returns:
            Full Lakehouse URL for the specified file.
        """
        return f"abfss://{file_system}@{self.storage_account_name}.dfs.fabric.microsoft.com/{lakehouse_id}/{file_name}"
