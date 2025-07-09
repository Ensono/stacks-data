"""Fabric Data Factory Utilities.

This module provides a class for interacting with the Microsoft Fabric REST API,
specifically for triggering and polling pipelines.

"""
import logging

import requests
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)


class DataPipeline:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """Initialize the DataFactory client with service principal credentials."""
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = None

    def get_access_token(self) -> str:
        """Get an access token for the Fabric API using the service principal."""
        if self._access_token is None:
            credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
            token = credential.get_token("https://api.fabric.microsoft.com/.default")
            self._access_token = token.token
        return self._access_token

    def invalidate_token(self) -> None:
        """Invalidate the cached access token."""
        self._access_token = None

    def get_auth_header(self) -> dict:
        """Returns the Authorization header for requests to Fabric API."""
        token = self.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def trigger_pipeline(self, workspace_id: str, pipeline_id: str, payload: dict = None) -> None:
        """Trigger a Fabric pipeline."""
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}"
            f"/jobs/instances?jobType=Pipeline"
        )
        headers = self.get_auth_header()
        headers["Content-Type"] = "application/json"
        default_payload = {"executionData": {"parameters": {"param_waitsec": "60"}}}
        response = requests.post(url, headers=headers, json=payload or default_payload)
        if response.status_code not in (200, 202):
            print("❌ Error triggering pipeline:")
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            print(
                "Response JSON: "
                f"{response.json() if response.headers.get('Content-Type') == 'application/json' else 'N/A'}"
            )
            response.raise_for_status()

    def poll_pipeline_until_complete(
        self, workspace_id: str, pipeline_id: str, interval: int = 10, timeout: int = 900
    ) -> tuple[str, int | None]:
        """Poll the Fabric pipeline run status every `interval` seconds until it completes or times out.

        Args:
            workspace_id: The ID of the Fabric workspace.
            pipeline_id: The ID of the Fabric pipeline.
            interval: Number of seconds between polling attempts. Defaults to 10.
            timeout: Maximum time in seconds to poll before timing out. Defaults to 900.

        Returns:
            A tuple containing the final status (str) and the duration in seconds (int) of the pipeline run.
            Returns (None, None) if no runs are found.
        """
        import time

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?$top=1"
        headers = self.get_auth_header()
        print(f"⏳ Polling pipeline {pipeline_id} in workspace {workspace_id}...")
        start_time = time.time()
        while True:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            runs = response.json().get("value", [])
            if not runs:
                print("⚠️ No pipeline runs found.")
                return None, None
            latest_run = runs[0]
            status = latest_run.get("status")
            duration = latest_run.get("duration") or int(time.time() - start_time)
            print(f"Status: {status}")
            if status in ["Succeeded", "Failed", "Cancelled", "Completed"]:
                return status, duration
            if time.time() - start_time > timeout:
                print("❌ Polling timed out.")
                return status, duration
            time.sleep(interval)
