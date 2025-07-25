import pytest

from stacks.data.platforms.fabric.lakehouse import LakehouseClient


def test_get_file_url():
    workspace_id = "workspace-123e4567-e89b"
    lakehouse_id = "lakehouse-08d36aeb-12d3"
    table_path = "Tables/dbo/my_table"

    lakehouse_client = LakehouseClient(workspace_id, lakehouse_id)

    expected_url = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{table_path}"
    assert lakehouse_client.get_file_url(table_path) == expected_url


def test_get_table_url_with_schema():
    workspace_id = "workspace-123e4567-e89b"
    lakehouse_id = "lakehouse-08d36aeb-12d3"
    table_name = "my_table"
    schema = "dbo"

    lakehouse_client = LakehouseClient(workspace_id, lakehouse_id)

    expected_url = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema}/{table_name}"
    )
    actual_url_with_schema_provided = lakehouse_client.get_table_url(table_name, schema)
    actual_url_with_default_schema = lakehouse_client.get_table_url(table_name)

    assert actual_url_with_schema_provided == expected_url
    assert actual_url_with_default_schema == expected_url


def test_get_table_url_without_schema():
    workspace_id = "workspace-123e4567-e89b"
    lakehouse_id = "lakehouse-08d36aeb-12d3"
    table_name = "my_table"

    lakehouse_client = LakehouseClient(workspace_id, lakehouse_id)

    expected_url = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"
    actual_url = lakehouse_client.get_table_url(table_name, None)

    assert actual_url == expected_url


def test_get_account_url():
    workspace_id = "workspace-123e4567-e89b"
    lakehouse_id = "lakehouse-08d36aeb-12d3"
    lakehouse_client = LakehouseClient(workspace_id, lakehouse_id)
    expected_url = "https://onelake.dfs.fabric.microsoft.com"
    assert lakehouse_client.get_account_url() == expected_url


@pytest.mark.parametrize("directory_path", ["/path/to/directory", "path/to/directory/"])
def test_get_full_directory_path(directory_path):
    lakehouse_id = "lakehouse-08d36aeb-12d3"
    workspace_id = "workspace-123e4567-e89b"
    client = LakehouseClient(workspace_id, lakehouse_id)
    expected_path = f"{lakehouse_id}/path/to/directory"
    assert client._get_full_directory_path(directory_path) == expected_path
