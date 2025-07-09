from stacks.data.platforms.azure.adls import AdlsClient


def test_get_file_url():
    adls_client = AdlsClient("teststorageaccount")
    container = "mycontainer"
    file_name = "myfolder/myfile.txt"
    expected_url = "abfss://mycontainer@teststorageaccount.dfs.core.windows.net/myfolder/myfile.txt"

    assert adls_client.get_file_url(container, file_name) == expected_url


def test_get_account_url():
    test_storage_account_name = "teststorageaccount"
    adls_client = AdlsClient(test_storage_account_name)
    expected_url = f"https://{test_storage_account_name}.dfs.core.windows.net"
    assert adls_client.get_account_url() == expected_url
