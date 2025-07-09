import os
from pathlib import Path
from unittest.mock import Mock, patch
from azure.storage.filedatalake import FileSystemClient, PathProperties

import pytest
from pyspark.sql import DataFrame
from tests.unit.pyspark.conftest import BRONZE_CONTAINER, SILVER_CONTAINER, TEST_CSV_DIR

from stacks.data.platforms.azure.adls import AdlsClient
from stacks.data.platforms.azure.etl import (
    EtlSession,
    read_latest_rundate_data,
    save_files_as_delta_tables,
    transform_and_save_as_delta,
    set_spark_properties,
)

TEST_ENV_VARS_FULL = {
    "AZURE_TENANT_ID": "dir_id",
    "AZURE_CLIENT_ID": "app_id",
    "AZURE_CLIENT_SECRET": "secret",
    "ADLS_ACCOUNT": "myadlsaccount",
    "CONFIG_BLOB_ACCOUNT": "myblobaccount",
}

TEST_ENV_VARS_PARTIAL = {
    "AZURE_TENANT_ID": "dir_id",
    "AZURE_CLIENT_ID": "app_id",
    "AZURE_CLIENT_SECRET": "secret",
}


@pytest.fixture
def mock_adls_client():
    with patch("stacks.data.platforms.common.datalake.DataLakeServiceClient", autospec=True) as mock_service_client:

        def get_paths_side_effect(path, recursive=True):
            test_path = Path(TEST_CSV_DIR)
            files_and_dirs = list(test_path.rglob("*")) if recursive else list(test_path.glob("*"))

            mock_paths = []
            for item in files_and_dirs:
                mock_path = Mock(spec=PathProperties)
                mock_path.name = str(item.relative_to(test_path))
                mock_path.is_directory = item.is_dir()
                mock_paths.append(mock_path)

            return mock_paths

        mock_client = AdlsClient("teststorageaccount")
        mock_file_system_client = Mock(spec=FileSystemClient)
        mock_file_system_client.get_paths.side_effect = get_paths_side_effect
        mock_service_client.return_value.get_file_system_client.return_value = mock_file_system_client

        yield mock_client


@pytest.fixture
def mock_etl_session(mock_adls_client, spark):
    with patch("stacks.data.platforms.azure.etl.EtlSession.get_spark_session_for_adls", return_value=spark), patch(
        "stacks.data.platforms.azure.etl.AdlsClient", return_value=mock_adls_client
    ):
        etl_session = EtlSession("pyspark-test")
        yield etl_session


@patch.dict("os.environ", TEST_ENV_VARS_FULL, clear=True)
def test_check_env(mock_etl_session):
    mock_etl_session.check_env()


@patch.dict("os.environ", {}, clear=True)
def test_check_env_missing_vars(mock_etl_session):
    with pytest.raises(EnvironmentError):
        mock_etl_session.check_env()


@patch.dict("os.environ", TEST_ENV_VARS_PARTIAL, clear=True)
def test_check_env_raises_partial_vars(mock_etl_session):
    with pytest.raises(EnvironmentError) as excinfo:
        mock_etl_session.check_env()
    assert "ADLS_ACCOUNT" in str(excinfo.value)
    assert "CONFIG_BLOB_ACCOUNT" in str(excinfo.value)


@patch.dict("os.environ", TEST_ENV_VARS_FULL, clear=True)
def test_set_spark_properties(spark):
    set_spark_properties(spark)
    adls_account = os.getenv("ADLS_ACCOUNT")
    assert spark.conf.get(f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net") == "OAuth"
    assert (
        spark.conf.get(f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net")
        == "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    assert spark.conf.get(f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net") == os.getenv(
        "AZURE_CLIENT_ID"
    )
    assert spark.conf.get(f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net") == os.getenv(
        "AZURE_CLIENT_SECRET"
    )
    assert (
        spark.conf.get(f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net")
        == f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}/oauth2/token"
    )


@pytest.mark.parametrize(
    "csv_files,expected_columns",
    [
        (
            ["links.csv", "ratings.csv"],
            [["movieId", "imdbId", "tmdbId"], ["userId", "movieId", "rating", "timestamp"]],
        ),
    ],
)
@patch("stacks.data.platforms.azure.adls.AdlsClient.get_file_url")
def test_save_files_as_delta_tables(mock_get_file_url, mock_adls_client, spark, csv_files, expected_columns, tmp_path):
    def side_effect(container, file_name):
        if container == BRONZE_CONTAINER:
            # fixed path for test input files
            return f"{TEST_CSV_DIR}/{file_name}"
        else:
            # temporary path for any test outputs
            return f"{tmp_path}/{file_name}"

    mock_get_file_url.side_effect = side_effect

    spark_read_options = {"header": "true", "inferSchema": "true", "delimiter": ","}
    save_files_as_delta_tables(
        spark, mock_adls_client, csv_files, "csv", BRONZE_CONTAINER, SILVER_CONTAINER, spark_read_options
    )

    for i, csv_file in enumerate(csv_files):
        filename_with_no_extension = Path(csv_file).stem
        expected_filepath = side_effect(SILVER_CONTAINER, filename_with_no_extension)
        df = spark.read.format("delta").load(expected_filepath)
        assert df is not None
        assert df.count() > 0
        assert df.columns == expected_columns[i]


@pytest.mark.parametrize(
    "file_format,write_options,read_options",
    [
        ("csv", {"header": "true", "sep": ","}, {"header": "true", "inferSchema": "true"}),
        ("parquet", {}, {}),
        ("json", {}, {}),
        ("delta", {}, {}),
    ],
)
@patch("stacks.data.platforms.azure.adls.AdlsClient.get_file_url")
def test_save_files_as_delta_tables_different_formats(
    mock_get_file_url, mock_adls_client, spark, tmp_path, file_format, write_options, read_options
):
    def side_effect(container, file_name):
        if container == BRONZE_CONTAINER:
            return f"{tmp_path}/{file_name}.{file_format}"
        else:
            return f"{tmp_path}/{file_name}"

    mock_get_file_url.side_effect = side_effect

    sample_data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(sample_data, ["Name", "Score"])

    test_files = ["testfile1", "testfile2"]

    for file in test_files:
        filepath = side_effect(BRONZE_CONTAINER, file)
        df.write.options(**write_options).format(file_format).save(filepath)

    save_files_as_delta_tables(
        spark, mock_adls_client, test_files, file_format, BRONZE_CONTAINER, SILVER_CONTAINER, read_options
    )

    for file in test_files:
        expected_filepath = side_effect(SILVER_CONTAINER, file)
        df_read = spark.read.format("delta").load(expected_filepath)
        assert df_read.count() == len(sample_data)  # same number of rows
        assert df_read.columns == ["Name", "Score"]  # same column names


def test_read_latest_rundate_data(spark, mock_adls_client, tmp_path):
    def mock_get_directory_contents(*args, **kwargs):
        return os.listdir(tmp_path)

    def mock_get_file_url(container, path):
        return path

    rundates = ["2023-04-01T12:34:56Z", "2023-08-18T090129.2247Z", "2023-08-15T130850.3738696Z"]
    directories = [f"rundate={date}" for date in rundates]

    # Create a Delta table in each directory
    for idx, dir in enumerate(directories, start=1):
        data_path = tmp_path / dir
        data_path.mkdir()
        data = [(idx, "Alice", dir, "2023-08-24 12:34:56", "pipeline", f"runid{idx}")]
        df = spark.createDataFrame(
            data,
            ["Id", "Name", "Directory", "meta_ingestion_datetime", "meta_ingestion_pipeline", "meta_ingestion_run_id"],
        )
        df.write.format("delta").mode("overwrite").save(str(data_path))

    with patch(
        "stacks.data.platforms.azure.adls.AdlsClient.get_directory_contents", side_effect=mock_get_directory_contents
    ), patch("stacks.data.platforms.azure.adls.AdlsClient.get_file_url", side_effect=mock_get_file_url):

        df = read_latest_rundate_data(spark, mock_adls_client, "dummy", str(tmp_path), "delta")

        assert df.columns == ["Id", "Name", "Directory"]
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0].Directory == "rundate=2023-08-18T090129.2247Z"


def test_transform_and_save_as_delta(spark, mock_adls_client, tmp_path):
    input_data = [(1, "Alice"), (2, "Bob")]
    input_df = spark.createDataFrame(input_data, ["Id", "Name"])

    def mock_transform(df: DataFrame) -> DataFrame:
        return df.withColumn("NewCol", df.Id + 1)

    output_file_name = "test_delta_table"
    expected_output_path = str(tmp_path / output_file_name)

    with patch("stacks.data.platforms.azure.adls.AdlsClient.get_file_url", return_value=expected_output_path):
        transform_and_save_as_delta(spark, mock_adls_client, input_df, mock_transform, str(tmp_path), output_file_name)

    saved_df = spark.read.format("delta").load(expected_output_path)

    assert saved_df.columns == ["Id", "Name", "NewCol"]
    rows = saved_df.collect()
    assert len(rows) == 2
    assert rows[0].NewCol == 2
    assert rows[1].NewCol == 3
