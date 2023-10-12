import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrame
from tests.unit.pyspark.conftest import BRONZE_CONTAINER, SILVER_CONTAINER, TEST_CSV_DIR

from datastacks.pyspark.etl import (
    read_latest_rundate_data,
    save_files_as_delta_tables,
    transform_and_save_as_delta,
    get_data_factory_param,
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
@patch("datastacks.pyspark.etl.get_adls_file_url")
def test_save_files_as_delta_tables(mock_get_adls_file_url, spark, csv_files, expected_columns, tmp_path):
    def side_effect(container, file_name):
        if container == BRONZE_CONTAINER:
            # fixed path for test input files
            return f"{TEST_CSV_DIR}/{file_name}"
        else:
            # temporary path for any test outputs
            return f"{tmp_path}/{file_name}"

    mock_get_adls_file_url.side_effect = side_effect

    spark_read_options = {"header": "true", "inferSchema": "true", "delimiter": ","}
    save_files_as_delta_tables(spark, csv_files, "csv", BRONZE_CONTAINER, SILVER_CONTAINER, spark_read_options)

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
@patch("datastacks.pyspark.etl.get_adls_file_url")
def test_save_files_as_delta_tables_different_formats(
    mock_get_adls_file_url, spark, tmp_path, file_format, write_options, read_options
):
    def side_effect(container, file_name):
        if container == BRONZE_CONTAINER:
            return f"{tmp_path}/{file_name}.{file_format}"
        else:
            return f"{tmp_path}/{file_name}"

    mock_get_adls_file_url.side_effect = side_effect

    sample_data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(sample_data, ["Name", "Score"])

    test_files = ["testfile1", "testfile2"]

    for file in test_files:
        filepath = side_effect(BRONZE_CONTAINER, file)
        df.write.options(**write_options).format(file_format).save(filepath)

    save_files_as_delta_tables(spark, test_files, file_format, BRONZE_CONTAINER, SILVER_CONTAINER, read_options)

    for file in test_files:
        expected_filepath = side_effect(SILVER_CONTAINER, file)
        df_read = spark.read.format("delta").load(expected_filepath)
        assert df_read.count() == len(sample_data)  # same number of rows
        assert df_read.columns == ["Name", "Score"]  # same column names


def test_read_latest_rundate_data(spark, tmp_path):
    def mock_get_adls_directory_contents(*args, **kwargs):
        return os.listdir(tmp_path)

    def mock_get_adls_file_url(container, path):
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
        "datastacks.pyspark.etl.get_adls_directory_contents", side_effect=mock_get_adls_directory_contents
    ), patch("datastacks.pyspark.etl.get_adls_file_url", side_effect=mock_get_adls_file_url):

        df = read_latest_rundate_data(spark, "dummy", str(tmp_path), "delta")

        assert df.columns == ["Id", "Name", "Directory"]
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0].Directory == "rundate=2023-08-18T090129.2247Z"


def test_transform_and_save_as_delta(spark, tmp_path):
    input_data = [(1, "Alice"), (2, "Bob")]
    input_df = spark.createDataFrame(input_data, ["Id", "Name"])

    def mock_transform(df: DataFrame) -> DataFrame:
        return df.withColumn("NewCol", df.Id + 1)

    output_file_name = "test_delta_table"
    expected_output_path = str(tmp_path / output_file_name)

    with patch("datastacks.pyspark.etl.get_adls_file_url", return_value=expected_output_path):
        transform_and_save_as_delta(spark, input_df, mock_transform, str(tmp_path), output_file_name)

    saved_df = spark.read.format("delta").load(expected_output_path)

    assert saved_df.columns == ["Id", "Name", "NewCol"]
    rows = saved_df.collect()
    assert len(rows) == 2
    assert rows[0].NewCol == 2
    assert rows[1].NewCol == 3


def test_get_data_factory_param():
    with patch.object(sys, "argv", ["script.py", "test_value"]):
        result = get_data_factory_param(1, "default_value")
        assert result == "test_value"


def test_get_data_factory_param_default():
    with patch.object(sys, "argv", ["script.py"]):
        result = get_data_factory_param(1, "default_value")
        assert result == "default_value"


def test_get_data_factory_param_bool():
    with patch.object(sys, "argv", ["script.py", "True"]):
        result = get_data_factory_param(1, default_value=False, convert_bool=True)
        assert result is True


def test_get_data_factory_param_bool_false():
    with patch.object(sys, "argv", ["script.py", "False"]):
        result = get_data_factory_param(1, default_value=True, convert_bool=True)
        assert result is False
