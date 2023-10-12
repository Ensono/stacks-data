import pytest
from pyspark.sql import Row

from datastacks.pyspark.pyspark_utils import (
    delta_table_exists,
    ensure_database_exists,
    read_datasource,
    rename_columns_to_snake_case,
    save_dataframe_as_delta,
)


@pytest.mark.parametrize(
    "file_format,read_options,write_options",
    [
        ("delta", {}, {}),
        ("parquet", {}, {}),
        ("json", {}, {}),
        ("csv", {"header": "true", "inferSchema": "true"}, {"header": "true"}),
    ],
)
def test_read_datasource(spark, tmp_path, file_format, read_options, write_options):
    sample_data = [Row(col_a=1, col_b=4, col_c="p"), Row(col_a=2, col_b=5, col_c="q"), Row(col_a=3, col_b=6, col_c="r")]

    df = spark.createDataFrame(sample_data)

    output_path = str(tmp_path / f"testfile.{file_format}")

    df.write.options(**write_options).format(file_format).save(output_path)

    df_read = read_datasource(spark, output_path, file_format, read_options)

    assert df_read.count() == len(sample_data)  # same number of rows
    assert len(df_read.columns) == len(sample_data[0])  # same number of columns


def test_read_datasource_as_table(spark):
    sample_data = [Row(col_a=1, col_b=4, col_c="p"), Row(col_a=2, col_b=5, col_c="q"), Row(col_a=3, col_b=6, col_c="r")]

    df = spark.createDataFrame(sample_data)

    table_name = "test_table"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    df_read = read_datasource(spark, table_name, "table")

    assert df_read.count() == len(sample_data)  # same number of rows
    assert len(df_read.columns) == len(sample_data[0])  # same number of columns


@pytest.mark.parametrize(
    "create_table,expected_result",
    [
        (True, True),  # Scenario when the Delta table exists
        (False, False),  # Scenario when the Delta table doesn't exist
    ],
)
def test_delta_table_exists(spark, tmp_path, create_table, expected_result):
    path = str(tmp_path / "delta_table")

    if create_table:
        dataframe = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["Id", "Name"])
        dataframe.write.format("delta").save(path)

    assert delta_table_exists(spark, path) == expected_result


@pytest.mark.parametrize(
    "initial_data, new_data, overwrite, merge_keys, expected_data",
    [
        # Table doesn't exist and should be created.
        ([], [(1, "Alice"), (2, "Bob")], False, None, [(1, "Alice"), (2, "Bob")]),
        # Overwriting the entire table.
        ([(1, "Alicia"), (2, "Robert")], [(1, "Alice"), (2, "Bob")], True, None, [(1, "Alice"), (2, "Bob")]),
        # Upsert with matching and non-matching rows.
        (
            [(1, "Alicia"), (3, "Charlie")],
            [(1, "Alice"), (2, "Bob")],
            False,
            ["Id"],
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        ),
    ],
)
def test_save_dataframe_as_delta(spark, tmp_path, initial_data, new_data, overwrite, merge_keys, expected_data):
    output_path = str(tmp_path)
    if initial_data:
        initial_dataframe = spark.createDataFrame(initial_data, ["Id", "Name"])
        initial_dataframe.write.format("delta").mode("overwrite").save(output_path)

    dataframe_to_save = spark.createDataFrame(new_data, ["Id", "Name"])
    save_dataframe_as_delta(spark, dataframe_to_save, output_path, overwrite, merge_keys)

    loaded_dataframe = spark.read.format("delta").load(output_path)
    loaded_data = sorted(loaded_dataframe.collect(), key=lambda row: row.Id)
    expected_data = sorted([Row(Id=id, Name=name) for id, name in expected_data], key=lambda row: row.Id)

    assert loaded_data == expected_data


def test_ensure_database_exists(spark, db_schema):
    # Ensure an existing database remains accessible
    assert spark.catalog.databaseExists(db_schema)
    ensure_database_exists(spark, db_schema)
    assert spark.catalog.databaseExists(db_schema)

    spark.sql(f"DROP DATABASE {db_schema}")

    # Ensure a database is recreated
    assert not spark.catalog.databaseExists(db_schema)
    ensure_database_exists(spark, db_schema)
    assert spark.catalog.databaseExists(db_schema)


def test_rename_columns_to_snake_case(spark):
    row = Row("movieId", "imdbId", "tmdbId")
    test_df = spark.createDataFrame([row(1, 2, 3)])

    renamed_df = rename_columns_to_snake_case(test_df)

    assert renamed_df.columns == ["movie_id", "imdb_id", "tmdb_id"]
