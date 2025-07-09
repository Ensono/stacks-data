import uuid

from pytest import fixture

TEST_DATA_DIR = "tests/data/"
TEST_CSV_DIR = TEST_DATA_DIR + "movies_dataset"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"


@fixture
def random_db_name():
    return f"db_{uuid.uuid4().hex}"


@fixture
def db_schema(spark, random_db_name):
    """Creates a database with a unique name and drops it after the test."""
    spark.sql(f"CREATE DATABASE {random_db_name}")

    yield random_db_name

    spark.sql(f"DROP DATABASE IF EXISTS {random_db_name} CASCADE")
