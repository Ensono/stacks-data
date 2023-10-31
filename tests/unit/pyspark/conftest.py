import uuid
from unittest.mock import MagicMock, patch

from pytest import fixture

from stacks.data.pyspark.pyspark_utils import get_spark_session

TEST_DATA_DIR = "tests/data/"
TEST_CSV_DIR = TEST_DATA_DIR + "movies_dataset"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"


@fixture(scope="session")
def spark(tmp_path_factory):
    """Spark session fixture with a temporary directory as a Spark warehouse."""
    temp_dir = tmp_path_factory.mktemp("spark-warehouse")
    spark_config = {"spark.sql.warehouse.dir": temp_dir}
    spark = get_spark_session("pyspark-test", spark_config)

    yield spark

    spark.stop()


@fixture
def json_contents():
    file_path = TEST_DATA_DIR + "data_quality/test_dq_config.json"
    with open(file_path, "r") as file:
        file_contents = file.read()

    yield file_contents


@fixture
def mock_blob_client(json_contents):
    with patch("stacks.data.pyspark.storage_utils.BlobServiceClient") as mock_BlobServiceClient:
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = json_contents
        mock_BlobServiceClient.return_value.get_blob_client.return_value = mock_blob_client

        yield mock_BlobServiceClient


@fixture
def random_db_name():
    return f"db_{uuid.uuid4().hex}"


@fixture
def db_schema(spark, random_db_name):
    """Creates a database with a unique name and drops it after the test."""
    spark.sql(f"CREATE DATABASE {random_db_name}")

    yield random_db_name

    spark.sql(f"DROP DATABASE IF EXISTS {random_db_name} CASCADE")
