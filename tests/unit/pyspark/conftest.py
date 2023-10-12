import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

from pytest import fixture

from datastacks.pyspark.pyspark_utils import get_spark_session

TEST_DATA_DIR = Path(__file__).parent.resolve() / "data"
TEST_CSV_DIR = TEST_DATA_DIR / "movies_dataset"
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
def mock_adls_client():
    with patch("datastacks.pyspark.storage_utils.DataLakeServiceClient") as mock_DataLakeServiceClient:

        def get_paths_side_effect(path, recursive=True):
            test_path = Path(TEST_CSV_DIR)
            files_and_dirs = test_path.rglob("*") if recursive else test_path.glob("*")

            mock_paths = []
            for item in files_and_dirs:
                mock_path = MagicMock(spec_set=["name"])
                mock_path.name = str(item.relative_to(test_path))
                mock_paths.append(mock_path)

            return mock_paths

        mock_file_system_client = MagicMock()
        mock_file_system_client.get_paths.side_effect = get_paths_side_effect
        mock_DataLakeServiceClient.return_value.get_file_system_client.return_value = mock_file_system_client
        yield mock_DataLakeServiceClient


@fixture
def json_contents():
    file_path = TEST_DATA_DIR / "test_config.json"
    with open(file_path, "r") as file:
        file_contents = file.read()

    yield file_contents


@fixture
def mock_blob_client(json_contents):
    with patch("datastacks.pyspark.storage_utils.BlobServiceClient") as mock_BlobServiceClient:
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
