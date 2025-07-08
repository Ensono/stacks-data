import uuid

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
def random_db_name():
    return f"db_{uuid.uuid4().hex}"


@fixture
def db_schema(spark, random_db_name):
    """Creates a database with a unique name and drops it after the test."""
    spark.sql(f"CREATE DATABASE {random_db_name}")

    yield random_db_name

    spark.sql(f"DROP DATABASE IF EXISTS {random_db_name} CASCADE")
