# Default Spark config for the Delta Lake access.
SPARK_CONFIG_FOR_DELTA_INTEGRATION = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

SPARK_PACKAGES_FOR_DELTA_INTEGRATION = [
    "org.apache.hadoop:hadoop-azure:3.4.1",
    "org.apache.hadoop:hadoop-common:3.4.1",
]
