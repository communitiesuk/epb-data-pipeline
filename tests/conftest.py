import logging
import os
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# Load environment variables from .env if present
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip().strip("'\"")



@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Sets up a local Spark session configured with Iceberg Hadoop Catalog."""
    warehouse_path = str(tmp_path_factory.mktemp("iceberg_warehouse"))

    spark_session = (
        SparkSession.builder.appName("IcebergUnitTests")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def logger():
    class Logger:
        def info(self, message):
            return