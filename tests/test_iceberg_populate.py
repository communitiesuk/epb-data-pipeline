# tests/test_schema_sync.py
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from scripts.populate_iceberg import (
    get_postgres_columns_and_types,
    dynamically_update_catalog_table,
    iceberg_merge,
)

logger = logging.getLogger("__name__")


def test_get_postgres_columns_and_types(spark):
    data = [("CERT-123", "10 High St", 2023), ("CERT-456", "11 Low St", 2024)]
    columns = ["certificate_number", "address", "year"]

    df = spark.createDataFrame(data, columns)
    df.createOrReplaceTempView("mock_postgres_table")

    result = get_postgres_columns_and_types(spark, "mock_postgres_table")

    assert result == {
        "certificate_number": "string",
        "address": "string",
        "year": "bigint",
    }


def test_dynamically_update_catalog_table(spark):
    table_path = "local.default.test_schema_table"

    spark.sql(f"CREATE TABLE {table_path} (id INT, old_col STRING) USING iceberg")

    source_columns = {"id": "int", "new_col": "string"}

    dynamically_update_catalog_table(spark, source_columns, table_path, logger)

    updated_schema = spark.table(table_path).schema.fieldNames()
    assert "new_col" in updated_schema
    assert "old_col" not in updated_schema
    assert "id" in updated_schema


def test_perform_iceberg_merge_standard(spark, tmp_path):
    table_path = "local.default.test_merge"
    dwh_table = "source_view"

    spark.sql(
        f"CREATE TABLE {table_path} (certificate_number STRING, data STRING) USING iceberg"
    )
    spark.sql(f"INSERT INTO {table_path} VALUES ('123', 'old_data')")

    schema = StructType(
        [
            StructField("certificate_number", StringType()),
            StructField("data", StringType()),
        ]
    )
    source_df = spark.createDataFrame(
        [("123", "new_data"), ("456", "inserted_data")], schema
    )
    source_df.createOrReplaceTempView(dwh_table)

    columns = ["certificate_number", "data"]

    iceberg_merge(spark, columns, "test_merge", table_path, dwh_table, logger)

    results = spark.table(table_path).orderBy("certificate_number").collect()
    assert len(results) == 2
    assert results[0]["data"] == "new_data"
    assert results[1]["data"] == "inserted_data"


def test_perform_iceberg_merge_rr_logic(spark):
    table_path = "local.default.domestic_rr"
    dwh_table = "source_view_rr"

    spark.sql(
        f"CREATE TABLE {table_path} (certificate_number STRING, improvement_item STRING, value INT) USING iceberg"
    )
    spark.sql(f"INSERT INTO {table_path} VALUES ('123', 'itemA', 10)")

    schema = StructType(
        [
            StructField("certificate_number", StringType()),
            StructField("improvement_item", StringType()),
            StructField("value", IntegerType()),
        ]
    )
    source_df = spark.createDataFrame(
        [("123", "itemA", 20), ("123", "itemB", 15)], schema
    )
    source_df.createOrReplaceTempView(dwh_table)

    columns = ["certificate_number", "improvement_item", "value"]

    iceberg_merge(spark, columns, "domestic_rr", table_path, dwh_table, logger)

    results = spark.table(table_path).orderBy("improvement_item").collect()

    assert len(results) == 2
    assert results[0]["value"] == 20
    assert results[1]["value"] == 15