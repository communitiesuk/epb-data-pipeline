from pyspark.sql import SparkSession


def create_iceberg_table(spark, glue_table_path, s3_path, logger):
    logger.info("Creating Iceberg table if it does not exist.")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {glue_table_path}
    USING iceberg
    LOCATION '{s3_path}'
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.compression.codec' = 'zstd',
        'optimize_rewrite_delete_file_threshold'='5'
    )
    """)

    logger.info("Iceberg table ensured.")


def get_postgres_columns_and_types(spark: SparkSession, postgres_table_name: str):
    postgres_table_schema = spark.table(postgres_table_name).schema
    postgres_columns_with_types = {
        f.name: f.dataType.simpleString() for f in postgres_table_schema.fields
    }
    return postgres_columns_with_types


def dynamically_update_catalog_table(
        glue_spark, postgres_columns_with_types, GLUE_TABLE_PATH, logger
):
    logger.info("Synchronizing Iceberg table schema with PostgreSQL schema.")
    postgres_columns = postgres_columns_with_types.keys()

    glue_table_schema = glue_spark.table(GLUE_TABLE_PATH).schema
    glue_columns = [f.name for f in glue_table_schema.fields]

    # Identify new columns
    new_columns = set(postgres_columns) - set(glue_columns)
    for column_name in new_columns:
        col_type = postgres_columns_with_types[column_name]
        logger.info(f"Adding new column → {column_name}: {col_type}")

        glue_spark.sql(f"""
            ALTER TABLE {GLUE_TABLE_PATH}
            ADD COLUMN {column_name} {col_type}
        """)

    # Identify columns to drop
    drop_columns = set(glue_columns) - set(postgres_columns)
    for column_name in drop_columns:
        logger.info(f"Dropping column → {column_name}")
        glue_spark.sql(f"""
          ALTER TABLE {GLUE_TABLE_PATH}
          DROP COLUMN {column_name}
          """)


def iceberg_merge(
        spark, columns, catalog_table_name, glue_table_path, db_table_name, logger
):
    logger.info("Performing MERGE INTO operation on Iceberg table.")

    column_list = ", ".join(columns)
    value_list = ", ".join([f"source.{col}" for col in columns])

    if "_rr" in catalog_table_name:
        recommendation_item_name = (
            "improvement_item"
            if catalog_table_name == "domestic_rr"
            else "recommendation_item"
        )
        spark.sql(f"""
        MERGE INTO {glue_table_path} AS target
        USING {db_table_name} AS source
        ON (target.certificate_number = source.certificate_number AND target.{recommendation_item_name} = source.{recommendation_item_name})
        WHEN MATCHED
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT ({column_list}) VALUES ({value_list})
        """)
    else:
        spark.sql(f"""
        MERGE INTO {glue_table_path} AS target
        USING {db_table_name} AS source
        ON target.certificate_number = source.certificate_number
        WHEN MATCHED
                    THEN UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT ({column_list}) VALUES ({value_list})
        """)