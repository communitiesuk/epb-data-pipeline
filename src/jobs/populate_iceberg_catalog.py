import boto3
import sys

from pathlib import Path
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from scripts.populate_iceberg import (
    get_postgres_columns_and_types,
    dynamically_update_catalog_table,
    iceberg_merge,
    create_iceberg_table,
)

if __name__ == "__main__":
    # -------------------------------------------------------
    #               PARSE JOB ARGUMENTS
    # -------------------------------------------------------

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATABASE_NAME",
            "S3_BUCKET",
            "CONNECTION_NAME",
            "CATALOG_TABLE_NAME",
            "DB_TABLE_NAME",
        ],
    )

    DATABASE_NAME = args["DATABASE_NAME"]
    S3_BUCKET = args["S3_BUCKET"]
    CONNECTION_NAME = args["CONNECTION_NAME"]
    CATALOG_TABLE_NAME = args["CATALOG_TABLE_NAME"]
    DWH_TABLE_NAME = args["DB_TABLE_NAME"]

    S3_PATH = f"s3://{S3_BUCKET}/{CATALOG_TABLE_NAME}/"
    GLUE_TABLE_PATH = f"glue_catalog.{DATABASE_NAME}.{CATALOG_TABLE_NAME}"

    try:
        optional_arg = getResolvedOptions(sys.argv, ["ENABLE_OPTIMIZERS_AFTER_JOB"])
        ENABLE_OPTIMIZERS_AFTER_JOB = (
            optional_arg["ENABLE_OPTIMIZERS_AFTER_JOB"].lower() == "true"
        )
    except Exception:
        ENABLE_OPTIMIZERS_AFTER_JOB = True

    # -------------------------------------------------------
    #               SETUP SPARK + GLUE CONTEXT
    # -------------------------------------------------------
    sc = SparkContext()
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger.info("Glue job initialized successfully.")

    # -------------------------------------------------------
    #        INITIALIZE ICEBERG CATALOG IF NECESSARY
    # -------------------------------------------------------
    logger.info("Creating Iceberg table if it does not exist.")

    sql_spark = (
        SparkSession.builder.config(
            "spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", S3_PATH)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue_catalog.job-language", "python")
        .getOrCreate()
    )

    create_iceberg_table(sql_spark, GLUE_TABLE_PATH, S3_PATH, logger)

    logger.info("Iceberg table ensured.")

    # -------------------------------------------------------
    #             CONFIGURE TABLE OPTIMIZERS
    # -------------------------------------------------------
    sts = boto3.client("sts")
    iam = boto3.client("iam")
    glue = boto3.client("glue")

    identity = sts.get_caller_identity()

    role_name = identity["Arn"].split("/")[-2]
    role = iam.get_role(RoleName=role_name)
    role_arn = role["Role"]["Arn"]

    def get_catalog_id(glue_client, catalog_name):
        response = glue_client.get_databases()

        for db in response["DatabaseList"]:
            if db["Name"] == catalog_name:
                return db["CatalogId"]

    def set_optimizers_status(glue_client, db, table, optimizer_types, enabled=True):
        for opt_type in optimizer_types:
            try:
                optimizer = glue_client.get_table_optimizer(
                    CatalogId=get_catalog_id(glue_client, db),
                    DatabaseName=db,
                    TableName=table,
                    Type=opt_type,
                )

                if optimizer["TableOptimizerConfiguration"]["enabled"] != enabled:
                    logger.info(f"Setting {opt_type} optimizer 'enabled' to {enabled}")

                    new_config = optimizer["TableOptimizerConfiguration"].copy()
                    new_config["enabled"] = enabled

                    glue_client.update_table_optimizer(
                        CatalogId=get_catalog_id(glue_client, db),
                        DatabaseName=db,
                        TableName=table,
                        Type=opt_type,
                        TableOptimizerConfiguration=new_config,
                    )
            except glue_client.exceptions.EntityNotFoundException:
                logger.warn(f"Optimizer {opt_type} not found. Skipping status update.")
            except Exception as e:
                logger.error(f"Failed to update {opt_type}: {str(e)}")

    optimizer_configurations = {
        "compaction": {},
        "retention": {
            "retentionConfiguration": {
                "icebergConfiguration": {
                    "snapshotRetentionPeriodInDays": 3,
                    "numberOfSnapshotsToRetain": 1,
                    "cleanExpiredFiles": True,
                }
            }
        },
        "orphan_file_deletion": {},
    }

    for optimizer_type in optimizer_configurations.keys():
        try:
            logger.warn(f"Trying to create optimizer for {optimizer_type}")

            glue.create_table_optimizer(
                CatalogId=get_catalog_id(glue, DATABASE_NAME),
                DatabaseName=DATABASE_NAME,
                TableName=CATALOG_TABLE_NAME,
                Type=optimizer_type,
                TableOptimizerConfiguration={
                    "roleArn": role_arn,
                    "enabled": True,
                    **optimizer_configurations[optimizer_type],
                },
            )
            logger.warn(f"Optimizer {optimizer_type} configured")
        except glue.exceptions.AlreadyExistsException:
            logger.warn(f"Table optimizer of type {optimizer_type} already present")

    logger.warn("Pausing optimizers to prevent race conditions.")
    set_optimizers_status(
        glue,
        DATABASE_NAME,
        CATALOG_TABLE_NAME,
        optimizer_configurations.keys(),
        enabled=False,
    )

    # -------------------------------------------------------
    #       EXTRACT DATA FROM POSTGRESQL SOURCE
    # -------------------------------------------------------
    logger.info("Reading source table from PostgreSQL.")

    PostgreSQL_src = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": DWH_TABLE_NAME,
            "connectionName": CONNECTION_NAME,
        },
        transformation_ctx="PostgreSQL_src",
    )

    postgres_df = PostgreSQL_src.toDF()
    postgres_df.createOrReplaceTempView(DWH_TABLE_NAME)

    logger.info(f"Loaded PostgreSQL table: {DWH_TABLE_NAME}")

    # -------------------------------------------------------
    #  DYNAMICALLY UPDATE GLUE TABLE COLUMN DEFINITIONS
    # -------------------------------------------------------

    postgres_columns_with_types = get_postgres_columns_and_types(spark, DWH_TABLE_NAME)
    columns = postgres_columns_with_types.keys()

    dynamically_update_catalog_table(
        sql_spark, postgres_columns_with_types, GLUE_TABLE_PATH, logger
    )

    # -------------------------------------------------------
    #      PERFORM ICEBERG MERGE INTO TABLE
    # -------------------------------------------------------
    try:
        iceberg_merge(
            sql_spark,
            columns,
            CATALOG_TABLE_NAME,
            GLUE_TABLE_PATH,
            DWH_TABLE_NAME,
            logger,
        )
    except Exception as e:
        logger.error(f"Job failed during Merge: {str(e)}")
        raise e
    finally:
        if ENABLE_OPTIMIZERS_AFTER_JOB:
            logger.info(
                "Resuming optimizers for background maintenance (last job in workflow)."
            )
            set_optimizers_status(
                glue,
                DATABASE_NAME,
                CATALOG_TABLE_NAME,
                optimizer_configurations.keys(),
                enabled=True,
            )
        else:
            logger.info(
                "Skipping optimizer re-enable — not the last job in the workflow. Optimizers will be re-enabled by the final year job to prevent compaction races with subsequent MERGE operations."
            )

        job.commit()
        logger.info("Glue job completed successfully.")
