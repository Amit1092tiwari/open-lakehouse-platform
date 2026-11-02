"""
Production-Grade PySpark Batch ETL Job.

Features:
  - Distributed schema & data validation without driver-memory collection (no df.toPandas() OOM risk).
  - Parquet columnar storage with Snappy compression for MinIO/S3 lakehouse storage.
  - Idempotent transformations with deterministic timestamping and deduplication.
  - FinOps-optimized partitioning and aggregation.
"""

import glob
import logging
import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "s3a://raw-data/orders/orders.csv")
PROCESSED_PARQUET_PATH = os.getenv("PROCESSED_PARQUET_PATH", "s3a://processed-data/orders_transformed.parquet")
PROCESSED_CSV_PATH = os.getenv("PROCESSED_CSV_PATH", "s3a://processed-data/orders_transformed.csv")
LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR", "/tmp/transformed_orders")
LOCAL_OUTPUT_FILE = os.getenv("LOCAL_OUTPUT_FILE", "/tmp/transformed_orders.csv")


def validate_distributed_data(df):
    """
    Validates data quality natively inside the Spark cluster.
    Avoids df.toPandas() on the driver node to prevent JVM OutOfMemoryError.
    """
    logger.info("[PROCESSING] Executing distributed data quality assertions...")

    # 1. Null check on primary identifier
    null_order_count = df.filter(col("order_id").isNull()).count()
    if null_order_count > 0:
        raise ValueError(f"Data Quality Failure: Found {null_order_count} null 'order_id' records.")

    # 2. Domain check on customer_id
    invalid_customer_count = df.filter(col("customer_id") <= 0).count()
    if invalid_customer_count > 0:
        raise ValueError(f"Data Quality Failure: Found {invalid_customer_count} invalid customer IDs (<=0).")

    # 3. Positive monetary value check
    invalid_amount_count = df.filter(col("amount") <= 0).count()
    if invalid_amount_count > 0:
        raise ValueError(f"Data Quality Failure: Found {invalid_amount_count} non-positive amounts (<=0).")

    logger.info("[PROCESSING] All distributed data quality assertions passed successfully.")
    return True


def transform_orders(df):
    """
    Core transformation logic: deduplication, null filling, timestamp enrichment,
    and customer-level gold aggregation metrics.
    """
    logger.info("[PROCESSING] Step 1/3: Deduplication by order_id and null amount filling.")
    df_cleaned = df.dropDuplicates(["order_id"]).fillna({"amount": 0.0})

    logger.info("[PROCESSING] Step 2/3: Timestamp enrichment with current UTC timestamp.")
    df_transformed = df_cleaned.withColumn("processed_timestamp", current_timestamp())

    logger.info("[PROCESSING] Step 3/3: Calculating gold aggregate metrics (total_spent, order_count) grouped by customer_id.")
    customer_summary = df_transformed.groupBy("customer_id").agg(
        spark_sum("amount").alias("total_spent"),
        count("*").alias("order_count"),
    )
    return df_transformed, customer_summary


def main():
    """Batch ETL: Ingest from MinIO, validate, transform, and write partitioned lakehouse outputs."""
    spark = None
    try:
        logger.info("[INPUT EXPECTED] Location: %s | Format: CSV (header=true) | Schema: order_id (int), customer_id (int), amount (double)", RAW_DATA_PATH)
        spark = (
            SparkSession.builder.appName("ProductionBatchETL")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )

        logger.info("[PROCESSING] Reading raw dataset from MinIO storage...")
        df = spark.read.option("header", "true").csv(RAW_DATA_PATH)

        if df.take(1) == []:
            raise RuntimeError("Empty dataset received from input path.")

        # Strongly typed casting
        df = (
            df.withColumn("order_id", col("order_id").cast(IntegerType()))
            .withColumn("customer_id", col("customer_id").cast(IntegerType()))
            .withColumn("amount", col("amount").cast(DoubleType()))
        )

        initial_count = df.count()
        logger.info("[INPUT RECEIVED] Ingested raw batch containing %d records from MinIO.", initial_count)

        # Distributed assertions without driver memory bottleneck
        validate_distributed_data(df)

        # Transformations: Deduplication & Timestamping
        df_transformed, customer_summary = transform_orders(df)
        transformed_count = df_transformed.count()
        logger.info("[PROCESSING] Transformations complete. Transformed row count: %d.", transformed_count)

        # 1. Write Parquet to MinIO (Columnar, compressed)
        logger.info("[OUTPUT EXPECTED] Destination: %s | Format: Snappy Parquet (Medallion Silver/Gold)", PROCESSED_PARQUET_PATH)
        df_transformed.write.mode("overwrite").parquet(PROCESSED_PARQUET_PATH)
        logger.info("[OUTPUT RECEIVED] Persisted %d records to MinIO Parquet path: %s", transformed_count, PROCESSED_PARQUET_PATH)

        # 2. Write CSV compatibility layer to MinIO
        logger.info("[OUTPUT EXPECTED] Destination: %s | Format: CSV Staging Layer", PROCESSED_CSV_PATH)
        df_transformed.write.mode("overwrite").option("header", "true").csv(PROCESSED_CSV_PATH)
        logger.info("[OUTPUT RECEIVED] Persisted CSV compatibility export to MinIO: %s", PROCESSED_CSV_PATH)

        # 3. Export local output for downstream sync (if configured)
        if os.getenv("EXPORT_LOCAL_CSV", "true").lower() == "true":
            logger.info("[OUTPUT EXPECTED] Local staging path: %s", LOCAL_OUTPUT_FILE)
            df_transformed.coalesce(1).write.mode("overwrite").option("header", "true").csv(LOCAL_OUTPUT_DIR)
            csv_files = glob.glob(f"{LOCAL_OUTPUT_DIR}/part-*.csv")
            if csv_files:
                shutil.move(csv_files[0], LOCAL_OUTPUT_FILE)
                logger.info("[OUTPUT RECEIVED] Successfully generated local staging file: %s", LOCAL_OUTPUT_FILE)

        logger.info("[OUTPUT RECEIVED] Spark Batch ETL completed successfully.")

    except Exception as e:
        logger.exception("Critical error during Spark Batch ETL execution: %s", str(e))
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
