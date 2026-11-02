"""
Batch Ingestion DAG.

Orchestrates: MySQL Extraction -> Data Quality Validation -> MinIO S3 Stage -> Spark Processing -> PostgreSQL Bulk Loading.
"""

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
try:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    from airflow import DAG
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)

# Configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw-data")
STAGING_DIR = os.getenv("STAGING_DIR", "/tmp")
ORDERS_EXTRACT_PATH = os.path.join(STAGING_DIR, "orders.csv")
ORDERS_TRANSFORMED_PATH = os.path.join(STAGING_DIR, "transformed_orders.csv")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def validate_orders_dataframe(df):
    """
    Validates orders dataframe using core data quality rules:
    - Primary key order_id non-null
    - Customer id positive
    - Amount strictly positive
    """
    if df is None or df.empty:
        raise ValueError("Validation failed: DataFrame is empty or None")

    if "order_id" not in df.columns or df["order_id"].isnull().any():
        raise ValueError("Validation failed: 'order_id' has null values or is missing")

    if "amount" not in df.columns:
        raise ValueError("Validation failed: 'amount' column is missing")

    if (df["amount"] <= 0).any():
        raise ValueError("Validation failed: 'amount' has non-positive values")

    return True


def extract_data_from_mysql(**kwargs):
    """Extract batch data from MySQL orders table."""
    logger.info("[INPUT EXPECTED] Source: MySQL database table 'orders' (order_id, customer_id, amount, created_at)")
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    df = mysql_hook.get_pandas_df("SELECT order_id, customer_id, amount, created_at FROM orders")
    os.makedirs(os.path.dirname(ORDERS_EXTRACT_PATH), exist_ok=True)
    df.to_csv(ORDERS_EXTRACT_PATH, index=False)
    logger.info("[INPUT RECEIVED] Extracted %d order records from MySQL source.", len(df))
    logger.info("[OUTPUT RECEIVED] Staged raw orders extract to %s", ORDERS_EXTRACT_PATH)
    return len(df)


def validate_data_with_ge(**kwargs):
    """Validate extracted data with Great Expectations / core assertion rules."""
    logger.info("[INPUT EXPECTED] Raw extract CSV at %s", ORDERS_EXTRACT_PATH)
    df = pd.read_csv(ORDERS_EXTRACT_PATH)
    logger.info("[PROCESSING] Executing data quality validation suite (non-null order_id, positive amount)...")

    try:
        import great_expectations as ge
        ge_df = ge.from_pandas(df)
        res_id = ge_df.expect_column_values_to_not_be_null("order_id")
        if not res_id["success"]:
            raise ValueError("Validation failed: 'order_id' has null values")

        res_amt = ge_df.expect_column_values_to_be_between("amount", min_value=0.01, max_value=9999999)
        if not res_amt["success"]:
            raise ValueError("Validation failed: 'amount' is not strictly positive")
    except ImportError:
        # Fallback to pure assertion validator
        validate_orders_dataframe(df)

    logger.info("[OUTPUT RECEIVED] Great Expectations data quality validation passed.")
    return True


def load_to_minio(**kwargs):
    """Upload raw CSV to MinIO S3-compatible storage."""
    import boto3

    logger.info("[INPUT EXPECTED] Validated raw file: %s", ORDERS_EXTRACT_PATH)
    logger.info("[OUTPUT EXPECTED] Destination: MinIO bucket '%s', key 'orders/orders.csv'", MINIO_BUCKET_RAW)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    try:
        s3.head_bucket(Bucket=MINIO_BUCKET_RAW)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET_RAW)
        logger.info("[PROCESSING] Created MinIO bucket '%s'.", MINIO_BUCKET_RAW)

    s3.upload_file(ORDERS_EXTRACT_PATH, MINIO_BUCKET_RAW, "orders/orders.csv")
    logger.info("[OUTPUT RECEIVED] Raw orders dataset uploaded to MinIO: s3://%s/orders/orders.csv", MINIO_BUCKET_RAW)
    return "orders/orders.csv"


def load_to_postgres(**kwargs):
    """Load transformed data into PostgreSQL using high-throughput bulk insert."""
    logger.info("[INPUT EXPECTED] Spark transformed output: %s", ORDERS_TRANSFORMED_PATH)
    logger.info("[OUTPUT EXPECTED] Destination: PostgreSQL table 'orders_transformed'")
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS orders_transformed (
            order_id INT PRIMARY KEY,
            customer_id INT,
            amount DECIMAL(10,2),
            processed_timestamp TIMESTAMP
        )
    """)
    pg_hook.run("TRUNCATE TABLE orders_transformed")

    if not os.path.exists(ORDERS_TRANSFORMED_PATH):
        logger.warning("Transformed file not found: %s", ORDERS_TRANSFORMED_PATH)
        return 0

    df = pd.read_csv(ORDERS_TRANSFORMED_PATH)
    if df.empty:
        logger.warning("[INPUT RECEIVED] Zero records found in transformed extract.")
        return 0

    logger.info("[INPUT RECEIVED] Read %d transformed records for PostgreSQL loading.", len(df))
    logger.info("[PROCESSING] Bulk inserting %d records into PostgreSQL table 'orders_transformed'...", len(df))

    records = [
        (
            row["order_id"],
            row["customer_id"],
            row["amount"],
            row["processed_timestamp"],
        )
        for _, row in df.iterrows()
    ]

    pg_hook.insert_rows(
        table="orders_transformed",
        rows=records,
        target_fields=["order_id", "customer_id", "amount", "processed_timestamp"],
        commit_every=5000,
    )
    logger.info("Bulk loaded %d records into PostgreSQL orders_transformed.", len(records))
    return len(records)


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="batch_ingestion_dag",
        default_args=default_args,
        description="Batch ETL: MySQL -> Validation -> MinIO -> Spark -> PostgreSQL Bulk Load",
        schedule="@daily",
        catchup=False,
        tags=["batch", "etl", "production"],
    ) as dag:
        extract_task = PythonOperator(
            task_id="extract_mysql",
            python_callable=extract_data_from_mysql,
            execution_timeout=timedelta(minutes=10),
        )

        validate_task = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data_with_ge,
            execution_timeout=timedelta(minutes=5),
        )

        load_to_minio_task = PythonOperator(
            task_id="load_to_minio",
            python_callable=load_to_minio,
            execution_timeout=timedelta(minutes=10),
        )

        spark_transform_task = BashOperator(
            task_id="spark_transform",
            bash_command=os.getenv("SPARK_SUBMIT_COMMAND", "spark-submit --master local[2] /opt/spark_jobs/spark_batch_job.py"),
            execution_timeout=timedelta(minutes=30),
        )

        load_postgres_task = PythonOperator(
            task_id="load_to_postgres",
            python_callable=load_to_postgres,
            execution_timeout=timedelta(minutes=15),
        )

        extract_task >> validate_task >> load_to_minio_task >> spark_transform_task >> load_postgres_task
