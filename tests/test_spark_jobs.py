"""Unit tests for Spark Batch and Streaming Job transformations."""

import os
import sys
import ast
import pytest

SPARK_DIR = os.path.join(os.path.dirname(__file__), "..", "spark")
sys.path.insert(0, SPARK_DIR)


def test_spark_batch_job_exports():
    """Verify spark_batch_job has required transformation and validation functions."""
    batch_job_path = os.path.join(SPARK_DIR, "spark_batch_job.py")
    assert os.path.exists(batch_job_path)

    with open(batch_job_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    functions = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    assert "validate_distributed_data" in functions
    assert "transform_orders" in functions
    assert "main" in functions


def test_spark_streaming_job_exports():
    """Verify spark_streaming_job has required parsing, anomaly filter, and sink functions."""
    stream_job_path = os.path.join(SPARK_DIR, "spark_streaming_job.py")
    assert os.path.exists(stream_job_path)

    with open(stream_job_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    functions = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    assert "parse_and_filter_stream" in functions
    assert "save_to_postgres" in functions
    assert "main" in functions


def test_spark_batch_transform_logic_with_pyspark():
    """Verify Spark batch transformation with live SparkSession if pyspark is available."""
    try:
        from pyspark.sql import SparkSession
        from spark_batch_job import transform_orders
    except ImportError:
        pytest.skip("PySpark not installed in test runner environment (runs in container)")

    spark = SparkSession.builder.master("local[1]").appName("UnitTestBatch").getOrCreate()
    try:
        data = [
            (1, 101, 100.0),
            (2, 101, 200.0),
            (3, 102, 50.0),
            (1, 101, 100.0),  # Duplicate
        ]
        columns = ["order_id", "customer_id", "amount"]
        df = spark.createDataFrame(data, columns)

        df_transformed, customer_summary = transform_orders(df)

        assert df_transformed.count() == 3  # Deduplicated
        assert customer_summary.count() == 2  # 2 unique customers
    finally:
        spark.stop()

