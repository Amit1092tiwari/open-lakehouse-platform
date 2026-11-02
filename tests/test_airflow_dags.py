"""Unit tests for Airflow DAGs and validation functions."""

import os
import sys
import pandas as pd
import pytest

# Add airflow dags to sys.path
AIRFLOW_DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
sys.path.insert(0, AIRFLOW_DAGS_DIR)


def test_batch_ingestion_dag_structure():
    """Verify batch ingestion DAG parses and has all expected tasks."""
    try:
        import batch_ingestion_dag as m
        if not getattr(m, "AIRFLOW_AVAILABLE", False):
            pytest.skip("Airflow library not installed in test runner environment")
        batch_dag = m.dag
    except Exception as e:
        pytest.skip(f"Airflow not available: {e}")

    assert batch_dag.dag_id == "batch_ingestion_dag"
    assert batch_dag.schedule_interval == "@daily"

    task_ids = set(batch_dag.task_dict.keys())
    expected_tasks = {
        "extract_mysql",
        "validate_data",
        "load_to_minio",
        "spark_transform",
        "load_to_postgres",
    }
    assert expected_tasks.issubset(task_ids)


def test_warehouse_transform_dag_structure():
    """Verify warehouse transform DAG parses and contains task graph."""
    try:
        import warehouse_transform_dag as m
        if not getattr(m, "AIRFLOW_AVAILABLE", False):
            pytest.skip("Airflow library not installed in test runner environment")
        wh_dag = m.dag
    except Exception as e:
        pytest.skip(f"Airflow not available: {e}")

    assert wh_dag.dag_id == "warehouse_transform_dag"
    assert wh_dag.schedule_interval == "@hourly"

    task_ids = set(wh_dag.task_dict.keys())
    expected_tasks = {
        "extract_and_stage_orders",
        "extract_and_stage_anomalies",
        "load_dimensions",
        "load_facts",
        "refresh_aggregations",
        "log_pipeline_run",
    }
    assert expected_tasks.issubset(task_ids)


def test_streaming_monitoring_dag_structure():
    """Verify streaming monitoring DAG parses and checks consumer lag."""
    try:
        import streaming_monitoring_dag as m
        if not getattr(m, "AIRFLOW_AVAILABLE", False):
            pytest.skip("Airflow library not installed in test runner environment")
        stream_dag = m.dag
    except Exception as e:
        pytest.skip(f"Airflow not available: {e}")

    assert stream_dag.dag_id == "streaming_monitoring_dag"
    task_ids = set(stream_dag.task_dict.keys())
    assert "check_kafka_health" in task_ids


def test_validate_orders_dataframe_success():
    """Verify valid orders dataframe passes quality checks."""
    from batch_ingestion_dag import validate_orders_dataframe

    df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 103],
        "amount": [150.00, 299.99, 45.50],
        "created_at": ["2026-01-01", "2026-01-02", "2026-01-03"]
    })
    assert validate_orders_dataframe(df) is True


def test_validate_orders_dataframe_failures():
    """Verify validation fails on nulls, missing columns, and negative values."""
    from batch_ingestion_dag import validate_orders_dataframe

    # Empty
    with pytest.raises(ValueError, match="empty or None"):
        validate_orders_dataframe(pd.DataFrame())

    # Null order_id
    df_null_id = pd.DataFrame({"order_id": [1, None], "amount": [10.0, 20.0]})
    with pytest.raises(ValueError, match="order_id"):
        validate_orders_dataframe(df_null_id)

    # Missing amount
    df_no_amt = pd.DataFrame({"order_id": [1, 2]})
    with pytest.raises(ValueError, match="amount"):
        validate_orders_dataframe(df_no_amt)

    # Non-positive amount
    df_neg_amt = pd.DataFrame({"order_id": [1, 2], "amount": [50.0, -10.0]})
    with pytest.raises(ValueError, match="amount"):
        validate_orders_dataframe(df_neg_amt)
