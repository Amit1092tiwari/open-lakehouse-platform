"""
Unit tests for the 100% Open-Source Technology Stack.

Tests:
1. Open-Source Warehouse Connector (PostgreSQL OLAP / DuckDB Columnar).
2. Snowflake fallback routing to open-source warehouse when unconfigured.
3. Apache Superset open-source semantic layer and dataset metadata generation.
4. Grafana open-source dashboard model generation.
5. MinIO & InfluxDB open-source streaming storage sink.
"""

import json
import os
import sys
import tempfile
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from warehouse import warehouse_connector
from bi_dashboards import bi_dashboard
from storage import minio_influxdb


def test_warehouse_connector_check_connection():
    """Verify open-source warehouse check_connection returns a valid boolean."""
    # Embedded/SQLite/Postgres connectivity check
    result = warehouse_connector.check_connection(engine_type="duckdb")
    assert isinstance(result, bool)
    assert result is True


def test_warehouse_connector_query_to_dataframe():
    """Verify executing SQL and returning DataFrame via open-source warehouse."""
    sql = "SELECT 42 AS answer, 'open_source' AS category"
    df = warehouse_connector.query_to_dataframe(sql, engine_type="duckdb")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["answer"] == 42
    assert df.iloc[0]["category"] == "open_source"


def test_warehouse_connector_load_dataframe():
    """Verify loading a DataFrame into warehouse table."""
    test_df = pd.DataFrame({
        "customer_id": [101, 102],
        "customer_name": ["Alice", "Bob"],
        "segment": ["Enterprise", "SMB"],
    })
    rows = warehouse_connector.load_dataframe(test_df, "test_dim_customers", engine_type="duckdb", overwrite=True)
    assert rows == 2

    # Query back
    result_df = warehouse_connector.query_to_dataframe("SELECT * FROM test_dim_customers", engine_type="duckdb")
    assert len(result_df) == 2
    assert "customer_id" in result_df.columns


def test_snowflake_fallback_to_opensource_warehouse():
    """Verify snowflake_connector gracefully falls back to open-source warehouse."""
    from snowflake import snowflake_connector

    # Ensure snowflake credentials are empty to test open-source fallback
    os.environ["SNOWFLAKE_ACCOUNT"] = ""
    os.environ["SNOWFLAKE_USER"] = ""

    assert snowflake_connector.is_snowflake_configured() is False

    # check_connection should succeed by routing to open-source warehouse
    connected = snowflake_connector.check_connection()
    assert connected is True

    # query_to_dataframe should query open-source warehouse
    df = snowflake_connector.query_to_dataframe("SELECT 100 AS metric")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["metric"] == 100


def test_export_to_apache_superset():
    """Verify Apache Superset open-source BI metadata generation."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        json_path = bi_dashboard.export_to_apache_superset(output_dir=tmp_dir)
        assert os.path.exists(json_path)

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["version"] == "1.0.0"
        assert "databases" in data
        assert len(data["databases"]) > 0
        db = data["databases"][0]
        assert "OpenSource_PostgreSQL_Warehouse" in db["database_name"]
        assert len(db["tables"]) >= 3
        assert any(t["table_name"] == "agg_daily_orders" for t in db["tables"])


def test_export_to_grafana():
    """Verify Grafana open-source dashboard model generation."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        json_path = bi_dashboard.export_to_grafana(output_dir=tmp_dir)
        assert os.path.exists(json_path)

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert "dashboard" in data
        dashboard = data["dashboard"]
        assert "Business Intelligence" in dashboard["title"]
        assert "panels" in dashboard
        assert len(dashboard["panels"]) >= 4

        # Check for key analytical panels
        titles = [p["title"] for p in dashboard["panels"]]
        assert any("Revenue" in t for t in titles)
        assert any("Orders" in t for t in titles)


def test_minio_influxdb_build_point():
    """Verify MinIO/InfluxDB open-source IoT sink constructs point correctly."""
    valid_data = {"device_id": 999, "reading_value": 37.5, "timestamp": 1710000000}
    point = minio_influxdb.build_influx_point(valid_data)
    assert point is not None

    invalid_data = {"device_id": None}
    assert minio_influxdb.build_influx_point(invalid_data) is None

