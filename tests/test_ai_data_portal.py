"""Unit tests for AI Data Portal Analytics & NL-to-SQL Engine."""

import os
import sys
import pytest

PORTAL_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "ai_data_portal")
sys.path.insert(0, PORTAL_DIR)

from app import PortalService, init_vector_rag


@pytest.fixture
def portal_service():
    chunker, embedder, vector_index = init_vector_rag()
    return PortalService(vector_index=vector_index, embedder=embedder)


def test_portal_kpi_metrics(portal_service):
    """Verify KPI metrics calculation from gold marts."""
    metrics = portal_service.get_kpi_metrics()
    assert "total_revenue" in metrics
    assert "total_orders" in metrics
    assert "active_customers" in metrics
    assert metrics["total_revenue"] > 0
    assert metrics["total_orders"] > 0
    assert metrics["active_customers"] > 0


def test_portal_chart_data(portal_service):
    """Verify daily revenue aggregations and loyalty tier segments."""
    chart_data = portal_service.get_chart_data()
    assert "daily_revenue" in chart_data
    assert "loyalty_tiers" in chart_data
    assert "recent_anomalies" in chart_data

    # Check daily revenue format
    assert len(chart_data["daily_revenue"]) > 0
    assert "date" in chart_data["daily_revenue"][0]
    assert "revenue" in chart_data["daily_revenue"][0]

    # Check loyalty tiers
    assert "TIER_PLATINUM" in chart_data["loyalty_tiers"]


def test_portal_pipeline_health(portal_service):
    """Verify pipeline health monitoring endpoints."""
    health = portal_service.get_pipeline_health()
    assert health["status"] == "HEALTHY"
    assert len(health["components"]) >= 4


def test_portal_sql_chat_top_revenue(portal_service):
    """Verify NL-to-SQL logic for 'top revenue' query."""
    res = portal_service.execute_sql_chat("Show me the top revenue customers")
    assert "SELECT" in res["sql"]
    assert "dim_customers" in res["sql"]
    assert len(res["rows"]) <= 5
    assert res["rows"][0]["customer_name"] == "Apex Capital"


def test_portal_sql_chat_tiers(portal_service):
    """Verify NL-to-SQL logic for 'loyalty tier' grouping."""
    res = portal_service.execute_sql_chat("Summarize customer count by loyalty tier")
    assert "GROUP BY loyalty_tier" in res["sql"]
    assert len(res["rows"]) > 0


def test_portal_sql_chat_anomalies(portal_service):
    """Verify NL-to-SQL logic for streaming anomalies."""
    res = portal_service.execute_sql_chat("Show recent sensor anomalies")
    assert "fact_sensor_readings" in res["sql"]
    assert len(res["rows"]) > 0


def test_portal_rag_chat(portal_service):
    """Verify Vector RAG synthesis and citations."""
    res = portal_service.execute_rag_chat("How to optimize Snowflake and Spark cost?")
    assert "answer" in res
    assert "citations" in res
    assert len(res["citations"]) > 0
    assert "search_latency_ms" in res


def test_portal_lakehouse_sql_execution(portal_service):
    """Verify portal service executes queries via open-source warehouse with fallback."""
    rows = portal_service._execute_lakehouse_sql("SELECT 1 AS num", fallback_rows=[{"num": 1}])
    assert len(rows) > 0
    assert rows[0]["num"] == 1

