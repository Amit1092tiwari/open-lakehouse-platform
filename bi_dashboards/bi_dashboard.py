"""
BI Dashboard Integration - Export warehouse data to BI platforms.

Queries the open-source PostgreSQL/DuckDB data warehouse (or Snowflake if configured)
and exports datasets and visualization specifications to:
1. Apache Superset (Premier Open-Source BI & Semantic Layer - Apache 2.0)
2. Grafana (Open-Source Metrics, Analytics, and Real-time Dashboards - AGPLv3)
3. Commercial BI fallbacks (Tableau, Looker, Power BI)
"""

import json
import logging
import os
import sys

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Snowflake Configuration (Optional Commercial Cloud Warehouse)
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_ENABLED = bool(SNOWFLAKE_ACCOUNT)

# Open-Source PostgreSQL Warehouse (Default)
DB_HOST = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "postgres"))
DB_PORT = os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "processed_db")
DB_USER = os.getenv("POSTGRES_USER", "pipeline_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_secret_2024")

# Open-Source BI Tool configs
SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY", "")

# Commercial BI Tool configs (Legacy / Optional)
TABLEAU_SERVER = os.getenv("TABLEAU_SERVER", "")
LOOKER_API_URL = os.getenv("LOOKER_API_URL", "")
POWER_BI_WORKSPACE_ID = os.getenv("POWER_BI_WORKSPACE_ID", "")

OUTPUT_DIR = os.getenv("BI_OUTPUT_DIR", "/tmp/bi_exports")

# Snowflake-compatible warehouse queries
SNOWFLAKE_QUERIES = {
    "daily_orders_summary": """
        SELECT
            dd.FULL_DATE,
            dd.DAY_NAME,
            dd.MONTH_NAME,
            dd.QUARTER,
            dd.YEAR,
            COALESCE(a.TOTAL_ORDERS, 0) AS TOTAL_ORDERS,
            COALESCE(a.TOTAL_REVENUE, 0) AS TOTAL_REVENUE,
            COALESCE(a.AVG_ORDER_VALUE, 0) AS AVG_ORDER_VALUE,
            COALESCE(a.UNIQUE_CUSTOMERS, 0) AS UNIQUE_CUSTOMERS
        FROM ANALYTICS.DIM_DATE dd
        LEFT JOIN ANALYTICS.AGG_DAILY_ORDERS a ON a.DATE_KEY = dd.DATE_KEY
        WHERE dd.YEAR >= 2024
        ORDER BY dd.FULL_DATE
    """,
    "customer_segments": """
        SELECT
            dc.CUSTOMER_ID,
            dc.CUSTOMER_NAME,
            dc.CUSTOMER_SEGMENT,
            COUNT(fo.ORDER_KEY) AS TOTAL_ORDERS,
            SUM(fo.NET_AMOUNT) AS TOTAL_SPENT,
            AVG(fo.NET_AMOUNT) AS AVG_ORDER_VALUE,
            MAX(dd.FULL_DATE) AS LAST_ORDER_DATE
        FROM ANALYTICS.DIM_CUSTOMERS dc
        LEFT JOIN ANALYTICS.FACT_ORDERS fo ON fo.CUSTOMER_KEY = dc.CUSTOMER_KEY
        LEFT JOIN ANALYTICS.DIM_DATE dd ON dd.DATE_KEY = fo.DATE_KEY
        WHERE dc.IS_CURRENT = TRUE
        GROUP BY dc.CUSTOMER_ID, dc.CUSTOMER_NAME, dc.CUSTOMER_SEGMENT
        ORDER BY TOTAL_SPENT DESC
    """,
    "sensor_anomaly_report": """
        SELECT
            dd.DEVICE_ID,
            dd.DEVICE_TYPE,
            dd.LOCATION,
            COUNT(fsr.READING_KEY) AS TOTAL_ANOMALIES,
            AVG(fsr.READING_VALUE) AS AVG_ANOMALY_VALUE,
            MAX(fsr.READING_VALUE) AS MAX_ANOMALY_VALUE,
            MAX(fsr.READING_TIMESTAMP) AS LAST_ANOMALY
        FROM ANALYTICS.DIM_DEVICES dd
        JOIN ANALYTICS.FACT_SENSOR_READINGS fsr ON fsr.DEVICE_KEY = dd.DEVICE_KEY
        WHERE fsr.IS_ANOMALY = TRUE
        GROUP BY dd.DEVICE_ID, dd.DEVICE_TYPE, dd.LOCATION
        ORDER BY TOTAL_ANOMALIES DESC
    """,
    "pipeline_health": """
        SELECT
            DAG_ID,
            RUN_TYPE,
            COUNT(*) AS TOTAL_RUNS,
            SUM(CASE WHEN STATUS = 'success' THEN 1 ELSE 0 END) AS SUCCESSFUL_RUNS,
            SUM(CASE WHEN STATUS = 'failed' THEN 1 ELSE 0 END) AS FAILED_RUNS,
            AVG(DURATION_SECONDS) AS AVG_DURATION_SECONDS,
            SUM(RECORDS_PROCESSED) AS TOTAL_RECORDS_PROCESSED
        FROM ANALYTICS.FACT_PIPELINE_RUNS
        WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
        GROUP BY DAG_ID, RUN_TYPE
        ORDER BY DAG_ID
    """,
}

# PostgreSQL open-source warehouse queries (lowercase)
PG_QUERIES = {
    "daily_orders_summary": """
        SELECT dd.full_date, dd.day_name, dd.month_name, dd.quarter, dd.year,
               COALESCE(a.total_orders, 0) AS total_orders,
               COALESCE(a.total_revenue, 0) AS total_revenue,
               COALESCE(a.avg_order_value, 0) AS avg_order_value,
               COALESCE(a.unique_customers, 0) AS unique_customers
        FROM dim_date dd
        LEFT JOIN agg_daily_orders a ON a.date_key = dd.date_key
        WHERE dd.year >= 2024 ORDER BY dd.full_date
    """,
    "customer_segments": """
        SELECT dc.customer_id, dc.customer_name, dc.customer_segment,
               COUNT(fo.order_key) AS total_orders, SUM(fo.net_amount) AS total_spent,
               AVG(fo.net_amount) AS avg_order_value, MAX(dd.full_date) AS last_order_date
        FROM dim_customers dc
        LEFT JOIN fact_orders fo ON fo.customer_key = dc.customer_key
        LEFT JOIN dim_date dd ON dd.date_key = fo.date_key
        WHERE dc.is_current = TRUE
        GROUP BY dc.customer_id, dc.customer_name, dc.customer_segment
        ORDER BY total_spent DESC
    """,
    "sensor_anomaly_report": """
        SELECT dd.device_id, dd.device_type, dd.location,
               COUNT(fsr.reading_key) AS total_anomalies,
               AVG(fsr.reading_value) AS avg_anomaly_value,
               MAX(fsr.reading_value) AS max_anomaly_value,
               MAX(fsr.reading_timestamp) AS last_anomaly
        FROM dim_devices dd
        JOIN fact_sensor_readings fsr ON fsr.device_key = dd.device_key
        WHERE fsr.is_anomaly = TRUE
        GROUP BY dd.device_id, dd.device_type, dd.location
        ORDER BY total_anomalies DESC
    """,
    "pipeline_health": """
        SELECT dag_id, run_type, COUNT(*) AS total_runs,
               SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
               AVG(duration_seconds) AS avg_duration_seconds,
               SUM(records_processed) AS total_records_processed
        FROM fact_pipeline_runs
        WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '30 days'
        GROUP BY dag_id, run_type ORDER BY dag_id
    """,
}


def get_snowflake_engine():
    """Create SQLAlchemy engine for Snowflake."""
    from sqlalchemy import create_engine

    url = (
        f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}"
        f"@{SNOWFLAKE_ACCOUNT}/{os.getenv('SNOWFLAKE_DATABASE', 'PIPELINE_DB')}"
        f"/{os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')}"
        f"?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE', 'PIPELINE_WH')}"
        f"&role={os.getenv('SNOWFLAKE_ROLE', 'PIPELINE_ROLE')}"
    )
    return create_engine(url)


def get_postgres_engine():
    """Create SQLAlchemy engine for PostgreSQL open-source warehouse."""
    from sqlalchemy import create_engine

    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True, pool_size=5)


def export_warehouse_data():
    """Export warehouse queries to CSV. Uses Snowflake if configured, else PostgreSQL open-source warehouse."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("[INPUT EXPECTED] Source warehouse: %s | Queries: %s", "Snowflake" if SNOWFLAKE_ENABLED else "PostgreSQL Open-Source Warehouse", list(PG_QUERIES.keys()))

    if SNOWFLAKE_ENABLED:
        logger.info("[PROCESSING] Querying Snowflake data warehouse...")
        engine = get_snowflake_engine()
        queries = SNOWFLAKE_QUERIES
    else:
        logger.info("[PROCESSING] Using PostgreSQL open-source warehouse...")
        engine = get_postgres_engine()
        queries = PG_QUERIES

    for name, query in queries.items():
        try:
            logger.info("[PROCESSING] Executing warehouse analytical query for '%s'...", name)
            df = pd.read_sql(query, con=engine)
            logger.info("[INPUT RECEIVED] Query '%s' returned %d records.", name, len(df))
            filepath = os.path.join(OUTPUT_DIR, f"{name}.csv")
            logger.info("[OUTPUT EXPECTED] Destination CSV: %s", filepath)
            df.to_csv(filepath, index=False)
            logger.info("[OUTPUT RECEIVED] Exported %s: %d rows -> %s", name, len(df), filepath)
        except Exception as e:
            logger.error("Failed to export %s: %s", name, e)


def export_to_apache_superset(output_dir: str = OUTPUT_DIR) -> str:
    """
    Generate dataset definitions and dashboard specifications for Apache Superset (Open Source BI).
    
    Creates a declarative JSON export that can be imported directly into Superset via CLI or API.
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info("[INPUT EXPECTED] Open-source semantic layer metrics & dataset specifications.")
    logger.info("[PROCESSING] Generating declarative dataset definitions and chart slice specs for Apache Superset...")
    superset_config = {
        "version": "1.0.0",
        "databases": [
            {
                "database_name": "OpenSource_PostgreSQL_Warehouse",
                "sqlalchemy_uri": f"postgresql://{DB_USER}:XXXXXXXX@{DB_HOST}:{DB_PORT}/{DB_NAME}",
                "tables": [
                    {
                        "table_name": "agg_daily_orders",
                        "schema": "public",
                        "metrics": [
                            {"metric_name": "total_revenue", "expression": "SUM(total_revenue)"},
                            {"metric_name": "total_orders", "expression": "SUM(total_orders)"},
                            {"metric_name": "avg_order_value", "expression": "AVG(avg_order_value)"},
                        ],
                    },
                    {
                        "table_name": "fact_orders",
                        "schema": "public",
                        "metrics": [
                            {"metric_name": "net_amount", "expression": "SUM(net_amount)"},
                            {"metric_name": "order_count", "expression": "COUNT(order_key)"},
                        ],
                    },
                    {
                        "table_name": "fact_sensor_readings",
                        "schema": "public",
                        "metrics": [
                            {"metric_name": "anomalies", "expression": "SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)"},
                            {"metric_name": "avg_reading", "expression": "AVG(reading_value)"},
                        ],
                    },
                ],
            }
        ],
        "dashboards": [
            {
                "dashboard_title": "Enterprise Data Platform Executive BI",
                "slices": [
                    "Daily Revenue Trend",
                    "Customer Lifetime Value by Segment",
                    "IoT Sensor Anomaly Detection Distribution",
                    "Airflow Pipeline Execution SLA",
                ],
            }
        ],
    }

    filepath = os.path.join(output_dir, "apache_superset_datasources.json")
    logger.info("[OUTPUT EXPECTED] Destination file: %s", filepath)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(superset_config, f, indent=2)
    logger.info("[OUTPUT RECEIVED] Exported Apache Superset open-source BI metadata: %s", filepath)
    return filepath


def export_to_grafana(output_dir: str = OUTPUT_DIR) -> str:
    """
    Generate Grafana dashboard JSON models for operational and business analytics.
    
    Can be loaded directly into Grafana (running on port 3000 in docker-compose).
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info("[INPUT EXPECTED] Operational and BI SQL panel queries for PostgreSQL warehouse.")
    logger.info("[PROCESSING] Constructing Grafana dashboard model with revenue, order, anomaly, and SLA panels...")
    grafana_dashboard = {
        "dashboard": {
            "id": None,
            "title": "Business Intelligence & Platform Performance",
            "tags": ["datapipeline", "analytics", "open-source", "postgres"],
            "timezone": "browser",
            "panels": [
                {
                    "title": "Total Revenue (YTD)",
                    "type": "stat",
                    "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0},
                    "targets": [
                        {
                            "rawSql": "SELECT COALESCE(SUM(total_revenue), 0) FROM agg_daily_orders;",
                            "format": "table",
                        }
                    ],
                },
                {
                    "title": "Total Orders",
                    "type": "stat",
                    "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0},
                    "targets": [
                        {
                            "rawSql": "SELECT COALESCE(SUM(total_orders), 0) FROM agg_daily_orders;",
                            "format": "table",
                        }
                    ],
                },
                {
                    "title": "IoT Active Anomalies (Last 24h)",
                    "type": "stat",
                    "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
                    "targets": [
                        {
                            "rawSql": "SELECT COUNT(*) FROM fact_sensor_readings WHERE is_anomaly = TRUE AND reading_timestamp >= NOW() - INTERVAL '24 hours';",
                            "format": "table",
                        }
                    ],
                },
                {
                    "title": "Pipeline Success Rate",
                    "type": "gauge",
                    "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
                    "targets": [
                        {
                            "rawSql": "SELECT (COUNT(CASE WHEN status='success' THEN 1 END)::float / NULLIF(COUNT(*), 0)) * 100 FROM fact_pipeline_runs;",
                            "format": "table",
                        }
                    ],
                },
                {
                    "title": "Daily Order Revenue Trend",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4},
                    "targets": [
                        {
                            "rawSql": "SELECT dd.full_date AS time, a.total_revenue AS value FROM agg_daily_orders a JOIN dim_date dd ON dd.date_key = a.date_key ORDER BY dd.full_date;",
                            "format": "time_series",
                        }
                    ],
                },
                {
                    "title": "Customer Segment Revenue Distribution",
                    "type": "piechart",
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4},
                    "targets": [
                        {
                            "rawSql": "SELECT dc.customer_segment AS metric, SUM(fo.net_amount) AS value FROM dim_customers dc JOIN fact_orders fo ON fo.customer_key = dc.customer_key GROUP BY dc.customer_segment;",
                            "format": "table",
                        }
                    ],
                },
            ],
            "refresh": "1m",
            "schemaVersion": 38,
        },
        "overwrite": True,
    }

    filepath = os.path.join(output_dir, "grafana_bi_dashboard.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(grafana_dashboard, f, indent=2)
    logger.info("Exported Grafana open-source dashboard model: %s", filepath)

    # Optional auto-sync if Grafana API key is configured
    if GRAFANA_API_KEY:
        try:
            import requests
            headers = {"Authorization": f"Bearer {GRAFANA_API_KEY}", "Content-Type": "application/json"}
            resp = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=headers, json=grafana_dashboard, timeout=10)
            if resp.ok:
                logger.info("Successfully published dashboard to Grafana server.")
            else:
                logger.warning("Grafana API sync returned: %s", resp.text)
        except Exception as e:
            logger.warning("Could not sync to Grafana API: %s", e)

    return filepath


def upload_to_tableau():
    """Upload datasets to Tableau via REST API (Commercial BI)."""
    import requests

    if not TABLEAU_SERVER:
        logger.info("Tableau not configured, skipping.")
        return

    site_id = os.getenv("TABLEAU_SITE_ID", "")
    username = os.getenv("TABLEAU_USERNAME", "admin")
    password = os.getenv("TABLEAU_PASSWORD", "admin")

    try:
        auth_payload = {
            "credentials": {
                "name": username,
                "password": password,
                "site": {"contentUrl": site_id},
            }
        }
        resp = requests.post(
            f"{TABLEAU_SERVER}/api/3.9/auth/signin",
            json=auth_payload,
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["credentials"]["token"]

        for csv_file in os.listdir(OUTPUT_DIR):
            if not csv_file.endswith(".csv"):
                continue
            filepath = os.path.join(OUTPUT_DIR, csv_file)
            with open(filepath, "rb") as f:
                upload_resp = requests.post(
                    f"{TABLEAU_SERVER}/api/3.9/sites/{site_id}/datasources",
                    headers={"X-Tableau-Auth": token},
                    files={"file": f},
                    timeout=60,
                )
                if upload_resp.ok:
                    logger.info("Uploaded %s to Tableau.", csv_file)
                else:
                    logger.error("Tableau upload failed for %s: %s", csv_file, upload_resp.text)

    except Exception as e:
        logger.error("Tableau upload error: %s", e)


def upload_to_looker():
    """Upload datasets to Looker via API (Commercial BI)."""
    import requests

    if not LOOKER_API_URL:
        logger.info("Looker not configured, skipping.")
        return

    try:
        auth_resp = requests.post(
            f"{LOOKER_API_URL}/login",
            data={
                "client_id": os.getenv("LOOKER_CLIENT_ID", ""),
                "client_secret": os.getenv("LOOKER_CLIENT_SECRET", ""),
            },
            timeout=30,
        )
        auth_resp.raise_for_status()
        logger.info("Authenticated with Looker. Connect Looker to warehouse directly for best results.")
    except Exception as e:
        logger.error("Looker auth error: %s", e)


def upload_to_power_bi():
    """Push datasets to Power BI via REST API (Commercial BI)."""
    import requests

    if not POWER_BI_WORKSPACE_ID:
        logger.info("Power BI not configured, skipping.")
        return

    access_token = os.getenv("POWER_BI_ACCESS_TOKEN", "")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        dataset_payload = {
            "name": "Pipeline Warehouse Data",
            "defaultMode": "Push",
            "tables": [
                {
                    "name": "daily_orders",
                    "columns": [
                        {"name": "full_date", "dataType": "DateTime"},
                        {"name": "total_orders", "dataType": "Int64"},
                        {"name": "total_revenue", "dataType": "Double"},
                        {"name": "avg_order_value", "dataType": "Double"},
                        {"name": "unique_customers", "dataType": "Int64"},
                    ],
                }
            ],
        }
        resp = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{POWER_BI_WORKSPACE_ID}/datasets",
            headers=headers,
            json=dataset_payload,
            timeout=30,
        )
        if resp.ok:
            logger.info("Power BI dataset created/updated.")
        else:
            logger.warning("Power BI response: %s", resp.text)
    except Exception as e:
        logger.error("Power BI error: %s", e)


if __name__ == "__main__":
    export_warehouse_data()
    export_to_apache_superset()
    export_to_grafana()
    upload_to_tableau()
    upload_to_looker()
    upload_to_power_bi()
