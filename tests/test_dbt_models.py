"""Unit tests for dbt Medallion architecture models and YAML configurations."""

import os
import re
import yaml
import pytest

DBT_ROOT = os.path.join(os.path.dirname(__file__), "..", "dbt_medallion")


def test_dbt_project_file():
    """Verify dbt_project.yml exists and has proper medallion configuration."""
    project_path = os.path.join(DBT_ROOT, "dbt_project.yml")
    assert os.path.exists(project_path)

    with open(project_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert config["name"] == "datapipeline_medallion"
    assert config["version"] == "1.0.0"
    assert "models" in config

    medallion = config["models"]["datapipeline_medallion"]
    assert medallion["staging"]["+materialized"] == "view"
    assert medallion["intermediate"]["+materialized"] == "view"
    assert medallion["marts"]["+materialized"] == "table"


def test_dbt_staging_models():
    """Verify staging models exist and reference source raw tables."""
    stg_orders_path = os.path.join(DBT_ROOT, "models", "staging", "ecommerce", "stg_ecommerce__orders.sql")
    stg_customers_path = os.path.join(DBT_ROOT, "models", "staging", "ecommerce", "stg_ecommerce__customers.sql")

    assert os.path.exists(stg_orders_path)
    assert os.path.exists(stg_customers_path)

    with open(stg_orders_path, "r", encoding="utf-8") as f:
        sql = f.read()
    assert "source('ecommerce', 'raw_orders')" in sql
    assert "order_id" in sql
    assert "order_amount" in sql

    with open(stg_customers_path, "r", encoding="utf-8") as f:
        sql = f.read()
    assert "source('ecommerce', 'raw_customers')" in sql
    assert "customer_id" in sql


def test_dbt_intermediate_enrichment_model():
    """Verify intermediate model joins orders and customers with business logic."""
    int_path = os.path.join(DBT_ROOT, "models", "intermediate", "int_orders_enriched.sql")
    assert os.path.exists(int_path)

    with open(int_path, "r", encoding="utf-8") as f:
        sql = f.read()

    assert "ref('stg_ecommerce__orders')" in sql
    assert "ref('stg_ecommerce__customers')" in sql
    assert "order_hk" in sql  # Surrogate hash key
    assert "net_realized_revenue" in sql  # FinOps calculation
    assert "order_value_tier" in sql


def test_dbt_gold_marts():
    """Verify gold dimension and fact models exist and follow dimensional standards."""
    dim_cust_path = os.path.join(DBT_ROOT, "models", "marts", "dim_customers.sql")
    fct_orders_path = os.path.join(DBT_ROOT, "models", "marts", "fct_daily_customer_orders.sql")

    assert os.path.exists(dim_cust_path)
    assert os.path.exists(fct_orders_path)

    with open(dim_cust_path, "r", encoding="utf-8") as f:
        dim_sql = f.read()
    assert "ref('int_orders_enriched')" in dim_sql
    assert "customer_loyalty_segment" in dim_sql or "loyalty_tier" in dim_sql

    with open(fct_orders_path, "r", encoding="utf-8") as f:
        fct_sql = f.read()
    assert "ref('int_orders_enriched')" in fct_sql
    assert "incremental" in fct_sql
    assert "cluster_by" in fct_sql


def test_dbt_schema_yaml_files():
    """Verify schema YAML files contain model descriptions and column tests."""
    for yml_rel in [
        os.path.join("models", "staging", "ecommerce", "_stg_ecommerce__models.yml"),
        os.path.join("models", "intermediate", "_int_models.yml"),
        os.path.join("models", "marts", "_marts_models.yml"),
    ]:
        full_path = os.path.join(DBT_ROOT, yml_rel)
        assert os.path.exists(full_path), f"Missing YAML file: {yml_rel}"
        with open(full_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        assert "models" in data or "sources" in data
