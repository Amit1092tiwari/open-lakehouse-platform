"""
Open-Source Warehouse Package.

Provides connection, query execution, DataFrame loading, and staging
utilities for 100% open-source analytical data warehouses (PostgreSQL OLAP
Star Schema and DuckDB Columnar Lakehouse Engine).
"""

from .warehouse_connector import (
    check_connection,
    execute_multi,
    execute_sql,
    get_connection,
    load_dataframe,
    query_to_dataframe,
    stage_and_copy,
)

__all__ = [
    "get_connection",
    "execute_sql",
    "execute_multi",
    "query_to_dataframe",
    "load_dataframe",
    "stage_and_copy",
    "check_connection",
]

