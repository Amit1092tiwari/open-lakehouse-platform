"""
Open-Source Data Warehouse Connector (PostgreSQL OLAP & DuckDB).

Replaces proprietary cloud data warehouses (e.g., Snowflake) with 100% open-source
analytical engines:
1. PostgreSQL Star Schema (OLAP warehouse running on port 5432).
2. DuckDB Embedded Columnar OLAP engine (zero external dependencies, direct
   Parquet/Iceberg/S3-MinIO analytics).

Provides unified APIs:
- get_connection(engine_type)
- execute_sql(sql, params, engine_type)
- execute_multi(sql_statements, engine_type)
- query_to_dataframe(sql, params, engine_type)
- load_dataframe(df, table_name, engine_type, overwrite)
- stage_and_copy(local_file_path, table_name, engine_type)
- check_connection(engine_type)
"""

import io
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default open source warehouse engine: "postgres" or "duckdb"
DEFAULT_ENGINE = os.getenv("WAREHOUSE_ENGINE", "duckdb" if not os.getenv("POSTGRES_HOST") and not os.getenv("DB_HOST") else "postgres").lower()

# PostgreSQL Connection Configuration
PG_HOST = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "postgres"))
PG_PORT = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
PG_DATABASE = os.getenv("POSTGRES_DB", "processed_db")
PG_USER = os.getenv("POSTGRES_USER", "pipeline_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_secret_2024")

# DuckDB Storage Path (":memory:" or local persistent DB path)
DUCKDB_PATH = os.getenv("DUCKDB_PATH", ":memory:")

_IN_MEMORY_FALLBACK_CONN: Optional[sqlite3.Connection] = None
_DUCKDB_CONN: Optional[Any] = None


def get_postgres_connection():
    """Create a connection to the PostgreSQL open-source warehouse."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=10,
        )
        logger.info("[OUTPUT RECEIVED] Connected to PostgreSQL open-source warehouse: %s@%s:%s/%s", PG_USER, PG_HOST, PG_PORT, PG_DATABASE)
        return conn
    except Exception as e:
        logger.warning("Could not connect to PostgreSQL (%s). Falling back to DuckDB/embedded.", e)
        raise


def get_duckdb_connection():
    """Create a connection to the DuckDB open-source columnar analytics engine."""
    global _IN_MEMORY_FALLBACK_CONN, _DUCKDB_CONN
    try:
        import duckdb
        if DUCKDB_PATH == ":memory:":
            if _DUCKDB_CONN is None:
                _DUCKDB_CONN = duckdb.connect(database=":memory:", read_only=False)
            return _DUCKDB_CONN
        conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)
        logger.info("[OUTPUT RECEIVED] Connected to DuckDB open-source columnar engine at: %s", DUCKDB_PATH)
        return conn
    except ImportError:
        # Fallback to shared in-process SQLite instance for embedded testing without duckdb binary
        if _IN_MEMORY_FALLBACK_CONN is None or DUCKDB_PATH == ":memory:":
            if _IN_MEMORY_FALLBACK_CONN is None:
                _IN_MEMORY_FALLBACK_CONN = sqlite3.connect(":memory:", check_same_thread=False)
            return _IN_MEMORY_FALLBACK_CONN
        sqlite_path = DUCKDB_PATH.replace(".duckdb", ".sqlite")
        return sqlite3.connect(sqlite_path, check_same_thread=False)


def get_connection(engine_type: Optional[str] = None):
    """
    Get a database connection to the active open-source warehouse.
    
    :param engine_type: 'postgres' or 'duckdb'. Defaults to WAREHOUSE_ENGINE environment variable.
    """
    engine = (engine_type or DEFAULT_ENGINE).lower()
    if engine in ("duckdb", "embedded"):
        return get_duckdb_connection()
    try:
        return get_postgres_connection()
    except Exception:
        # Fallback to DuckDB / embedded in-process storage
        return get_duckdb_connection()


def execute_sql(sql: str, params: Optional[Any] = None, engine_type: Optional[str] = None):
    """
    Execute a SQL statement on the open-source warehouse.
    Returns the cursor or result.
    """
    logger.info("[INPUT EXPECTED] SQL statement to execute on open-source warehouse.")
    logger.info("[PROCESSING] Executing SQL: %s...", sql.strip()[:80])
    conn = get_connection(engine_type)
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if hasattr(conn, "commit"):
            conn.commit()
        logger.info("[OUTPUT RECEIVED] Executed SQL successfully on open-source warehouse.")
        return cursor
    except Exception as e:
        logger.error("Open-source warehouse SQL error: %s", str(e))
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        if conn is not _IN_MEMORY_FALLBACK_CONN and conn is not _DUCKDB_CONN:
            conn.close()


def execute_multi(sql_statements: List[str], engine_type: Optional[str] = None):
    """Execute multiple SQL statements sequentially in a single transaction."""
    logger.info("[INPUT EXPECTED] Batch of %d SQL statements.", len(sql_statements))
    conn = get_connection(engine_type)
    try:
        cursor = conn.cursor()
        executed_count = 0
        for sql in sql_statements:
            sql_clean = sql.strip()
            if sql_clean:
                cursor.execute(sql_clean)
                executed_count += 1
                logger.debug("[PROCESSING] Executed SQL statement %d/%d: %s...", executed_count, len(sql_statements), sql_clean[:60])
        if hasattr(conn, "commit"):
            conn.commit()
        logger.info("[OUTPUT RECEIVED] Successfully executed and committed %d SQL statements.", executed_count)
        return executed_count
    except Exception as e:
        logger.error("Open-source warehouse multi-SQL error: %s", str(e))
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        if conn is not _IN_MEMORY_FALLBACK_CONN and conn is not _DUCKDB_CONN:
            conn.close()


def query_to_dataframe(sql: str, params: Optional[Any] = None, engine_type: Optional[str] = None) -> pd.DataFrame:
    """Execute query and return results as a pandas DataFrame."""
    logger.info("[INPUT EXPECTED] SQL analytical query: %s...", sql.strip()[:80])
    conn = get_connection(engine_type)
    try:
        logger.info("[PROCESSING] Executing query and building pandas DataFrame...")
        if hasattr(conn, "execute") and hasattr(conn, "df"):
            res = conn.execute(sql, params) if params else conn.execute(sql)
            df = res.df()
        else:
            df = pd.read_sql(sql, conn, params=params)
        logger.info("[OUTPUT RECEIVED] Analytical query returned %d rows and %d columns.", len(df), len(df.columns))
        return df
    except Exception as e:
        logger.error("Open-source warehouse query error: %s", str(e))
        raise
    finally:
        if conn is not _IN_MEMORY_FALLBACK_CONN and conn is not _DUCKDB_CONN:
            conn.close()


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine_type: Optional[str] = None,
    overwrite: bool = False,
) -> int:
    """
    Load a pandas DataFrame into an open-source warehouse table.
    """
    if df is None or df.empty:
        logger.warning("[PROCESSING] Empty DataFrame provided for table '%s'; skipping load.", table_name)
        return 0

    logger.info("[INPUT RECEIVED] Received DataFrame with %d rows and columns: %s", len(df), list(df.columns))
    logger.info("[OUTPUT EXPECTED] Destination table: '%s' (mode: %s)", table_name, "overwrite" if overwrite else "append")

    engine = (engine_type or DEFAULT_ENGINE).lower()
    conn = get_connection(engine)
    try:
        table_clean = table_name.lower().split(".")[-1]

        # If DuckDB native connection
        if hasattr(conn, "register") and not isinstance(conn, sqlite3.Connection):
            logger.info("[PROCESSING] Registering virtual view in DuckDB and executing %s...", "CREATE TABLE" if overwrite else "INSERT")
            conn.register("temp_df_view", df)
            if overwrite:
                conn.execute(f"CREATE OR REPLACE TABLE {table_clean} AS SELECT * FROM temp_df_view")
            else:
                conn.execute(f"INSERT INTO {table_clean} SELECT * FROM temp_df_view")
            logger.info("[OUTPUT RECEIVED] Loaded %d rows into DuckDB columnar table '%s'.", len(df), table_clean)
            return len(df)

        # If SQLite fallback connection
        if isinstance(conn, sqlite3.Connection):
            logger.info("[PROCESSING] Loading DataFrame via SQLite engine into table '%s'...", table_clean)
            if_exists_mode = "replace" if overwrite else "append"
            df.to_sql(table_clean, con=conn, if_exists=if_exists_mode, index=False)
            logger.info("[OUTPUT RECEIVED] Loaded %d rows into SQLite in-process table '%s'.", len(df), table_clean)
            return len(df)

        # PostgreSQL connection
        try:
            from sqlalchemy import create_engine
            logger.info("[PROCESSING] Loading DataFrame via PostgreSQL SQLAlchemy engine...")
            url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
            sql_engine = create_engine(url)
            if_exists_mode = "replace" if overwrite else "append"
            df.to_sql(table_clean, con=sql_engine, if_exists=if_exists_mode, index=False)
            logger.info("[OUTPUT RECEIVED] Loaded %d rows into PostgreSQL table '%s'.", len(df), table_clean)
            return len(df)
        except Exception:
            logger.info("[PROCESSING] Executing raw parameterized cursor batch insert...")
            cursor = conn.cursor()
            cols = list(df.columns)
            placeholders = ", ".join(["%s"] * len(cols))
            col_names = ", ".join(cols)
            if overwrite:
                cursor.execute(f"DELETE FROM {table_clean}")
            insert_query = f"INSERT INTO {table_clean} ({col_names}) VALUES ({placeholders})"
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            if hasattr(conn, "commit"):
                conn.commit()
            logger.info("[OUTPUT RECEIVED] Loaded %d rows into table '%s' via cursor.", len(df), table_clean)
            return len(df)
    except Exception as e:
        logger.error("Open-source warehouse load error for '%s': %s", table_name, str(e))
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        if conn is not _IN_MEMORY_FALLBACK_CONN and conn is not _DUCKDB_CONN:
            conn.close()


def stage_and_copy(
    local_file_path: str,
    table_name: str,
    engine_type: Optional[str] = None,
) -> int:
    """
    Stage and ingest a local CSV/Parquet file directly into the warehouse table.
    """
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Staging file not found: {local_file_path}")

    logger.info("[INPUT RECEIVED] Staging file: %s (size: %d bytes)", local_file_path, os.path.getsize(local_file_path))
    logger.info("[OUTPUT EXPECTED] Destination table: '%s'", table_name)

    conn = get_connection(engine_type)
    table_clean = table_name.lower().split(".")[-1]
    filename = os.path.basename(local_file_path)
    is_parquet = filename.endswith(".parquet")

    try:
        # Check if DuckDB native
        if hasattr(conn, "execute") and hasattr(conn, "df") and not isinstance(conn, sqlite3.Connection):
            logger.info("[PROCESSING] Executing direct DuckDB zero-copy scan of %s...", filename)
            if is_parquet:
                sql = f"INSERT INTO {table_clean} SELECT * FROM read_parquet('{local_file_path}')"
            else:
                sql = f"INSERT INTO {table_clean} SELECT * FROM read_csv_auto('{local_file_path}', header=True)"
            conn.execute(sql)
            logger.info("[OUTPUT RECEIVED] DuckDB copied '%s' into table '%s'.", filename, table_clean)
            return 1

        # Fallback via pandas
        logger.info("[PROCESSING] Parsing file into DataFrame for staging insert...")
        df = pd.read_parquet(local_file_path) if is_parquet else pd.read_csv(local_file_path)
        load_dataframe(df, table_clean, engine_type)
        logger.info("[OUTPUT RECEIVED] Staged and copied '%s' into table '%s' successfully.", filename, table_clean)
        return 1
    except Exception as e:
        logger.error("Staging copy error for '%s' -> '%s': %s", local_file_path, table_name, str(e))
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        if conn is not _IN_MEMORY_FALLBACK_CONN and conn is not _DUCKDB_CONN:
            conn.close()


def check_connection(engine_type: Optional[str] = None) -> bool:
    """Verify open-source warehouse connectivity. Returns True if alive."""
    try:
        conn = get_connection(engine_type)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        res = cursor.fetchone()
        if conn is not _IN_MEMORY_FALLBACK_CONN and conn is not _DUCKDB_CONN:
            conn.close()
        logger.info("[OUTPUT RECEIVED] Open-source warehouse connectivity check passed (result=%s).", res)
        return True
    except Exception as e:
        logger.warning("Open-source warehouse connection check failed: %s", str(e))
        return False


if __name__ == "__main__":
    status = check_connection()
    print(f"Open-Source Warehouse (PostgreSQL / DuckDB) connection status: {'SUCCESS' if status else 'FAILED'}")
