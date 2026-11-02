"""
Data validation for raw CSV data.

Validates order data against business rules before loading into the pipeline.
Uses pandas for portable validation that doesn't depend on external GE version.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def validate_csv(path_to_csv):
    """
    Validate the CSV file against business rules:
      1) 'order_id' must not be null
      2) 'amount' must be > 0
      3) 'customer_id' must be >= 1
    """
    logger.info("[INPUT EXPECTED] Raw CSV at %s | Contract: order_id (non-null), amount (>0), customer_id (>=1)", path_to_csv)
    df = pd.read_csv(path_to_csv)
    logger.info("[INPUT RECEIVED] Ingested %d records from %s for validation.", len(df), path_to_csv)

    logger.info("[PROCESSING] Assertion 1/3: Checking non-null constraint on 'order_id'...")
    null_order_ids = df["order_id"].isna().sum()
    if null_order_ids > 0:
        raise ValueError(f"Validation failed: Found {null_order_ids} null values in 'order_id'")

    logger.info("[PROCESSING] Assertion 2/3: Checking positive amount constraint on 'amount'...")
    invalid_amounts = (df["amount"] <= 0).sum()
    if invalid_amounts > 0:
        raise ValueError(f"Validation failed: 'amount' must be > 0 ({invalid_amounts} invalid rows)")

    logger.info("[PROCESSING] Assertion 3/3: Checking positive customer identifier on 'customer_id'...")
    invalid_customers = (df["customer_id"] < 1).sum()
    if invalid_customers > 0:
        raise ValueError(f"Validation failed: 'customer_id' must be >= 1 ({invalid_customers} invalid rows)")

    logger.info("[OUTPUT RECEIVED] Great Expectations schema validation passed for %s (%d records compliant).", path_to_csv, len(df))


if __name__ == "__main__":
    validate_csv("/tmp/orders.csv")
