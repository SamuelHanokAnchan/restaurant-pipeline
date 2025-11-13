# prefect/flows/silver_flow.py
"""
Silver flow (Prefect).
Reads bronze CSV, parses timestamps, computes response_time_hours, filters null ticket_id, writes silver CSV.
"""

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger
import numpy as np

DATA_DIR = Path("data")
BRONZE_FILE = DATA_DIR / "bronze" / "bronze.csv"
SILVER_DIR = DATA_DIR / "silver"
SILVER_FILE = SILVER_DIR / "silver.csv"

@flow(name="silver_flow")
def silver_flow():
    logger = get_run_logger()
    logger.info("Starting Silver flow")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    if not BRONZE_FILE.exists():
        logger.error(f"Bronze file not found: {BRONZE_FILE}")
        return None

    df = pd.read_csv(BRONZE_FILE, dtype=str)

    # Parse relevant timestamps where present
    # columns to attempt: first_response_at, resolved_at, updated_at (others tolerated)
    for col in ["first_response_at", "resolved_at", "updated_at", "created_at", "order_timestamp", "last_delivery_at", "sla_due_at"]:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            except Exception:
                df[col] = pd.to_datetime(df[col].astype(str), errors="coerce", utc=True)

    # response_time_hours = (resolved_at - first_response_at) / 3600
    if "resolved_at" in df.columns and "first_response_at" in df.columns:
        df["response_time_hours"] = (df["resolved_at"] - df["first_response_at"]).dt.total_seconds() / 3600.0
    else:
        df["response_time_hours"] = np.nan

    # Filter out rows with null ticket_id (if the column exists)
    if "ticket_id" in df.columns:
        before = len(df)
        df = df[df["ticket_id"].notna() & (df["ticket_id"].astype(str).str.strip() != "")]
        after = len(df)
        logger.info(f"Filtered null ticket_id rows: {before} -> {after}")
    else:
        logger.info("No ticket_id column in bronze; silver will contain all rows as-is.")

    # Write silver CSV (ensure proper formatting)
    df.to_csv(SILVER_FILE, index=False)
    logger.info(f"Wrote silver CSV: {SILVER_FILE}")

    return str(SILVER_FILE)
