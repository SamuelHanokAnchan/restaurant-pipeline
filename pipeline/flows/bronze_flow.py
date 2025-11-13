# prefect/flows/bronze_flow.py
"""
Bronze flow (Prefect).
Reads raw CSVs and the support_tickets JSONL and writes a single bronze CSV.
"""

from pathlib import Path
import pandas as pd
import logging
from prefect import flow, get_run_logger
import re

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw_csvs"
BRONZE_DIR = DATA_DIR / "bronze"
BRONZE_FILE = BRONZE_DIR / "bronze.csv"

# helper: snake_case normalization
def _to_snake(s: str) -> str:
    s = s or ""
    s = re.sub(r"[ \-]+", "_", s)
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"__+", "_", s)
    return s.strip("_").lower()

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_to_snake(c) for c in df.columns]
    return df

@flow(name="bronze_flow")
def bronze_flow():
    logger = get_run_logger()
    logger.info("Starting Bronze flow")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Read CSV files present in raw_csvs/
    csv_names = ["customers.csv", "orders.csv", "stores.csv", "products.csv", "items.csv", "supplies.csv"]
    csv_dfs = []
    for fname in csv_names:
        path = RAW_DIR / fname
        if path.exists():
            logger.info(f"Reading CSV: {path}")
            try:
                df = pd.read_csv(path, dtype=str)  # read as strings to avoid type surprises
                df = normalize_df_columns(df)
                df["_src"] = fname.replace(".csv","")
                csv_dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {path}: {e}")
        else:
            logger.info(f"CSV not found (skipping): {path}")

    if csv_dfs:
        df_csv_combined = pd.concat(csv_dfs, ignore_index=True, sort=False)
    else:
        # empty frame
        df_csv_combined = pd.DataFrame()

    # Read JSONL support tickets
    jsonl_path = DATA_DIR / "support_tickets.jsonl"
    if jsonl_path.exists():
        logger.info(f"Reading JSONL: {jsonl_path} (lines=True)")
        try:
            df_json = pd.read_json(jsonl_path, lines=True, dtype=False)
            df_json = normalize_df_columns(df_json)
            # flatten sentiment.score -> sentiment_score if needed
            if "sentiment" in df_json.columns:
                # if sentiment is dict-like
                try:
                    df_json["sentiment_score"] = df_json["sentiment"].apply(lambda x: x.get("score") if isinstance(x, dict) else None)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed reading JSONL: {e}")
            df_json = pd.DataFrame()
    else:
        logger.info("support_tickets.jsonl not found. Proceeding with CSVs only.")
        df_json = pd.DataFrame()

    # Union by columns: align columns, allow missing
    if not df_csv_combined.empty and not df_json.empty:
        # combine columns union
        combined = pd.concat([df_csv_combined, df_json], ignore_index=True, sort=False)
    elif not df_csv_combined.empty:
        combined = df_csv_combined
    else:
        combined = df_json

    # Ensure consistent column names (snake_case)
    combined = normalize_df_columns(combined)

    # Write bronze CSV (single file)
    combined.to_csv(BRONZE_FILE, index=False)
    logger.info(f"Wrote bronze CSV: {BRONZE_FILE}")

    return str(BRONZE_FILE)
