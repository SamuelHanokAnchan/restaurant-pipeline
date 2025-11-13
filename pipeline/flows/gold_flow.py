# prefect/flows/gold_flow.py
"""
Gold flow (Prefect).
Reads silver CSV and produces 4 business-facing CSVs:
  1) tickets_per_agent.csv (agent_id, tickets_count)
  2) tickets_by_status.csv (status, tickets_count)
  3) tickets_by_sentiment.csv (sentiment_label, count)
  4) avg_response_time_per_agent.csv (agent_id, avg_response_time_hours)
Also writes a combined gold_summary.csv (metric, key, value) and a small plot.
"""

from pathlib import Path
import pandas as pd
from prefect import flow, get_run_logger
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA_DIR = Path("data")
SILVER_FILE = DATA_DIR / "silver" / "silver.csv"
GOLD_DIR = DATA_DIR / "gold"

@flow(name="gold_flow")
def gold_flow():
    logger = get_run_logger()
    logger.info("Starting Gold flow")
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    if not SILVER_FILE.exists():
        logger.error(f"Silver file not found: {SILVER_FILE}")
        return None

    df = pd.read_csv(SILVER_FILE)

    # 1) tickets_per_agent.csv
    if "agent_id" in df.columns:
        tpa = df.groupby("agent_id", dropna=True).size().reset_index(name="tickets_count")
    else:
        tpa = pd.DataFrame(columns=["agent_id", "tickets_count"])
    tpa.to_csv(GOLD_DIR / "tickets_per_agent.csv", index=False)
    logger.info("Wrote tickets_per_agent.csv")

    # 2) tickets_by_status.csv
    if "status" in df.columns:
        tbs = df.groupby("status", dropna=True).size().reset_index(name="tickets_count")
    else:
        tbs = pd.DataFrame(columns=["status", "tickets_count"])
    tbs.to_csv(GOLD_DIR / "tickets_by_status.csv", index=False)
    logger.info("Wrote tickets_by_status.csv")

    # 3) tickets_by_sentiment.csv
    # sentiment score may be in sentiment_score or sentiment.score flattening
    if "sentiment_score" in df.columns:
        df["sentiment_score_val"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
    elif "sentiment" in df.columns:
        # attempt to parse if it's a dict-like string
        try:
            df["sentiment_score_val"] = pd.to_numeric(df["sentiment"].apply(lambda x: x.get("score") if isinstance(x, dict) else np.nan), errors="coerce")
        except Exception:
            df["sentiment_score_val"] = np.nan
    else:
        df["sentiment_score_val"] = np.nan

    def sentiment_label(v):
        try:
            v = float(v)
        except Exception:
            return "neutral"
        if v >= 0.5:
            return "positive"
        if v <= -0.5:
            return "negative"
        return "neutral"

    df["sentiment_label"] = df["sentiment_score_val"].apply(sentiment_label)
    tbsent = df.groupby("sentiment_label", dropna=False).size().reset_index(name="count")
    tbsent.to_csv(GOLD_DIR / "tickets_by_sentiment.csv", index=False)
    logger.info("Wrote tickets_by_sentiment.csv")

    # 4) avg_response_time_per_agent.csv
    if "agent_id" in df.columns and "response_time_hours" in df.columns:
        # ensure numeric
        df["response_time_hours"] = pd.to_numeric(df["response_time_hours"], errors="coerce")
        avg_resp = df.groupby("agent_id", dropna=True)["response_time_hours"].mean().reset_index(name="avg_response_time_hours")
    else:
        avg_resp = pd.DataFrame(columns=["agent_id", "avg_response_time_hours"])
    avg_resp.to_csv(GOLD_DIR / "avg_response_time_per_agent.csv", index=False)
    logger.info("Wrote avg_response_time_per_agent.csv")

    # gold_summary.csv: metric, key, value
    rows = []

    for _, r in tpa.iterrows():
        rows.append(("tickets_per_agent", r["agent_id"], int(r["tickets_count"])))
    for _, r in tbs.iterrows():
        rows.append(("tickets_by_status", r["status"], int(r["tickets_count"])))
    for _, r in tbsent.iterrows():
        rows.append(("tickets_by_sentiment", r["sentiment_label"], int(r["count"])))
    for _, r in avg_resp.iterrows():
        # format avg with 4 decimals
        rows.append(("avg_response_time_per_agent", r["agent_id"], float(round(r["avg_response_time_hours"] or 0.0, 4))))

    summary_df = pd.DataFrame(rows, columns=["metric", "key", "value"])
    summary_df.to_csv(GOLD_DIR / "gold_summary.csv", index=False)
    logger.info("Wrote gold_summary.csv")

    # Simple plot (save to summary_plot.png)
    try:
        fig, axes = plt.subplots(2, 1, figsize=(8, 10))
        # top agents by tickets
        if not tpa.empty:
            top_agents = tpa.sort_values("tickets_count", ascending=False).head(10)
            axes[0].bar(top_agents["agent_id"].astype(str), top_agents["tickets_count"])
            axes[0].set_title("Top 10 agents by tickets")
            axes[0].tick_params(axis="x", rotation=45)
        else:
            axes[0].text(0.5, 0.5, "No tickets_per_agent data", ha="center", va="center")
            axes[0].axis("off")

        # sentiment pie
        if not tbsent.empty:
            axes[1].pie(tbsent["count"], labels=tbsent["sentiment_label"].astype(str), autopct="%1.1f%%")
            axes[1].set_title("Tickets by sentiment")
        else:
            axes[1].text(0.5, 0.5, "No sentiment data", ha="center", va="center")
            axes[1].axis("off")

        fig.tight_layout()
        plot_path = GOLD_DIR / "summary_plot.png"
        fig.savefig(plot_path, bbox_inches="tight")
        plt.close(fig)
        logger.info(f"Wrote summary plot: {plot_path}")
    except Exception as e:
        logger.warning(f"Could not generate plot: {e}")

    return str(GOLD_DIR)
