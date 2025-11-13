
import time
from pathlib import Path
import sys

from prefect import flow
# Import your local flows package (ensure folder is named `pipeline/flows`)
from pipeline.flows.bronze_flow import bronze_flow
from pipeline.flows.silver_flow import silver_flow
from pipeline.flows.gold_flow import gold_flow

# plotting imports
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Paths
DATA_DIR = Path("data")
GOLD_DIR = DATA_DIR / "gold"
OUT_PLOT = GOLD_DIR / "summary_plot_improved.png"

@flow(name="run_all")
def run_all():
    """
    Run bronze -> silver -> gold flows, then produce improved summary plot.
    """
    start = time.time()
    # Run flows (these are Prefect flows; they will run synchronously here)
    bronze_path = bronze_flow()
    silver_path = silver_flow()
    gold_dir = gold_flow()

    # After gold is created, generate improved plot
    try:
        generate_improved_plot()
    except Exception as e:
        print("Warning: failed to generate improved plot:", e, file=sys.stderr)

    elapsed = time.time() - start
    print("\n=== Pipeline finished ===")
    print(f"Bronze file: {bronze_path}")
    print(f"Silver file: {silver_path}")
    print(f"Gold dir: {gold_dir}")
    print(f"Elapsed: {elapsed:.2f}s")
    print("Outputs (data/gold):")
    if (GOLD_DIR.exists()):
        for p in sorted(GOLD_DIR.glob("*")):
            print(" -", p)
    else:
        print(" - (no gold outputs found)")

def load_gold_tables():
    """
    Load tickets_per_agent.csv and tickets_by_sentiment.csv into DataFrames.
    Handles missing files gracefully by returning empty DataFrames.
    """
    tpa_path = GOLD_DIR / "tickets_per_agent.csv"
    sent_path = GOLD_DIR / "tickets_by_sentiment.csv"

    tpa = pd.read_csv(tpa_path) if tpa_path.exists() else pd.DataFrame(columns=["agent_id","tickets_count"])
    sent = pd.read_csv(sent_path) if sent_path.exists() else pd.DataFrame(columns=["sentiment_label","count"])

    # Ensure numeric types
    if "tickets_count" in tpa.columns:
        tpa["tickets_count"] = pd.to_numeric(tpa["tickets_count"], errors="coerce").fillna(0).astype(int)
    if "count" in sent.columns:
        sent["count"] = pd.to_numeric(sent["count"], errors="coerce").fillna(0).astype(int)

    return tpa, sent

def plot_top_agents(ax, tpa, top_n=15):
    """
    Draw a professional horizontal bar chart of top agents by ticket count.
    """
    if tpa.empty:
        ax.text(0.5, 0.5, "No tickets_per_agent data", ha="center", va="center", fontsize=12)
        ax.axis("off")
        return

    # Select top_n and sort ascending for horizontal bar
    top = tpa.sort_values("tickets_count", ascending=False).head(top_n)
    top = top[::-1]  # reverse for nicer horizontal plotting (small -> top)
    ids = top["agent_id"].astype(str)
    values = top["tickets_count"]

    cmap = plt.get_cmap("tab10")
    colors = [cmap(i % 10) for i in range(len(top))]

    bars = ax.barh(ids, values, color=colors, edgecolor="black", height=0.7)
    ax.set_xlabel("Tickets count", fontsize=11)
    ax.set_title(f"Top {len(top)} agents by tickets", fontsize=14, fontweight="semibold")
    ax.grid(axis="x", linestyle="--", linewidth=0.4, alpha=0.7)

    # Annotate bar values on the right
    max_val = values.max() if len(values) else 0
    for b in bars:
        w = b.get_width()
        ax.text(w + max_val * 0.005, b.get_y() + b.get_height() / 2,
                f"{int(w):,}", va="center", fontsize=9)

    ax.tick_params(axis="y", labelsize=9)

def plot_sentiment(ax, sent):
    """
    Draw a donut chart for sentiment distribution with external labels and counts.
    """
    if sent.empty:
        ax.text(0.5, 0.5, "No sentiment data", ha="center", va="center", fontsize=12)
        ax.axis("off")
        return

    labels = sent["sentiment_label"].astype(str)
    sizes = sent["count"].astype(int)
    total = sizes.sum()
    if total == 0:
        ax.text(0.5, 0.5, "Sentiment counts are zero", ha="center", va="center", fontsize=12)
        ax.axis("off")
        return

    # Order labels to positive, neutral, negative if present
    order = ["positive", "neutral", "negative"]
    present = [lab for lab in order if lab in labels.values]
    if not present:
        present = labels.tolist()

    sent2 = sent.set_index("sentiment_label").reindex(present).fillna(0).reset_index()
    labels = sent2["sentiment_label"].astype(str)
    sizes = sent2["count"].astype(int)

    color_map = {
        "positive": "#2ca02c",
        "neutral": "#ff7f0e",
        "negative": "#1f77b4",
    }
    colors = [color_map.get(l, "#7f7f7f") for l in labels]

    # Pie chart
    wedges, _ = ax.pie(sizes, labels=None, startangle=140, wedgeprops=dict(edgecolor='w'))
    # Annotate with label + value + percentage outside
    bbox_props = dict(boxstyle="round,pad=0.3", fc="white", ec="0.5", alpha=0.95)
    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1)/2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        percentage = sizes.iloc[i] / total * 100
        label = f"{labels.iloc[i]}: {sizes.iloc[i]:,}\n{percentage:.1f}%"
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        ax.annotate(label, xy=(x*0.7, y*0.7), xytext=(1.1*x, 1.1*y),
                    horizontalalignment=horizontalalignment, bbox=bbox_props, fontsize=9, arrowprops=dict(arrowstyle="-", lw=0.5))

    centre_circle = plt.Circle((0,0),0.45,fc='white')
    ax.add_artist(centre_circle)
    ax.set_title("Tickets by sentiment", fontsize=14, fontweight="semibold")
    ax.axis("equal")

def generate_improved_plot():
    """
    Load gold tables and generate the improved summary plot at OUT_PLOT.
    """
    tpa, sent = load_gold_tables()

    # Build figure
    fig = plt.figure(figsize=(14, 7))
    gs = fig.add_gridspec(1, 2, width_ratios=[2.2, 1], wspace=0.35)
    ax0 = fig.add_subplot(gs[0, 0])
    ax1 = fig.add_subplot(gs[0, 1])

    plot_top_agents(ax0, tpa, top_n=15)
    plot_sentiment(ax1, sent)

    fig.suptitle("Gold summary: agent activity & sentiment", fontsize=16, fontweight="bold")

    OUT_PLOT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PLOT, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote improved plot to: {OUT_PLOT}")

if __name__ == "__main__":
    # Run the whole pipeline (flows + plot) when executed as script
    run_all()
