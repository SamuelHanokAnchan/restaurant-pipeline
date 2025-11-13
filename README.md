# Restaurant Ticket Processing Pipeline

**Bronze → Silver → Gold Architecture using Python and Prefect**

---

## Overview

This project implements a complete data engineering pipeline for processing restaurant operational data and customer support ticket records. The pipeline follows a classical **Bronze → Silver → Gold architecture** and is fully orchestrated using **Prefect**, ensuring structured, traceable, and reproducible data transformations.

The system consolidates raw CSV files and a JSONL support ticket dataset, cleans and enriches the data, produces analytical aggregates, and generates a final visualization summarizing agent performance and sentiment distribution.

**The entire pipeline can be executed with a single command.**

This work was completed as part of the Applied Data Engineering coursework.

---

## Project Objectives

- Ingest raw structured and semi-structured data into a unified bronze layer
- Clean and standardize the data into a silver layer
- Produce business-ready aggregated datasets in the gold layer
- Automate pipeline execution using Prefect flows
- Generate a professional visualization summarizing key metrics
- Provide a clear, reproducible workflow suitable for academic and production contexts

---

## Folder Structure

```
restaurant_pipeline/
│
├── run_pipeline.py               # Main entrypoint (run entire pipeline + plot)
│
├── pipeline/
│   ├── flows/
│   │   ├── bronze_flow.py        # Ingest raw CSV + JSONL → bronze
│   │   ├── silver_flow.py        # Clean → silver
│   │   └── gold_flow.py          # Aggregate → gold
│   │
│   └── utils/                    # Optional utilities
│
├── data/
│   ├── raw_csvs/                 # Raw input CSVs (customers, orders, etc.)
│   ├── support_tickets.jsonl     # Raw JSONL file with support tickets
│   ├── bronze/                   # bronze.csv (combined raw data)
│   ├── silver/                   # silver.csv (clean data)
│   └── gold/                     # Aggregated CSVs + final plots
│
├── requirements.txt              # Python dependencies
├── README.md                     # Project documentation
└── .gitignore                    # Git ignore rules
```

---

## How the Pipeline Works

### 1. Bronze Layer

**File:** `pipeline/flows/bronze_flow.py`

The bronze layer ingests all raw data into a unified dataset:

- Reads all CSVs from `data/raw_csvs/`
- Accepts both standard filenames (e.g., `orders.csv`) and files prefixed with `raw_` (e.g., `raw_orders.csv`)
- Reads and normalizes the JSONL support ticket file
- Applies consistent `snake_case` column standardization
- Combines all sources into one unified dataset

**Output:** `data/bronze/bronze.csv`

### 2. Silver Layer

**File:** `pipeline/flows/silver_flow.py`

The silver layer performs data cleaning and quality checks:

- Cleans and validates timestamps
- Calculates ticket response times
- Filters invalid rows
- Removes duplicates and handles missing values
- Standardizes data types

**Output:** `data/silver/silver.csv`

### 3. Gold Layer

**File:** `pipeline/flows/gold_flow.py`

The gold layer produces business-ready aggregated datasets:

- `tickets_per_agent.csv` — Ticket volume by agent
- `tickets_by_status.csv` — Ticket distribution by status
- `tickets_by_sentiment.csv` — Sentiment analysis breakdown
- `avg_response_time_per_agent.csv` — Response time metrics per agent
- `gold_summary.csv` — Consolidated summary dataset

### 4. Final Visualization

**Output:** `data/gold/summary_plot_improved.png`

The visualization is generated automatically and includes:

- Horizontal bar chart of top agents by ticket volume
- Donut chart showing sentiment distribution (positive, neutral, negative)

---

## Requirements

- **Python 3.10+**
- **Prefect** — Workflow orchestration
- **Pandas** — Data manipulation
- **NumPy** — Numerical computing
- **Matplotlib** — Data visualization

Install all dependencies:

```bash
pip install -r requirements.txt
```

---

## Quick Start

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Run the Full Pipeline

```bash
python run_pipeline.py
```

**What happens when you run the command:**

1. Prefect initializes a flow execution
2. Bronze → Silver → Gold flows execute in sequence
3. All required CSV files are produced
4. The improved visualization is generated automatically
5. Pipeline completes with a summary report

---

## Expected Outputs

After running the pipeline, the following files will be created:

### Bronze Layer
- `data/bronze/bronze.csv` — Raw combined dataset

### Silver Layer
- `data/silver/silver.csv` — Cleaned and standardized data

### Gold Layer
- `data/gold/tickets_per_agent.csv` — Tickets by agent
- `data/gold/tickets_by_status.csv` — Tickets by status
- `data/gold/tickets_by_sentiment.csv` — Sentiment distribution
- `data/gold/avg_response_time_per_agent.csv` — Average response times
- `data/gold/gold_summary.csv` — Consolidated summary
- `data/gold/summary_plot_improved.png` — Final visualization

---

## Design Principles

Professional Standards — Follows industry best practices for multi-layer data pipelines

Modular Architecture — Easily extendable to include databases, dashboards, or API ingestion

Full Orchestration — Prefect ensures flows remain traceable, reproducible, and fully automated

Single-Command Execution — Entire pipeline runs with one command for easy evaluation and deployment

---

## Notes for Reviewers

- The project follows professional data engineering standards for multi-layer pipelines
- All logic is modular, allowing seamless extensions such as database sinks, dashboards, or API integrations
- Prefect orchestration ensures that flows remain traceable, reproducible, and fully automated
- The system is intentionally designed to execute with a single command for ease of evaluation

---

## Contact

For questions or feedback regarding this project:

**Samuel Hanok**  
Email: samuelhanok0@gmail.com

---

**Last Updated:** November 2025