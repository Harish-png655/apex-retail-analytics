# Apex Retail Analytics: Local Defensive Medallion Lakehouse

![CI Pipeline](https://github.com/Harish-png655/apex-retail-analytics/actions/workflows/ci.yml/badge.svg)

An end-to-end, zero-cloud-cost Data Lakehouse architecture implementing **Bronze**, **Silver**, and **Gold** processing tiers. Built using **LocalStack S3**, **PostgreSQL**, **Pandas**, **PyArrow**, **PySpark**, **Boto3**, and **Docker Desktop**.

This architecture enforces strict schema validation, defensive quality routing to a **Dead-Letter Queue (DLQ)** for non-compliant records, relational star-schema loading, and a **dual-engine Silver processing layer** supporting both lightweight Pandas/PyArrow and distributed PySpark DAG execution.

---

## 🏛️ Architecture Overview

```text
                      ┌───────────────────────────┐
                      │   Raw Mock Data Storage   │
                      │ (Orders & Returns Parquet)│
                      └─────────────┬─────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                       LOCALSTACK S3 CONTAINER                         │
│                                                                       │
│  BRONZE LAYER (s3://apex-data-lake/bronze/)                           │
│  • Raw Parquet ingestion enriched with `_ingested_at` & `_source_file`│
│                                   │                                   │
│                                   ▼                                   │
│  SILVER LAYER (Dual Engine: Pandas/PyArrow OR PySpark DAG)            │
│  • Schema standardization & type enforcement                          │
│  • Quality rules (non-null IDs, positive amounts, duplicate removal)  │
│  • S3 Promotion Bridge: Converts Spark multi-part directories into    │
│    standard single-file keys (`orders_clean.parquet`) for Gold usage │
│                                   │                                   │
│                 ┌─────────────────┴─────────────────┐                 │
│                 │ (Valid)                           │ (Corrupted)     │
│                 ▼                                   ▼                 │
│  Clean Parquet Datasets               Dead-Letter Queue (DLQ)         │
│  `s3://apex-data-lake/silver/`        s3://apex-dead-letter-queue/    │
│  • orders/orders_clean.parquet        silver_quarantine/              │
│  • returns/returns_clean.parquet                                      │
└─────────────────┬─────────────────────────────────────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────────────────────────────────────┐
│                      POSTGRESQL DATA WAREHOUSE                        │
│                    `apex_postgres` (Port 5433)                        │
│                                                                       │
│  • `gold_regional_performance` (Gross/Net sales, return rates, fees)   │
│  • `gold_customer_metrics` (Lifetime spend, orders, activity date)   │
└───────────────────────────────────────────────────────────────────────┘

```

---

## 🗂️ Medallion Tier Specifications

### 🥉 Bronze Tier (Ingestion & Lineage)

* **Path:** `s3://apex-data-lake/bronze/`
* **Format:** Apache Parquet
* **Logic:** Reads landing raw data and appends operational lineage attributes without altering original record values.
* **Metadata Fields:** `_ingested_at` (UTC ISO Timestamp), `_source_file` (Filename origin).

### 🥈 Silver Tier (Dual-Engine Quality Validation & Quarantine)

* **Path:** `s3://apex-data-lake/silver/`
* **Format:** Apache Parquet
* **Execution Options:**
1. **Pandas / PyArrow:** Single-node, memory-efficient processing engine for small-to-medium batch workloads.
2. **PySpark DAG:** Distributed Big Data processing engine utilizing Hadoop S3A file system abstraction and `boto3` key promotion.


* **Logic:** Applies explicit data casting, standardizes lower-case naming conventions, removes duplicates, and filters records against strict rules:
* `order_id` AND `customer_id` NOT NULL
* `amount_usd` > 0 AND `seller_fee` >= 0
* `transaction_date` MUST be a valid timestamp


* **DLQ Routing:** Any record failing validation is isolated and written to `s3://apex-dead-letter-queue/silver_quarantine/` with a `_quarantine_reason` tag.

### 🥇 Gold Tier (Warehouse Models)

* **Target Engine:** PostgreSQL 15 (`apex_warehouse` DB)
* **Logic:** Left joins clean orders with clean returns to calculate return flags and true net financial performance.
* **Target Tables:**
* `gold_regional_performance`: Aggregated by date and region (Gross Sales, Net Sales, Total Fees, Return Count, Return Rate %).
* `gold_customer_metrics`: Aggregated by customer (Lifetime Gross/Net Spend, Total Orders, Last Active Date).



---

## 💻 Repository Structure

```text
apex-retail-analytics/
├── .github/
│   └── workflows/
│       └── ci.yml                  # Automated GitHub Actions testing pipeline
├── .flake8                         # Flake8 linter configuration
├── docker-compose.yml              # LocalStack S3 & PostgreSQL service definitions
├── main.py                         # Single-entrypoint CLI pipeline orchestrator
├── pytest.ini                      # Pytest setup configuration
├── README.md                       # Architecture & execution documentation
├── requirements.txt                # Python environment dependencies
├── scripts/
│   ├── init_minio.py               # LocalStack S3 bucket provisioner
│   ├── data_gen/
│   │   ├── generate_data.py        # Raw data generation script
│   │   └── upload_to_s3.py         # S3 landing uploader
│   ├── etl/
│   │   ├── bronze_ingestion.py     # Bronze layer metadata enrichment
│   │   ├── silver_transformation.py# Default Pandas/PyArrow Silver engine
│   │   └── gold_transformation.py  # Gold metrics compiler & Postgres loader
│   └── spark_jobs/
│       └── spark_silver_transformation.py # PySpark DAG Silver engine & S3 promoter
└── tests/
    ├── test_silver_transformation.py # Silver layer DQ unit tests
    └── test_gold_transformation.py   # Gold metrics aggregation unit tests

```

---

## 🚀 Execution Instructions

### 1. Environment Setup

Clone the repository and activate a Python 3.10+ virtual environment:

```bash
git clone [https://github.com/Harish-png655/apex-retail-analytics.git](https://github.com/Harish-png655/apex-retail-analytics.git)
cd apex-retail-analytics

python -m venv venv
source venv/Scripts/activate  # On Windows Git Bash

pip install -r requirements.txt

```

### 2. Infrastructure Initialization

Spin up LocalStack (Port 4566) and PostgreSQL (Port 5433):

```bash
docker compose up -d

```

### 3. Pipeline Execution (Dual Engine)

Run the full end-to-end pipeline using either processing engine via the CLI `--engine` flag:

```bash
# Option A: Run default engine (Pandas / PyArrow)
python main.py

# Option B: Run distributed PySpark engine
python main.py --engine pyspark

```

---

## 🔍 Database Inspection & Queries

To verify table contents loaded into PostgreSQL:

```bash
python -c "
import sqlalchemy, pandas as pd
engine = sqlalchemy.create_engine('postgresql+psycopg://apex_admin:apex_password@localhost:5433/apex_warehouse')

print('=== TOP 5 REGIONAL SALES DAYS ===')
print(pd.read_sql('SELECT * FROM gold_regional_performance ORDER BY net_sales_usd DESC LIMIT 5;', engine).to_string(index=False))

print('=== TOP 5 CUSTOMERS BY NET SPEND ===')
print(pd.read_sql('SELECT * FROM gold_customer_metrics ORDER BY lifetime_net_spend DESC LIMIT 5;', engine).to_string(index=False))
"

```

---

## 🛠️ Docker Quick Reference

```bash
# Start background stack (Preserves data)
docker compose up -d

# Stop background stack (Preserves data)
docker compose stop

# Hard Reset (Removes containers and wipes all storage volumes clean)
docker compose down -v

```

---

## 🧪 Testing & Automated CI/CD

The pipeline features unit tests written in `pytest` to validate core transformation logic in memory without live infrastructure dependencies.

### Running Tests Locally

Ensure your virtual environment is active, then run:

```bash
pytest

```

### Code Quality & Formatting

Run automated style and linting checks across all scripts and tests:

```bash
# Auto-format code according to Black standard
black .

# Check PEP 8 compliance and catch syntax/lint errors
flake8

```

### Continuous Integration (GitHub Actions)

Every `push` or `pull_request` to the `main` branch automatically triggers `.github/workflows/ci.yml` to:

* Provision a clean Python runner environment.
* Install pinned pipeline dependencies from `requirements.txt`.
* Perform static lint and formatting checks via `black` and `flake8`.
* Execute the complete `pytest` suite across Silver Data Quality rules and Gold metrics aggregations.

---