# Apex Retail Analytics: Local Defensive Medallion Lakehouse

An end-to-end, zero-cloud-cost Data Lakehouse architecture implementing **Bronze**, **Silver**, and **Gold** processing tiers. Built using **LocalStack S3**, **PostgreSQL**, **Pandas**, **PyArrow**, and **Boto3** on Windows/Docker Desktop.

This architecture enforces strict schema validation, defensive quality routing to a **Dead-Letter Queue (DLQ)** for non-compliant records, and relational star-schema loading for downstream analytics.

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
│  SILVER LAYER (s3://apex-data-lake/silver/)                           │
│  • Schema standardization & type enforcement                          │
│  • Quality rules (non-null IDs, positive amounts, duplicate removal)  │
│                                   │                                   │
│                 ┌─────────────────┴─────────────────┐                 │
│                 │ (Valid)                           │ (Corrupted)     │
│                 ▼                                   ▼                 │
│  Clean Parquet Datasets               Dead-Letter Queue (DLQ)         │
│  `orders_clean.parquet`               s3://apex-dead-letter-queue/    │
│  `returns_clean.parquet`              quarantine/                     │
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

### 🥈 Silver Tier (Quality Validation & Quarantine)
* **Path:** `s3://apex-data-lake/silver/`
* **Format:** Apache Parquet
* **Logic:** Applies explicit data casting, standardizes lower-case naming conventions, removes duplicates, and filters records against strict rules:
  * `order_id` AND `customer_id` NOT NULL
  * `amount_usd` > 0 AND `seller_fee` >= 0
  * `transaction_date` MUST be valid timestamp
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
├── docker-compose.yml              # LocalStack S3 & PostgreSQL service definitions
├── main.py                         # Single-entrypoint pipeline orchestrator
├── README.md                       # Architecture & execution documentation
├── requirements.txt                # Python environment dependencies
├── scripts/
│   ├── init_minio.py               # LocalStack S3 bucket provisioner
│   ├── data_gen/
│   │   └── upload_to_s3.py         # Mock raw data generator & S3 loader
│   └── etl/
│       ├── bronze_ingestion.py     # Bronze layer metadata enrichment
│       ├── silver_transformation.py# Silver DQ engine & DLQ router
│       └── gold_transformation.py  # Gold metrics compiler & Postgres loader
```

---

## 🚀 Execution Instructions

### 1. Environment Setup

Clone repository and activate a Python 3.10+ virtual environment:

```bash
git clone https://github.com/your-username/apex-retail-analytics.git
cd apex-retail-analytics

python -m venv venv
source venv/Scripts/activate  # On Windows Git Bash

pip install pandas pyarrow boto3 sqlalchemy psycopg
```

### 2. Infrastructure Initialization

Spin up LocalStack (Port 4566) and PostgreSQL (Port 5433):

```bash
docker compose up -d
```

### 3. Pipeline Execution

Run the complete pipeline end-to-end with the orchestrator:

```bash
python main.py
```

---

## 🔍 Database Inspection & Queries

To verify table contents in PostgreSQL:

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

The pipeline features unit tests written in `pytest` to validate core transformation logic in memory without needing live infrastructure dependencies.

### Running Tests Locally
Ensure your virtual environment is active, then run:

```bash
pytest