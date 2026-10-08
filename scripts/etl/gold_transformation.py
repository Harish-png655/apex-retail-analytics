import io
import os
import boto3
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
from botocore.client import Config

# PostgreSQL local container connection
DB_URL = "postgresql+psycopg://apex_admin:apex_password@localhost:5433/apex_warehouse"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def run_gold_transformation():
    print("--- Processing Silver -> Gold Aggregations ---")
    s3 = get_s3_client()
    bucket_name = "apex-data-lake"

    # 1. Fetch Silver Datasets from S3
    orders_obj = s3.get_object(Bucket=bucket_name, Key="silver/orders/orders_clean.parquet")
    returns_obj = s3.get_object(Bucket=bucket_name, Key="silver/returns/returns_clean.parquet")

    df_orders = pd.read_parquet(io.BytesIO(orders_obj['Body'].read()))
    df_returns = pd.read_parquet(io.BytesIO(returns_obj['Body'].read()))

    # 2. Enrich Orders with Return Status
    df_returns["is_returned"] = True
    df_merged = df_orders.merge(
        df_returns[["order_id", "is_returned"]], 
        on="order_id", 
        how="left"
    )
    df_merged["is_returned"] = df_merged["is_returned"].fillna(False)

    # Calculate Net Revenue (excluding returned orders)
    df_merged["net_amount_usd"] = df_merged.apply(
        lambda r: 0.0 if r["is_returned"] else r["amount_usd"], axis=1
    )

    # 3. Aggregation A: Regional Performance Gold Table
    df_merged["date"] = pd.to_datetime(df_merged["transaction_date"]).dt.date
    
    dim_regional = df_merged.groupby(["date", "region"]).agg(
        gross_sales_usd=("amount_usd", "sum"),
        net_sales_usd=("net_amount_usd", "sum"),
        total_seller_fees=("seller_fee", "sum"),
        total_orders=("order_id", "count"),
        returned_orders=("is_returned", "sum")
    ).reset_index()

    dim_regional["return_rate_pct"] = (
        (dim_regional["returned_orders"] / dim_regional["total_orders"]) * 100
    ).round(2)

    # 4. Aggregation B: Customer Metrics Gold Table
    dim_customer = df_merged.groupby("customer_id").agg(
        total_orders_placed=("order_id", "count"),
        lifetime_gross_spend=("amount_usd", "sum"),
        lifetime_net_spend=("net_amount_usd", "sum"),
        total_returns=("is_returned", "sum"),
        last_active_date=("date", "max")
    ).reset_index()

    # 5. Load into PostgreSQL Gold Warehouse
    print("Writing Gold data to PostgreSQL Data Warehouse (`apex_dw`)...")
    engine = create_engine(DB_URL)

    dim_regional.to_sql("gold_regional_performance", engine, if_exists="replace", index=False)
    print(f"Loaded {len(dim_regional)} rows into `gold_regional_performance`.")

    dim_customer.to_sql("gold_customer_metrics", engine, if_exists="replace", index=False)
    print(f"Loaded {len(dim_customer)} rows into `gold_customer_metrics`.")

    print("\nPhase 6 Gold Layer Pipeline Completed Successfully!")

if __name__ == "__main__":
    run_gold_transformation()
