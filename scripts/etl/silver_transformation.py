import io
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def validate_and_clean_orders(df: pd.DataFrame):
    """Pure transformation function for unit testing."""
    df = df.copy()

    # Type Casting
    df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce")
    df["seller_fee"] = pd.to_numeric(df["seller_fee"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Data Quality Validation Rules
    valid_mask = (
        df["order_id"].notna()
        & df["customer_id"].notna()
        & (df["amount_usd"] > 0)
        & (df["seller_fee"] >= 0)
        & df["transaction_date"].notna()
    )

    clean_df = df[valid_mask].drop_duplicates(subset=["order_id"]).copy()
    quarantine_df = df[~valid_mask].copy()

    clean_df["_processed_at"] = datetime.now(timezone.utc).isoformat()
    if len(quarantine_df) > 0:
        quarantine_df["_quarantine_reason"] = "Failed Schema/DQ rules"

    return clean_df, quarantine_df


def transform_silver_orders(s3, bucket_name, dlq_bucket):
    print("--- Processing Orders (Bronze -> Silver) ---")
    bronze_key = "bronze/orders_raw/orders_raw_bronze.parquet"

    obj = s3.get_object(Bucket=bucket_name, Key=bronze_key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    clean_df, quarantine_df = validate_and_clean_orders(df)

    # Save Clean Records
    silver_key = "silver/orders/orders_clean.parquet"
    out_buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(clean_df), out_buf)
    s3.put_object(Bucket=bucket_name, Key=silver_key, Body=out_buf.getvalue())
    print(
        f"Clean Orders saved: {len(clean_df)} rows -> s3://{bucket_name}/{silver_key}"
    )

    # Save Quarantined Records
    if len(quarantine_df) > 0:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dlq_key = f"silver_quarantine/orders_quarantine_{ts}.parquet"
        dlq_buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(quarantine_df), dlq_buf)
        s3.put_object(Bucket=dlq_bucket, Key=dlq_key, Body=dlq_buf.getvalue())
        print(
            f"Quarantined Orders: {len(quarantine_df)} rows -> "
            f"s3://{dlq_bucket}/{dlq_key}"
        )


def transform_silver_returns(s3, bucket_name, dlq_bucket):
    print("--- Processing Returns (Bronze -> Silver) ---")
    bronze_key = "bronze/returns_raw/returns_raw_bronze.parquet"

    obj = s3.get_object(Bucket=bucket_name, Key=bronze_key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    if "processed_timestamp" in df.columns:
        df["processed_timestamp"] = pd.to_datetime(
            df["processed_timestamp"], errors="coerce"
        )
    elif "return_date" in df.columns:
        df["processed_timestamp"] = pd.to_datetime(df["return_date"], errors="coerce")

    return_id_col = "return_id" if "return_id" in df.columns else df.columns[0]
    order_id_col = "order_id" if "order_id" in df.columns else df.columns[1]

    valid_mask = df[return_id_col].notna() & df[order_id_col].notna()
    clean_df = df[valid_mask].drop_duplicates(subset=[return_id_col]).copy()

    clean_df["_processed_at"] = datetime.now(timezone.utc).isoformat()

    silver_key = "silver/returns/returns_clean.parquet"
    out_buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(clean_df), out_buf)
    s3.put_object(Bucket=bucket_name, Key=silver_key, Body=out_buf.getvalue())
    print(
        f"Clean Returns saved: {len(clean_df)} rows -> s3://{bucket_name}/{silver_key}"
    )


def run_silver_transformation():
    s3 = get_s3_client()
    transform_silver_orders(s3, "apex-data-lake", "apex-dead-letter-queue")
    transform_silver_returns(s3, "apex-data-lake", "apex-dead-letter-queue")
    print("\nPhase 5 Silver Layer Transformation Completed Successfully!")


if __name__ == "__main__":
    run_silver_transformation()
