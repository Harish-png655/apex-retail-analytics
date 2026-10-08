import io
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from botocore.client import Config

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def ingest_raw_to_bronze():
    s3 = get_s3_client()
    bucket_name = "apex-data-lake"
    tables = ["orders_raw", "returns_raw"]

    for table in tables:
        raw_key = f"raw/{table}.parquet"
        bronze_key = f"bronze/{table}/{table}_bronze.parquet"

        print(f"Reading raw data from s3://{bucket_name}/{raw_key}...")
        
        # Download raw parquet object into memory
        response = s3.get_object(Bucket=bucket_name, Key=raw_key)
        raw_bytes = response['Body'].read()
        
        # Read into Pandas dataframe via PyArrow
        df = pd.read_parquet(io.BytesIO(raw_bytes))

        # Enforce metadata column enrichment (Lineage & Audit)
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = f"s3://{bucket_name}/{raw_key}"

        print(f"Writing Bronze dataset to s3://{bucket_name}/{bronze_key}...")
        
        # Convert enriched DataFrame back to Parquet bytes
        table_out = pa.Table.from_pandas(df)
        out_buffer = io.BytesIO()
        pq.write_table(table_out, out_buffer)

        # Upload back to LocalStack S3 in bronze layer
        s3.put_object(
            Bucket=bucket_name,
            Key=bronze_key,
            Body=out_buffer.getvalue()
        )
        print(f"Successfully ingested {table} into Bronze layer.")

if __name__ == "__main__":
    ingest_raw_to_bronze()