import os

import boto3
from botocore.client import Config

# Initialize S3 client for LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

bucket_name = "apex-data-lake"
data_dir = "data/storage/raw"

if os.path.exists(data_dir):
    uploaded_count = 0
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)
                s3_path = f"raw/{file}"
                s3.upload_file(local_path, bucket_name, s3_path)
                print(f"Uploaded {local_path} -> s3://{bucket_name}/{s3_path}")
                uploaded_count += 1
    if uploaded_count == 0:
        print(f"No .parquet files found in {data_dir}.")
else:
    print(f"Directory {data_dir} not found.")
