import boto3
from botocore.client import Config

# Initialize S3 client for LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

buckets = ["apex-data-lake", "apex-dead-letter-queue"]

for bucket in buckets:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Successfully created bucket: {bucket}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket already exists: {bucket}")
    except Exception as e:
        print(f"Error creating {bucket}: {e}")
