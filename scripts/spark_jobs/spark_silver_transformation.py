import os
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
HADOOP_DIR = os.path.join(PROJECT_ROOT, ".hadoop")
if os.path.exists(HADOOP_DIR):
    os.environ["HADOOP_HOME"] = HADOOP_DIR


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def promote_spark_part_to_single_file(bucket, temp_folder_key, target_file_key):
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=temp_folder_key)

    part_key = None
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".parquet") and "part-" in obj["Key"]:
                part_key = obj["Key"]
                break

    if part_key:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": part_key},
            Key=target_file_key,
        )
        for obj in response["Contents"]:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])


def get_spark_session():
    packages = (
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    storage_dir = os.path.join(PROJECT_ROOT, "data", "storage")

    return (
        SparkSession.builder.appName("ApexSparkSilverTransformation")
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
        .config("spark.hadoop.fs.s3a.buffer.dir", storage_dir)
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .master("local[*]")
        .getOrCreate()
    )


def transform_spark_silver_orders(spark):
    print("--- Spark: Processing Orders (Bronze -> Silver) ---")
    bronze_path = "s3a://apex-data-lake/bronze/orders_raw/orders_raw_bronze.parquet"

    df = spark.read.parquet(bronze_path)

    df = (
        df.withColumn("amount_usd", F.col("amount_usd").cast(DoubleType()))
        .withColumn("seller_fee", F.col("seller_fee").cast(DoubleType()))
        .withColumn("transaction_date", F.col("transaction_date").cast(TimestampType()))
    )

    valid_cond = (
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & (F.col("amount_usd") > 0)
        & (F.col("seller_fee") >= 0)
        & F.col("transaction_date").isNotNull()
    )

    df_clean = (
        df.filter(valid_cond)
        .dropDuplicates(["order_id"])
        .withColumn("_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )

    df_quarantine = df.filter(~valid_cond).withColumn(
        "_quarantine_reason", F.lit("Failed Schema/DQ rules")
    )

    temp_silver_target = "s3a://apex-data-lake/silver/orders_temp/"
    df_clean.coalesce(1).write.mode("overwrite").parquet(temp_silver_target)

    promote_spark_part_to_single_file(
        "apex-data-lake",
        "silver/orders_temp/",
        "silver/orders/orders_clean.parquet",
    )
    print("Clean Orders saved via Spark -> silver/orders/orders_clean.parquet")

    if df_quarantine.count() > 0:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dlq_temp = f"s3a://apex-dead-letter-queue/silver_quarantine_temp_{ts}/"
        df_quarantine.coalesce(1).write.mode("overwrite").parquet(dlq_temp)
        promote_spark_part_to_single_file(
            "apex-dead-letter-queue",
            f"silver_quarantine_temp_{ts}/",
            f"silver_quarantine/orders_quarantine_{ts}.parquet",
        )
        print(
            "Quarantined Orders saved via Spark -> "
            f"silver_quarantine/orders_quarantine_{ts}.parquet"
        )


def transform_spark_silver_returns(spark):
    print("--- Spark: Processing Returns (Bronze -> Silver) ---")
    bronze_path = "s3a://apex-data-lake/bronze/returns_raw/returns_raw_bronze.parquet"

    df = spark.read.parquet(bronze_path)

    cols = df.columns
    return_id_col = "return_id" if "return_id" in cols else cols[0]
    order_id_col = "order_id" if "order_id" in cols else cols[1]

    if "processed_timestamp" in cols:
        df = df.withColumn(
            "processed_timestamp",
            F.col("processed_timestamp").cast(TimestampType()),
        )
    elif "return_date" in cols:
        df = df.withColumn(
            "processed_timestamp", F.col("return_date").cast(TimestampType())
        )

    valid_cond = F.col(return_id_col).isNotNull() & F.col(order_id_col).isNotNull()

    df_clean = (
        df.filter(valid_cond)
        .dropDuplicates([return_id_col])
        .withColumn("_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )

    temp_returns_target = "s3a://apex-data-lake/silver/returns_temp/"
    df_clean.coalesce(1).write.mode("overwrite").parquet(temp_returns_target)
    promote_spark_part_to_single_file(
        "apex-data-lake",
        "silver/returns_temp/",
        "silver/returns/returns_clean.parquet",
    )
    print("Clean Returns saved via Spark -> silver/returns/returns_clean.parquet")


def run_spark_silver_transformation():
    spark = get_spark_session()
    transform_spark_silver_orders(spark)
    transform_spark_silver_returns(spark)
    spark.stop()
    print("\nPhase 5 PySpark Silver Layer Transformation Completed Successfully!")


if __name__ == "__main__":
    run_spark_silver_transformation()
