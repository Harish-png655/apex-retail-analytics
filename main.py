import argparse
import subprocess
import sys
import time


def run_step(step_name, script_path):
    print("\n==========================================")
    print(f"▶ EXECUTING: {step_name}")
    print("==========================================")
    start_time = time.time()
    result = subprocess.run([sys.executable, script_path], check=False)
    elapsed = round(time.time() - start_time, 2)

    if result.returncode != 0:
        print(f"❌ FAILED: {step_name} (Exit Code: {result.returncode})")
        sys.exit(result.returncode)
    else:
        print(f"✔ SUCCESS: {step_name} ({elapsed}s)")


def main():
    parser = argparse.ArgumentParser(
        description="Apex Retail Data Lakehouse ETL Pipeline"
    )
    parser.add_argument(
        "--engine",
        choices=["default", "pyspark"],
        default="default",
        help="Select Silver processing engine (pandas | pyspark)",
    )
    args = parser.parse_args()

    pipeline_start = time.time()
    print("🚀 Starting Apex Retail Data Lakehouse ETL Pipeline")
    print(f"⚙ Processing Engine for Silver Tier: {args.engine.upper()}")

    run_step("1. S3 Infrastructure Initialization", "scripts/init_minio.py")
    run_step("2. Raw Mock Data Upload", "scripts/data_gen/upload_to_s3.py")
    run_step(
        "3. Bronze Layer Ingestion & Audit Metadata",
        "scripts/etl/bronze_ingestion.py",
    )

    if args.engine == "pyspark":
        run_step(
            "4. Silver Layer DQ Validation & Quarantine (PySpark DAG)",
            "scripts/spark_jobs/spark_silver_transformation.py",
        )
    else:
        run_step(
            "4. Silver Layer DQ Validation & Quarantine (Pandas/PyArrow)",
            "scripts/etl/silver_transformation.py",
        )

    run_step(
        "5. Gold Layer Aggregations & PostgreSQL Load",
        "scripts/etl/gold_transformation.py",
    )

    total_time = round(time.time() - pipeline_start, 2)
    print(
        f"\n🎉 Lakehouse Pipeline Executed Successfully in {total_time}s! "
        f"Engine used: {args.engine}"
    )


if __name__ == "__main__":
    main()
