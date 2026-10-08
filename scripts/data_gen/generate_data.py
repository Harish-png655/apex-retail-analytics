import os
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = "data/storage/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_ORDERS = 100000  # 100k synthetic order events
REGIONS = ["NA_US", "NA_CA", "NA_MX", "EU_UK", "EU_DE"]


def generate_orders():
    print(f"Generating {NUM_ORDERS} order records...")
    orders = []
    base_date = datetime(2026, 1, 1)

    for _ in range(NUM_ORDERS):
        # Introduce deliberate data quality anomalies (~1% rate) to test Fail-Fast logic
        is_corrupt_date = random.random() < 0.01
        is_corrupt_amount = random.random() < 0.01

        order_id = str(uuid.uuid4())
        customer_id = f"CUST_{random.randint(10000, 99999)}"
        tx_date = (
            None
            if is_corrupt_date
            else (base_date + timedelta(days=random.randint(0, 180))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )
        region = random.choice(REGIONS)

        # Corrupt amount: negative or zero value
        amount = (
            round(random.uniform(-50.0, 0.0), 2)
            if is_corrupt_amount
            else round(random.uniform(10.0, 1500.0), 2)
        )
        seller_fee = round(amount * 0.15, 2) if amount > 0 else 0.0

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "transaction_date": tx_date,
                "region": region,
                "amount_usd": amount,
                "seller_fee": seller_fee,
                "ingestion_timestamp": datetime.utcnow().isoformat(),
            }
        )

    df_orders = pd.DataFrame(orders)
    file_path = os.path.join(OUTPUT_DIR, "orders_raw.parquet")
    df_orders.to_parquet(file_path, index=False)
    print(f"Successfully wrote raw orders to {file_path}")
    return df_orders


def generate_returns(df_orders):
    print("Generating return events...")
    # ~8% return rate
    valid_orders = df_orders[df_orders["order_id"].notnull()]["order_id"].tolist()
    returned_orders = random.sample(valid_orders, k=int(len(valid_orders) * 0.08))

    reasons = ["DEFECTIVE", "WRONG_SIZE", "LATE_DELIVERY", "BUYER_REGRET"]
    returns = []

    for oid in returned_orders:
        returns.append(
            {
                "return_id": str(uuid.uuid4()),
                "order_id": oid,
                "return_reason": random.choice(reasons),
                "processed_timestamp": datetime.utcnow().isoformat(),
            }
        )

    df_returns = pd.DataFrame(returns)
    file_path = os.path.join(OUTPUT_DIR, "returns_raw.parquet")
    df_returns.to_parquet(file_path, index=False)
    print(f"Successfully wrote raw returns to {file_path}")


if __name__ == "__main__":
    df_orders = generate_orders()
    generate_returns(df_orders)
    print("Phase 2 Data Generation Completed Successfully!")
