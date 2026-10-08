import pandas as pd
import pytest

from scripts.etl.gold_transformation import (
    calculate_gold_customer_metrics,
    calculate_gold_regional_performance,
)


@pytest.fixture
def sample_silver_data():
    orders = pd.DataFrame(
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-A",
                "amount_usd": 100.0,
                "seller_fee": 10.0,
                "transaction_date": "2026-03-01 10:00:00",
                "region": "NORTH",
            },
            {
                "order_id": "ORD-2",
                "customer_id": "CUST-A",
                "amount_usd": 200.0,
                "seller_fee": 20.0,
                "transaction_date": "2026-03-01 11:00:00",
                "region": "NORTH",
            },
            {
                "order_id": "ORD-3",
                "customer_id": "CUST-B",
                "amount_usd": 150.0,
                "seller_fee": 15.0,
                "transaction_date": "2026-03-02 12:00:00",
                "region": "SOUTH",
            },
        ]
    )
    returns = pd.DataFrame(
        [
            {
                "return_id": "RET-1",
                "order_id": "ORD-2",
                "processed_timestamp": "2026-03-02 09:00:00",
            }
        ]
    )
    return orders, returns


def test_gold_regional_performance(sample_silver_data):
    orders, returns = sample_silver_data
    regional_df = calculate_gold_regional_performance(orders, returns)

    north_row = regional_df[regional_df["region"] == "NORTH"].iloc[0]

    assert north_row["gross_sales_usd"] == 300.0
    assert north_row["net_sales_usd"] == 100.0  # ORD-2 ($200) was returned
    assert north_row["total_orders"] == 2
    assert north_row["returned_orders"] == 1
    assert north_row["return_rate_pct"] == 50.0


def test_gold_customer_metrics(sample_silver_data):
    orders, returns = sample_silver_data
    cust_df = calculate_gold_customer_metrics(orders, returns)

    cust_a = cust_df[cust_df["customer_id"] == "CUST-A"].iloc[0]

    assert cust_a["lifetime_gross_spend"] == 300.0
    assert cust_a["lifetime_net_spend"] == 100.0
    assert cust_a["total_orders_placed"] == 2
    assert cust_a["total_returns"] == 1
