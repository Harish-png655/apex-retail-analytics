import pandas as pd

from scripts.etl.silver_transformation import validate_and_clean_orders


def test_silver_dq_valid_records():
    raw_df = pd.DataFrame(
        [
            {
                "order_id": "ORD-1001",
                "customer_id": "CUST-001",
                "amount_usd": 150.50,
                "seller_fee": 15.00,
                "transaction_date": "2026-03-01 10:00:00",
                "region": "NORTH",
            }
        ]
    )

    clean_df, dlq_df = validate_and_clean_orders(raw_df)

    assert len(clean_df) == 1
    assert len(dlq_df) == 0
    assert clean_df.iloc[0]["order_id"] == "ORD-1001"


def test_silver_dq_null_id_routing_to_dlq():
    raw_df = pd.DataFrame(
        [
            {
                "order_id": None,
                "customer_id": "CUST-002",
                "amount_usd": 200.00,
                "seller_fee": 20.00,
                "transaction_date": "2026-03-01 11:00:00",
                "region": "SOUTH",
            }
        ]
    )

    clean_df, dlq_df = validate_and_clean_orders(raw_df)

    assert len(clean_df) == 0
    assert len(dlq_df) == 1
    assert dlq_df.iloc[0]["_quarantine_reason"] == "Failed Schema/DQ rules"


def test_silver_dq_negative_amount_routing_to_dlq():
    raw_df = pd.DataFrame(
        [
            {
                "order_id": "ORD-1002",
                "customer_id": "CUST-003",
                "amount_usd": -50.00,
                "seller_fee": 5.00,
                "transaction_date": "2026-03-01 12:00:00",
                "region": "EAST",
            }
        ]
    )

    clean_df, dlq_df = validate_and_clean_orders(raw_df)

    assert len(clean_df) == 0
    assert len(dlq_df) == 1
    assert dlq_df.iloc[0]["_quarantine_reason"] == "Failed Schema/DQ rules"
