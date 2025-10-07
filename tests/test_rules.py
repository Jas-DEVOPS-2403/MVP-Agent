"""Unit tests for rule evaluation logic."""
import pandas as pd

from src.rules import apply_rules


def test_apply_rules_flags_high_value_transaction() -> None:
    transactions = pd.DataFrame(
        [
            {"txn_id": "TXN001", "amount": 5000, "country": "US"},
            {"txn_id": "TXN002", "amount": 20000, "country": "RU"},
        ]
    )
    rules_config = {
        "rules": [
            {
                "id": "high_amount",
                "description": "High value transaction",
                "field": "amount",
                "operator": "greater_than",
                "value": 10000,
            }
        ]
    }

    matches = apply_rules(transactions, rules_config)

    assert not matches.empty
    assert matches.loc[0, "txn_id"] == "TXN002"
    assert matches.loc[0, "rule_id"] == "high_amount"
