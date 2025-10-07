"""Rule-evaluation utilities."""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List

import pandas as pd

RuleConfig = Dict[str, Any]


def _ensure_iterable(value: Any) -> Iterable[Any]:
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


_OPERATORS: Dict[str, Callable[[pd.Series, Any], pd.Series]] = {
    "greater_than": lambda series, threshold: series.astype(float) > float(threshold),
    "less_than": lambda series, threshold: series.astype(float) < float(threshold),
    "equal": lambda series, target: series == target,
    "in": lambda series, values: series.isin(_ensure_iterable(values)),
}


def apply_rules(transactions: pd.DataFrame, config: RuleConfig) -> pd.DataFrame:
    """Return a dataframe listing the transactions that triggered rules."""
    results: List[Dict[str, Any]] = []
    for rule in config.get("rules", []):
        field = rule.get("field")
        operator = rule.get("operator")
        value = rule.get("value")

        if not field or field not in transactions:
            continue
        if operator not in _OPERATORS:
            continue

        try:
            mask = _OPERATORS[operator](transactions[field], value)
        except Exception:
            continue

        matched = transactions.loc[mask, ["txn_id"]]
        for txn_id in matched["txn_id"].tolist():
            results.append(
                {
                    "txn_id": txn_id,
                    "rule_id": rule.get("id", ""),
                    "description": rule.get("description", ""),
                }
            )

    if not results:
        return pd.DataFrame(columns=["txn_id", "rule_id", "description"])

    return pd.DataFrame(results)
