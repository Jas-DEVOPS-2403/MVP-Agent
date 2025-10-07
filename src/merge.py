"""Combine rule and anomaly outputs."""
from __future__ import annotations

from typing import Set

import pandas as pd


def merge_results(transactions: pd.DataFrame, rule_matches: pd.DataFrame) -> pd.DataFrame:
    """Combine raw transactions with rule match results."""
    merged = transactions.copy()
    if "txn_id" not in merged.columns:
        raise ValueError("Transactions dataframe requires a 'txn_id' column.")

    flagged_ids: Set[str] = set()
    if isinstance(rule_matches, pd.DataFrame) and not rule_matches.empty:
        flagged_ids = set(rule_matches["txn_id"].astype(str).tolist())

    merged["rule_alert"] = merged["txn_id"].astype(str).isin(flagged_ids)
    return merged
