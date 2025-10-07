"""Feature engineering routines."""
from __future__ import annotations

import pandas as pd


def build_features(transactions: pd.DataFrame) -> pd.DataFrame:
    """Engineer lightweight features for downstream scoring."""
    features = transactions.copy()

    if "timestamp" in features.columns:
        features["txn_timestamp"] = pd.to_datetime(features["timestamp"], errors="coerce")
        features["txn_hour"] = features["txn_timestamp"].dt.hour
    else:
        features["txn_timestamp"] = pd.NaT
        features["txn_hour"] = pd.NA

    if "amount" in features.columns:
        amount = features["amount"].astype(float)
        std = amount.std(ddof=0)
        if std == 0:
            features["amount_zscore"] = 0.0
        else:
            features["amount_zscore"] = (amount - amount.mean()) / std
    else:
        features["amount_zscore"] = 0.0

    return features
