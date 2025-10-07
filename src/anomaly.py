"""Simple anomaly scoring logic."""
from __future__ import annotations

import pandas as pd


def score_transactions(features: pd.DataFrame, threshold: float = 2.5) -> pd.DataFrame:
    """Attach anomaly scores and labels to the feature table."""
    scored = features.copy()
    if "amount_zscore" in scored.columns:
        scored["anomaly_score"] = scored["amount_zscore"].abs()
    else:
        scored["anomaly_score"] = 0.0

    scored["is_anomalous"] = scored["anomaly_score"] >= threshold
    return scored
