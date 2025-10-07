"""Reporting utilities for the AML pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from . import learn


def _coerce_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (np.generic,)):
        return value.item()
    return value


def _to_python_records(frame: pd.DataFrame, columns: List[str]) -> List[Dict[str, Any]]:
    if frame.empty:
        return []
    trimmed = frame.loc[:, [col for col in columns if col in frame.columns]]
    records = []
    for record in trimmed.to_dict(orient="records"):
        records.append({key: _coerce_value(val) for key, val in record.items()})
    return records


def generate_report(
    enriched: pd.DataFrame,
    rule_matches: pd.DataFrame,
    feedback_path: Path | str,
    top_n: int = 5,
) -> Dict[str, Any]:
    """Create a serialisable summary structure for downstream consumers."""
    summary: Dict[str, Any] = {
        "total_transactions": int(len(enriched)),
        "rule_alerts": int(0 if rule_matches.empty else len(rule_matches)),
    }

    if "anomaly_score" in enriched.columns:
        summary["max_anomaly_score"] = float(enriched["anomaly_score"].max())
        if "is_anomalous" in enriched.columns:
            summary["anomalies_over_threshold"] = int(enriched["is_anomalous"].sum())
        else:
            summary["anomalies_over_threshold"] = 0
        top_anomalies = enriched.nlargest(top_n, "anomaly_score")
        summary["top_anomalies"] = _to_python_records(
            top_anomalies, ["txn_id", "amount", "anomaly_score", "rule_alert"]
        )
    else:
        summary["max_anomaly_score"] = 0.0
        summary["anomalies_over_threshold"] = 0
        summary["top_anomalies"] = []

    if "rule_alert" in enriched.columns:
        alerted = enriched.loc[enriched["rule_alert"], ["txn_id", "amount", "country"]]
    else:
        if {"txn_id", "amount", "country"}.issubset(enriched.columns):
            alerted = enriched.iloc[0:0][["txn_id", "amount", "country"]]
        else:
            alerted = pd.DataFrame(columns=["txn_id", "amount", "country"])
    summary["alerted_transactions"] = _to_python_records(alerted, ["txn_id", "amount", "country"])

    feedback_summary = learn.load_feedback_summary(feedback_path)
    if feedback_summary:
        summary["feedback_summary"] = feedback_summary

    return summary
