"""Placeholder learning utilities for feedback-driven updates."""
from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


def load_feedback_summary(feedback_path: Path | str) -> Dict[str, int]:
    """Aggregate analyst feedback by label."""
    path = Path(feedback_path)
    if not path.exists() or path.stat().st_size == 0:
        return {}

    feedback = pd.read_csv(path)
    if feedback.empty or "label" not in feedback.columns:
        return {}

    counts = feedback.groupby("label")["txn_id"].count().to_dict()
    return {str(label): int(count) for label, count in counts.items()}
