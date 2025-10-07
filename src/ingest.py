"""Data ingestion helpers."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_transactions(csv_path: Path | str) -> pd.DataFrame:
    """Load the transaction ledger from a CSV file."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Transaction file not found: {path}")

    frame = pd.read_csv(path)
    if "txn_id" not in frame.columns:
        raise ValueError("Expected a 'txn_id' column in the transactions file.")

    return frame
