"""Shared utility helpers for the AML pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml(path: Path | str) -> Dict[str, Any]:
    """Load a YAML file and return an empty dict if the file is blank."""
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
