"""CLI entry point that wires the AML pipeline together."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from src import anomaly, features, ingest, merge, report, rules, utils

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_PATH = BASE_DIR / "data" / "sample.csv"
DEFAULT_RULES_PATH = BASE_DIR / "config" / "rules.yaml"
DEFAULT_FEEDBACK_PATH = BASE_DIR / "data" / "feedback.csv"


def run_pipeline(
    data_path: Path | str = DEFAULT_DATA_PATH,
    rules_path: Path | str = DEFAULT_RULES_PATH,
    feedback_path: Path | str = DEFAULT_FEEDBACK_PATH,
) -> Dict[str, Any]:
    """Execute the AML pipeline on the provided resources."""
    transactions = ingest.load_transactions(data_path)
    rules_config = utils.load_yaml(rules_path)
    rule_matches = rules.apply_rules(transactions, rules_config)

    feature_table = features.build_features(transactions)
    scored_transactions = anomaly.score_transactions(feature_table)
    enriched_transactions = merge.merge_results(scored_transactions, rule_matches)

    return report.generate_report(enriched_transactions, rule_matches, feedback_path)


def main() -> None:
    """Run the pipeline and print a JSON summary."""
    summary = run_pipeline()
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
