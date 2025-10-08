# src/rules.py
from __future__ import annotations
import pandas as pd
from datetime import timedelta
from typing import Dict, List, Any

# ------------------------------
# Helpers
# ------------------------------

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add safe defaults for expected columns if missing."""
    df = df.copy()
    defaults = {
        "kyc_verified": True,
        "pep_flag": False,
        "country_src": None,
        "country_dst": None,
        "currency": None,
        "channel": None,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    # Normalize common fields to reduce case-mismatch bugs
    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str).str.upper()
    if "country_src" in df.columns:
        df["country_src"] = df["country_src"].astype(str).str.upper()
    if "country_dst" in df.columns:
        df["country_dst"] = df["country_dst"].astype(str).str.upper()
    if "channel" in df.columns:
        df["channel"] = df["channel"].astype(str).str.lower()
    return df


def _thr_for_currency(cfg: Dict[str, Any], cur: str) -> float:
    """Pick per-currency threshold if available, else global USD fallback."""
    tpc = cfg.get("thresholds_per_currency", {}) or {}
    global_thr = cfg["thresholds"]["large_txn_usd"]
    return float(tpc.get(cur, global_thr))


def _append_hits(rows: List[dict], df_subset: pd.DataFrame, rule_id: str, severity: float, mk_reason) -> None:
    """Append standardized hits to rows. mk_reason(row) -> str."""
    for r in df_subset.itertuples(index=False):
        rows.append({
            "txn_id": r.txn_id,
            "rule": rule_id,
            "severity": float(severity),
            "reason": mk_reason(r)
        })


# ------------------------------
# Individual rules
# ------------------------------

def rule_large_txn_currency_aware(df: pd.DataFrame, cfg: Dict[str, Any]) -> List[dict]:
    """Flag amounts above per-currency threshold."""
    hits: List[dict] = []
    for cur, g in df.groupby("currency", dropna=False):
        thr = _thr_for_currency(cfg, cur)
        m = g["amount"] > thr
        _append_hits(
            hits,
            g[m],
            "R2_LARGE",
            0.6,
            lambda r, t=thr, c=cur: f"Amount {r.amount} {c} > {t} {c}"
        )
    return hits


def rule_structuring_currency_aware(df: pd.DataFrame, cfg: Dict[str, Any]) -> List[dict]:
    """
    Flag 'structuring' (smurfing): >=N near-threshold txns within rolling window,
    per (customer, currency).
    """
    base = float(cfg["thresholds"]["large_txn_usd"])
    band = float(cfg["thresholds"]["near_threshold_band"])
    min_ev = int(cfg["thresholds"]["structuring_min_events"])
    win_m = int(cfg["thresholds"]["structuring_window_minutes"])

    d = df.copy()
    d["ts"] = pd.to_datetime(d["timestamp"], errors="coerce")

    hits: List[dict] = []
    # Per (customer, currency) profile
    for (cid, cur), g in d.groupby(["customer_id", "currency"], dropna=False):
        thr = _thr_for_currency(cfg, cur)
        lo, hi = thr - band, thr - 1
        gg = g[g["amount"].between(lo, hi, inclusive="both")].sort_values("ts")
        if gg.empty:
            continue
        idx = gg.index.tolist()
        for i in range(len(idx)):
            start = gg.loc[idx[i], "ts"]
            end = start + timedelta(minutes=win_m)
            within = gg[(gg["ts"] >= start) & (gg["ts"] <= end)]
            if len(within) >= min_ev:
                _append_hits(
                    hits,
                    within,
                    "R1_STRUCT",
                    0.9,
                    lambda r, n=len(within), w=win_m, c=cur, t=thr:
                        f"{n} near-threshold txns within {w}m for customer {cid} in {c} (≈thr {t})"
                )
    return hits


def rule_risky_corridor(df: pd.DataFrame, cfg: Dict[str, Any]) -> List[dict]:
    """Flag if either source or destination country is in high-risk list."""
    hr = set(cfg.get("high_risk_countries", []))
    if not hr:
        return []
    m = df["country_src"].isin(hr) | df["country_dst"].isin(hr)
    hits: List[dict] = []
    _append_hits(
        hits,
        df[m],
        "R3_RISKY_COUNTRY",
        0.5,
        lambda r: f"High-risk corridor {r.country_src}->{r.country_dst}"
    )
    return hits


def rule_cross_border_cash(df: pd.DataFrame) -> List[dict]:
    """Flag cross-border cash transactions."""
    m = (df["channel"] == "cash") & (df["country_src"] != df["country_dst"])
    hits: List[dict] = []
    _append_hits(
        hits,
        df[m],
        "R6_CASH_XBORDER",
        0.6,
        lambda r: f"Cross-border cash {r.country_src}->{r.country_dst}"
    )
    return hits


def rule_kyc_required(df: pd.DataFrame) -> List[dict]:
    """Flag missing/unverified KYC."""
    m = df["kyc_verified"].fillna(False) == False
    hits: List[dict] = []
    _append_hits(
        hits,
        df[m],
        "R4_KYC",
        0.7,
        lambda r: "Missing/unverified KYC"
    )
    return hits


def rule_pep(df: pd.DataFrame, cfg: Dict[str, Any]) -> List[dict]:
    """Flag PEP transactions above PEP threshold."""
    pep_thr = float(cfg.get("thresholds", {}).get("pep_txn_usd", 5000))
    m = (df["pep_flag"].fillna(False) == True) & (df["amount"] > pep_thr)
    hits: List[dict] = []
    _append_hits(
        hits,
        df[m],
        "R5_PEP",
        0.8,
        lambda r, t=pep_thr: f"PEP transaction over threshold ({r.amount} > {t})"
    )
    return hits


# ------------------------------
# Orchestrator
# ------------------------------

def run_rules(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply all enabled rules and return a standardized DataFrame:
    columns = ["txn_id", "rule", "severity", "reason"].
    """
    df = _ensure_columns(df)
    rows: List[dict] = []

    # Always-on rules
    rows += rule_large_txn_currency_aware(df, cfg)
    rows += rule_structuring_currency_aware(df, cfg)
    rows += rule_risky_corridor(df, cfg)
    rows += rule_cross_border_cash(df)

    # Config-toggled rules
    if cfg.get("kyc_required", False):
        rows += rule_kyc_required(df)

    if cfg.get("pep_watchlist", False):
        rows += rule_pep(df, cfg)

    if not rows:
        return pd.DataFrame(columns=["txn_id", "rule", "severity", "reason"])
    return pd.DataFrame(rows, columns=["txn_id", "rule", "severity", "reason"])


# ------------------------------
# Compatibility facade
# ------------------------------

_DEFAULT_THRESHOLDS: Dict[str, float] = {
    "large_txn_usd": 10000.0,
    "near_threshold_band": 500.0,
    "structuring_min_events": 3,
    "structuring_window_minutes": 60,
    "pep_txn_usd": 5000.0,
}
_RESULT_COLUMNS = ["txn_id", "rule_id", "rule_description", "matched_value", "severity", "reason"]


def _finalise_result(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with consistent columns for downstream consumers."""
    if df.empty:
        return pd.DataFrame(columns=_RESULT_COLUMNS)
    framed = df.copy()
    for col in _RESULT_COLUMNS:
        if col not in framed.columns:
            framed[col] = None
    return framed.loc[:, _RESULT_COLUMNS]


def _normalize_modern_config(cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    """Ensure required keys exist for the modern rules engine."""
    cfg = cfg or {}
    normalized: Dict[str, Any] = dict(cfg)
    thresholds = dict(_DEFAULT_THRESHOLDS)
    thresholds.update(normalized.get("thresholds", {}))
    normalized["thresholds"] = thresholds
    normalized.setdefault("thresholds_per_currency", {})
    normalized.setdefault("high_risk_countries", [])
    normalized.setdefault("kyc_required", False)
    normalized.setdefault("pep_watchlist", False)
    return normalized


def _safe_series(df: pd.DataFrame, field: str) -> pd.Series:
    """Return dataframe column or a None-filled series if missing."""
    if field in df.columns:
        return df[field]
    return pd.Series([None] * len(df), index=df.index)


def _legacy_condition_mask(series: pd.Series, operator: str, value: Any) -> pd.Series:
    """Evaluate a simple rule condition and return a boolean mask."""
    op = (operator or "").lower()
    if op in {"greater_than", "greater_than_or_equal"}:
        numbers = pd.to_numeric(series, errors="coerce")
        if value is None:
            return numbers > float("inf")  # Always False
        comparator = float(value)
        mask = numbers > comparator if op == "greater_than" else numbers >= comparator
    elif op in {"less_than", "less_than_or_equal"}:
        numbers = pd.to_numeric(series, errors="coerce")
        if value is None:
            return numbers < float("-inf")  # Always False
        comparator = float(value)
        mask = numbers < comparator if op == "less_than" else numbers <= comparator
    elif op in {"equals", "equal"}:
        mask = series == value
    elif op in {"not_equals", "not_equal"}:
        mask = series != value
    elif op == "in":
        candidates = value if isinstance(value, (list, tuple, set)) else [value]
        mask = series.isin(list(candidates))
    elif op in {"not_in", "nin"}:
        candidates = value if isinstance(value, (list, tuple, set)) else [value]
        mask = ~series.isin(list(candidates))
    elif op == "contains":
        mask = series.astype(str).str.contains(str(value), case=False, na=False)
    elif op == "starts_with":
        mask = series.astype(str).str.startswith(str(value), na=False)
    elif op == "ends_with":
        mask = series.astype(str).str.endswith(str(value), na=False)
    else:
        raise ValueError(f"Unsupported operator '{operator}' in legacy rule.")
    return mask.fillna(False)


def _apply_legacy_rule(df: pd.DataFrame, rule: Dict[str, Any]) -> pd.DataFrame:
    """Apply a single legacy rule definition and return matches."""
    missing_keys = {"id", "field", "operator"} - set(rule)
    if missing_keys:
        raise ValueError(f"Legacy rule missing required keys: {missing_keys}")
    if "txn_id" not in df.columns:
        raise ValueError("Transactions dataframe requires a 'txn_id' column.")

    series = _safe_series(df, rule["field"])
    mask = _legacy_condition_mask(series, rule["operator"], rule.get("value"))
    matches = df.loc[mask].copy()
    if matches.empty:
        return pd.DataFrame(columns=_RESULT_COLUMNS)

    result = matches[["txn_id"]].copy()
    if rule["field"] in matches.columns:
        result["matched_value"] = matches[rule["field"]]
    else:
        result["matched_value"] = None

    result["rule_id"] = rule.get("id")
    result["rule_description"] = rule.get("description")
    return _finalise_result(result)


def _apply_legacy_rules(df: pd.DataFrame, rules_cfg: Dict[str, Any]) -> pd.DataFrame:
    """Evaluate legacy YAML rules format (list under 'rules')."""
    rules_list = rules_cfg.get("rules", [])
    if not isinstance(rules_list, list):
        raise ValueError("Legacy rules configuration must contain a list under 'rules'.")
    if not rules_list:
        return pd.DataFrame(columns=_RESULT_COLUMNS)

    frames = [_apply_legacy_rule(df, rule) for rule in rules_list]
    if not frames:
        return pd.DataFrame(columns=_RESULT_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    return _finalise_result(combined)


def apply_rules(df: pd.DataFrame, cfg: Dict[str, Any] | None) -> pd.DataFrame:
    """
    Backwards-compatible entry point expected by the rest of the pipeline.

    - Legacy configs (list of rules under 'rules') use simple column comparisons.
    - Modern configs rely on the advanced rule engine implemented in `run_rules`.
    """
    cfg = cfg or {}
    if "rules" in cfg:
        return _apply_legacy_rules(df, cfg)

    modern_cfg = _normalize_modern_config(cfg)
    modern_hits = run_rules(df, modern_cfg)
    if modern_hits.empty:
        return pd.DataFrame(columns=_RESULT_COLUMNS)

    modern_hits = modern_hits.rename(columns={"rule": "rule_id"})
    modern_hits["rule_description"] = None
    modern_hits["matched_value"] = None
    return _finalise_result(modern_hits)
