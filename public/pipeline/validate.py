from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


KNOWN_CURRENCIES = {"USD", "EUR", "GBP", "NGN", "KES", "JPY"}

PRODUCT_MIN_PRICE = {
    "MacBook Pro M3": 1000.0,
    "iPhone 15 Pro": 400.0,
    "Samsung S23 Ultra": 400.0,
    "Enterprise Server": 5000.0,
    "Network Switch": 1000.0,
    "Rack Cabinet": 400.0,
    "Herman Miller Chair": 500.0,
    "Electric Standing Desk": 300.0,
    "Conference Table": 800.0,
}

def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Validation stage with Strict Financial Audit.
    Returns (passed_df, rejected_df, report_dict).
    """
    out = df.copy()
    out["validation_issues"] = ""
    out["severity"] = "OK"

    def add_issue(mask: pd.Series, code: str, severity: str) -> None:
        nonlocal out
        if mask is None or mask.sum() == 0:
            return
        # Append issue codes
        current = out.loc[mask, "validation_issues"].astype("string")
        out.loc[mask, "validation_issues"] = current.where(current == "", current + "; ") + code
        # Upgrade severity if higher
        rank = {"OK": 0, "SUSPICIOUS": 1, "CRITICAL": 2}
        for idx in out[mask].index:
            if rank[severity] > rank[out.at[idx, "severity"]]:
                out.at[idx, "severity"] = severity

    # 1. Financial Realism & Business Plausibility
    add_issue(out["amount_usd"] <= 0, "INVALID_REVENUE_AMOUNT", "CRITICAL")
    
    if "margin_pct" in out.columns:
        add_issue(out["margin_pct"] < -50, "SUSPICIOUS_LOSS_MARGIN", "SUSPICIOUS")
        add_issue(out["margin_pct"] > 85, "UNREALISTIC_PROFIT_MARGIN", "SUSPICIOUS")
        add_issue((out["margin_pct"] < -100) | (out["margin_pct"] > 100), "MARGIN_OUT_OF_RANGE", "CRITICAL")

    if "product" in out.columns:
        for product, min_val in PRODUCT_MIN_PRICE.items():
            mask = (out["product"] == product) & (out["amount_usd"] < min_val)
            add_issue(mask, f"UNIT_PRICE_LOW_{_to_code(product)}", "SUSPICIOUS")

    add_issue(out["amount_usd"] > 50000, "EXCESSIVE_TRANSACTION_VALUE", "SUSPICIOUS")
    add_issue(~out["currency"].isin(KNOWN_CURRENCIES), "UNKNOWN_CURRENCY", "CRITICAL")

    # 2. Split Logic
    is_failed = out["severity"] != "OK"
    rejected = out[is_failed].copy()
    rejected["failure_reason"] = rejected["validation_issues"]
    
    passed = out[~is_failed].copy()
    passed = passed.drop(columns=["validation_issues", "severity"])

    # Final counts
    total_rows = int(len(out))
    valid_rows = int(len(passed))
    
    report = {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "rejected_rows": total_rows - valid_rows,
        "quality_score": round((valid_rows / total_rows) * 100.0, 2) if total_rows else 0.0,
        "audit_timestamp": pd.Timestamp.now().isoformat()
    }
    return passed, rejected, report

def _to_code(s: str) -> str:
    return s.upper().replace(" ", "_").replace(".", "")

def write_report(report: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
