from __future__ import annotations

import numpy as np
import pandas as pd


EXCHANGE_RATES = {
    "USD": 1.0,
    "EUR": 1.09,
    "GBP": 1.27,
    "NGN": 0.00065,
    "KES": 0.0075,
    "JPY": 0.0067,
}

# Product cost mapping (USD). Unknown products remain NaN for validation to catch.
COST_MAPPING = {
    "MacBook Pro M3": 1800.0,
    "iPhone 15 Pro": 600.0,
    "Samsung S23 Ultra": 800.0,
    "Enterprise Server": 10000.0,
    "Network Switch": 3000.0,
    "Rack Cabinet": 800.0,
    "Herman Miller Chair": 900.0,
    "Electric Standing Desk": 500.0,
    "Conference Table": 1400.0,
    "Logitech MX Master": 50.0,
    "Sony WH-1000XM5": 250.0,
    "Thunderbolt Dock": 180.0,
}


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformation stage.
    - Vectorized currency conversion -> amount_usd
    - Cost mapping -> cost_usd (NaN if unknown)
    - Completed-only profit/margin (NaN for non-completed)
    - Time features + analytical flags
    """
    out = df.copy()

    # Currency conversion (vectorized)
    if "currency" not in out.columns or "amount" not in out.columns:
        raise ValueError("Missing required columns for transform: currency, amount")

    rates = out["currency"].map(EXCHANGE_RATES).astype("float64")
    out["amount_usd"] = out["amount"].astype("float64") * rates

    # Cost mapping
    if "product" in out.columns:
        out["cost_usd"] = out["product"].map(COST_MAPPING).astype("float64")
    else:
        out["cost_usd"] = np.nan

    # Completed-only business metrics
    out["profit_usd"] = np.nan
    out["margin_pct"] = np.nan

    if "order_status" not in out.columns:
        raise ValueError("Missing required column for transform: order_status")

    completed = out["order_status"].astype("string").str.upper() == "COMPLETED"
    out.loc[completed, "profit_usd"] = out.loc[completed, "amount_usd"] - out.loc[completed, "cost_usd"]
    out.loc[completed, "margin_pct"] = (out.loc[completed, "profit_usd"] / out.loc[completed, "amount_usd"]) * 100.0

    # Time features
    if "order_date" not in out.columns:
        raise ValueError("Missing required column for transform: order_date")
    out["order_date"] = pd.to_datetime(out["order_date"], errors="coerce")
    out["order_year"] = out["order_date"].dt.year.astype("Int64")
    out["order_month"] = out["order_date"].dt.month.astype("Int64")
    out["order_quarter"] = out["order_date"].dt.quarter.astype("Int64")

    # Analytical flags
    out["is_loss_making"] = out["profit_usd"] < 0
    out["is_high_margin"] = out["margin_pct"] > 40
    # Anomaly detection (distinct from loss-making):
    # flag unusually high/low transaction values using an IQR rule on amount_usd.
    q1 = out["amount_usd"].quantile(0.25)
    q3 = out["amount_usd"].quantile(0.75)
    iqr = q3 - q1
    if pd.isna(iqr) or iqr == 0:
        out["is_anomaly"] = False
    else:
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        out["is_anomaly"] = (out["amount_usd"] < lower) | (out["amount_usd"] > upper)

    return out
