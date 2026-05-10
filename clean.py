from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


_NON_ALNUM_RE = re.compile(r"[^0-9A-Za-z]+")


def _to_snake_case(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = _NON_ALNUM_RE.sub("_", name)
    name = name.strip("_").lower()
    return name


def _clean_numeric_series(s: pd.Series) -> pd.Series:
    """Extract a numeric value from messy strings."""
    if s is None:
        return s
    raw = s.astype("string")
    is_paren_negative = raw.str.contains(r"^\s*\(.*\)\s*$", regex=True, na=False)
    raw = raw.str.replace(r"[\(\)]", "", regex=True)
    raw = raw.str.replace(r"[^0-9,.\-]+", "", regex=True)
    has_dot = raw.str.contains(r"\.", regex=True, na=False)
    has_comma = raw.str.contains(r",", regex=True, na=False)
    both = has_dot & has_comma
    raw = raw.mask(both, raw.str.replace(",", "", regex=False))
    raw = raw.mask(~has_dot & has_comma, raw.str.replace(",", ".", regex=False))
    num = pd.to_numeric(raw, errors="coerce")
    num = num.where(~is_paren_negative, -num)
    return num.astype("float64")


def _normalize_category(s: pd.Series, mode: str) -> pd.Series:
    s = s.astype("string").str.strip()
    if mode == "upper":
        return s.str.upper()
    if mode == "lower":
        return s.str.lower()
    if mode == "title":
        return s.str.title()
    return s


def _normalize_region(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    s = s.str.replace(r"\s+", " ", regex=True).str.upper()
    return s.replace(
        {
            "N AMERICA": "NORTH AMERICA",
            "N. AMERICA": "NORTH AMERICA",
            "NORTHAMERICA": "NORTH AMERICA",
            "ASIA PACIFIC": "APAC",
        }
    )


@dataclass(frozen=True)
class CustomerNameGenerator:
    """Deterministically assign synthetic names based on customer ID hash."""
    seed: int = 20260503

    def generate_mapping(self, customer_ids: Iterable[str]) -> dict[str, str]:
        first_names = ["James", "Aisha", "Michael", "Fatima", "David", "Sofia", "Wei", "Carlos", "Amara", "Noah", "Zara", "Hassan", "Elena", "Mateo", "Priya", "Omar", "Nia", "Daniel", "Yara", "Samuel", "Linh", "Amina", "Diego", "Grace", "Kenji", "Leila", "Ethan", "Mina", "Tariq", "Chloe", "Ibrahim", "Hana", "Lucas", "Maya", "Zain", "Isabella", "Aditya", "Nadia", "Kwame", "Ava", "Emeka", "Yuki", "Ana", "Ahmed", "Mei", "Oliver", "Sara", "Amir", "Victoria", "Ravi", "Naomi", "Malik", "Julia", "Arjun", "Hyejin", "Thomas", "Halima", "George", "Rosa", "Santiago"]
        last_names = ["Carter", "Bello", "Johnson", "Khan", "Smith", "Garcia", "Nguyen", "Patel", "Okafor", "Hernandez", "Kim", "Silva", "Brown", "Ahmed", "Wang", "Davis", "Martinez", "Singh", "Lopez", "Taylor", "Ibrahim", "Clark", "Morgan", "Chen", "Rossi", "Sato", "Kowalski", "Novak", "Mensah", "Dubois", "Andersson", "Nakamura", "Ivanov", "Haddad", "Hussain", "Jensen", "O'Connor", "Campbell", "Ali", "Rahman", "Gonzalez", "Fernandez", "Adebayo", "Ndlovu", "Okeke", "Chaudhry", "Sharma", "Banerjee", "Das", "Choi", "Park", "Lee", "Yamamoto", "Tanaka", "Suzuki", "Khanh", "Hoang", "Tran", "Bianchi", "Conti"]
        
        combos = [f"{f} {l}" for f in first_names for l in last_names]
        mapping: dict[str, str] = {}
        
        for cid in sorted({str(x) for x in customer_ids if pd.notna(x)}):
            h = hashlib.sha256(f"{self.seed}-{cid}".encode()).hexdigest()
            idx = int(h, 16) % len(combos)
            mapping[cid] = combos[idx]
        return mapping


def clean_data(df: pd.DataFrame, *, seed: int = 20260503) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    STRICT CLEANING + AUDIT STAGE.
    Returns (cleaned_df, rejected_df).
    """
    raw_snapshot = df.copy()
    out = df.copy()
    
    # 1) Column Normalization
    out.columns = [_to_snake_case(c) for c in out.columns]
    
    # 2) Numeric Extraction
    if "amount" in out.columns:
        out["amount"] = _clean_numeric_series(out["amount"])
    if "discount_rate" in out.columns:
        out["discount_rate"] = _clean_numeric_series(out["discount_rate"])

    # 3) Date Parsing (Strict)
    if "order_date" in out.columns:
        out["order_date"] = pd.to_datetime(out["order_date"], errors="coerce", format="mixed")

    # 4) Categorical Standardization
    if "currency" in out.columns:
        out["currency"] = _normalize_category(out["currency"], "upper")
    if "region" in out.columns:
        out["region"] = _normalize_region(out["region"])
    if "country" in out.columns:
        out["country"] = _normalize_category(out["country"], "title")
    if "order_status" in out.columns:
        out["order_status"] = _normalize_category(out["order_status"], "upper").replace({
            "COMPLETE": "COMPLETED", "DONE": "COMPLETED", "CANCELLED": "CANCELED"
        })

    # 5) Rejection Logic (Critical)
    critical_cols = ["order_id", "amount", "order_date", "currency"]
    missing_crit = out[critical_cols].isna().any(axis=1)
    
    # Identify Rejections
    rejected = raw_snapshot[missing_crit].copy()
    rejected["severity"] = "CRITICAL"
    
    reasons = []
    for idx, row in out[missing_crit].iterrows():
        nulls = [c.upper() for c in critical_cols if pd.isna(row[c])]
        reasons.append(f"UNPARSEABLE_OR_MISSING_{'_'.join(nulls)}")
    rejected["failure_reason"] = reasons
    
    out = out[~missing_crit].reset_index(drop=True)

    # 6) Deduplication (Severity: WARNING)
    if "order_id" in out.columns:
        dups_mask = out["order_id"].duplicated(keep=False)
        if dups_mask.any():
            # keep row with most info
            out["_temp_score"] = out.notna().sum(axis=1)
            out = out.sort_values(["order_id", "_temp_score"], ascending=[True, False])
            
            dropped_mask = out["order_id"].duplicated(keep="first")
            dropped_rows = out[dropped_mask].copy()
            
            # Map back to raw for logging
            dropped_raw = raw_snapshot.loc[dropped_rows.index].copy()
            dropped_raw["severity"] = "WARNING"
            dropped_raw["failure_reason"] = "REDUNDANT_DUPLICATE_ORDER_ID"
            
            rejected = pd.concat([rejected, dropped_raw])
            out = out[~dropped_mask].reset_index(drop=True).drop(columns=["_temp_score"])

    # 7) Synthetic Names
    if "customer" in out.columns:
        gen = CustomerNameGenerator(seed=seed)
        mapping = gen.generate_mapping(out["customer"].astype("string").tolist())
        out["customer_name"] = out["customer"].astype("string").map(mapping)
        
        cust_ids = sorted(mapping.keys())
        rng = np.random.default_rng(seed + 99)
        types = rng.choice(["B2B", "B2C"], size=len(cust_ids), p=[0.7, 0.3])
        type_map = dict(zip(cust_ids, types))
        out["customer_type"] = out["customer"].astype("string").map(type_map)

    return out, rejected
