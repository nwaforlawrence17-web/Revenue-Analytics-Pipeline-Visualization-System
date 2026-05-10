from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def generate_validation_report(
    raw_df: pd.DataFrame,
    final_valid_df: pd.DataFrame,
    all_rejections_df: pd.DataFrame,
    output_path: Path,
) -> dict[str, Any]:
    """
    Strict audit report aligning all metrics with the raw dataset.
    """
    raw_total = len(raw_df)
    valid_total = len(final_valid_df)
    rejected_total = len(all_rejections_df)

    # Check for math inconsistency
    checksum_error = False
    if (valid_total + rejected_total) != raw_total:
        checksum_error = True

    # 1. Breakdown of rejection reasons
    # Split semicolon reasons if they exist
    all_reasons = all_rejections_df["failure_reason"].astype("string").str.split("; ").explode()
    reason_counts = all_reasons.value_counts().to_dict()

    # 2. Severity breakdown
    severity_counts = all_rejections_df["severity"].value_counts().to_dict()
    # Ensure all categories exist
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "WARNING", "SUSPICIOUS"]:
        if sev not in severity_counts:
            severity_counts[sev] = 0

    # 3. Data Quality Dimensions
    # Completeness: No missing critical fields
    missing_crit_reasons = [r for r in reason_counts if "MISSING" in r or "NULL" in r]
    missing_crit_count = all_rejections_df[
        all_rejections_df["failure_reason"].str.contains("MISSING|NULL", na=False)
    ].shape[0]
    completeness_score = round(((raw_total - missing_crit_count) / raw_total) * 100, 2)

    # Uniqueness: Deduplicated rows
    dup_count = all_rejections_df[all_rejections_df["failure_reason"].str.contains("DUPLICATE", na=False)].shape[0]
    uniqueness_score = round(((raw_total - dup_count) / raw_total) * 100, 2)

    # Validity: Format and range checks
    invalid_count = all_rejections_df[
        all_rejections_df["failure_reason"].str.contains("INVALID|OUT_OF_RANGE", na=False)
    ].shape[0]
    validity_score = round(((raw_total - invalid_count) / raw_total) * 100, 2)

    # Accuracy/Realism: Financial anomalies
    accuracy_count = all_rejections_df[
        all_rejections_df["failure_reason"].str.contains("UNIT_PRICE|UNREALISTIC|SUSPICIOUS", na=False)
    ].shape[0]
    accuracy_score = round(((raw_total - accuracy_count) / raw_total) * 100, 2)

    # Consistency: Calculation and currency checks
    consistency_count = all_rejections_df[
        all_rejections_df["failure_reason"].str.contains("DISCREPANCY|UNKNOWN_CURRENCY", na=False)
    ].shape[0]
    consistency_score = round(((raw_total - consistency_count) / raw_total) * 100, 2)

    # 4. Pipeline Health
    quality_score = round((valid_total / raw_total) * 100, 2)
    status = "FAIL"
    if quality_score >= 80:
        status = "PASS"
    elif quality_score >= 60:
        status = "PASS_WITH_WARNINGS"

    report = {
        "report_metadata": {
            "audit_timestamp": pd.Timestamp.now().isoformat(),
            "checksum_status": "VERIFIED" if not checksum_error else "ERROR_IN_CALCULATION",
            "pipeline_status": status
        },
        "summary_counts": {
            "raw_total_rows": raw_total,
            "valid_rows_gold": valid_total,
            "rejected_rows_total": rejected_total,
            "mismatch_delta": raw_total - (valid_total + rejected_total)
        },
        "data_quality_dimensions": {
            "completeness_score": f"{completeness_score}%",
            "validity_score": f"{validity_score}%",
            "uniqueness_score": f"{uniqueness_score}%",
            "accuracy_score": f"{accuracy_score}%",
            "consistency_score": f"{consistency_score}%",
            "overall_quality_score": f"{quality_score}%"
        },
        "severity_breakdown": severity_counts,
        "rejection_reason_breakdown": reason_counts
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    return report
