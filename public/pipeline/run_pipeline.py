from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from clean import clean_data
from report_generator import generate_validation_report
from transform import transform_data
from validate import validate_data


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run() -> int:
    root = _project_root()
    # Updated paths per portfolio optimization
    raw_path = root / "data" / "raw_sales_master.csv"
    outputs_dir = root / "data"
    error_log_path = outputs_dir / "error_log.csv"
    report_path = outputs_dir / "validation_report.json"
    validated_path = outputs_dir / "validated_data.csv"

    outputs_dir.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        print(f"[run_pipeline] ERROR: raw dataset not found at: {raw_path}")
        return 2

    print("==========================================")
    print(" REVENUE PIPELINE PRO: PORTFOLIO SYNC")
    print("==========================================")

    # 1. Ingest
    raw_df = pd.read_csv(raw_path)
    print(f"[ingest] Loaded raw rows: {len(raw_df):,}")

    # 2. Clean
    cleaned_df, rejected_clean = clean_data(raw_df)
    
    # 3. Transform
    transformed_df = transform_data(cleaned_df)

    # 4. Validate (Strict Split)
    final_valid_df, rejected_val, _ = validate_data(transformed_df)
    
    # 5. Consolidated Error Log
    all_rejections = pd.concat([rejected_clean, rejected_val], ignore_index=True)
    
    # 6. Generate Audit Report
    generate_validation_report(
        raw_df=raw_df,
        final_valid_df=final_valid_df,
        all_rejections_df=all_rejections,
        output_path=report_path
    )

    # 7. Persist ONLY essential files for Senior Portfolio
    # We no longer save 'cleaned_data.csv' or 'transformed_data.csv' 
    # to maintain a clean, professional project structure.
    final_valid_df.to_csv(validated_path, index=False)
    all_rejections.to_csv(error_log_path, index=False)

    print("==========================================")
    print(" PIPELINE EXECUTION COMPLETE")
    print(f" Raw Source:      {raw_path.name}")
    print(f" Valid (Gold):    {len(final_valid_df)} rows -> {validated_path.name}")
    print(f" Rejected (Log):   {len(all_rejections)} rows -> {error_log_path.name}")
    print("==========================================")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
