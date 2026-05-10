import pandas as pd
import numpy as np

def audit():
    v_path = "data/validated_data.csv"
    e_path = "data/error_log.csv"
    r_path = "data/raw_sales_master.csv"
    
    print("--- VALIDATED DATA AUDIT ---")
    v = pd.read_csv(v_path)
    
    # 1. Null Check
    nulls = v.isna().sum()
    if nulls.any():
        print(f"FAILED: Found nulls in validated data:\n{nulls[nulls > 0]}")
    else:
        print("PASSED: No nulls in Gold dataset.")
        
    # 2. Uniqueness
    dups = v["order_id"].duplicated().sum()
    if dups > 0:
        print(f"FAILED: Found {dups} duplicate order_ids.")
    else:
        print("PASSED: All Order IDs are unique.")
        
    # 3. Calculation Integrity
    # profit = amount - cost
    # allow for tiny floating point diff
    calc_diff = np.abs(v["profit_usd"] - (v["amount_usd"] - v["cost_usd"]))
    bad_calc = (calc_diff > 0.01).sum()
    if bad_calc > 0:
        print(f"FAILED: {bad_calc} rows have profit calculation discrepancies.")
    else:
        print("PASSED: Profit calculation is 100% consistent.")
        
    # 4. Realistic Margins (0 to 100 range for gold data)
    bad_margin = ((v["margin_pct"] < -100) | (v["margin_pct"] > 100)).sum()
    if bad_margin > 0:
        print(f"FAILED: {bad_margin} rows have margins out of -100 to 100 range.")
    else:
        print("PASSED: All margins are within plausible bounds.")

    print("\n--- ERROR LOG AUDIT ---")
    e = pd.read_csv(e_path)
    required_cols = {"severity", "failure_reason"}
    missing = required_cols - set(e.columns)
    if missing:
        print(f"FAILED: Error log missing columns: {missing}")
    else:
        print("PASSED: Error log contains strict audit columns.")
    
    print(f"Error log contains {len(e)} records.")
    
if __name__ == "__main__":
    audit()
