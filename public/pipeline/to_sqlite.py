import pandas as pd
import sqlite3
from pathlib import Path

def migrate_to_sqlite():
    root = Path(__file__).resolve().parents[1]
    csv_path = root / "data" / "validated_data.csv"
    db_path = root / "data" / "warehouse.db"

    if not csv_path.exists():
        print(f"Error: {csv_path} not found.")
        return

    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)

    # Re-calculate canonical fields for the warehouse
    def get_canonical_region(reg):
        mapping = {"NORTH AMERICA": "North America", "APAC": "Asia-Pacific", "EMEA": "Europe, Middle East and Africa", "AFRICA": "Africa"}
        return mapping.get(str(reg).strip().upper(), str(reg).strip().title())

    def get_region_group(reg):
        if reg == "North America": return "AMERICAS"
        if reg == "Asia-Pacific": return "APAC"
        if reg in ["Europe, Middle East and Africa", "Africa"]: return "EMEA"
        return "OTHER"

    df['region'] = df['region'].apply(get_canonical_region)
    df['region_group'] = df['region'].apply(get_region_group)
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Rename columns to match the Semantic Layer's expected schema
    df = df.rename(columns={
        'customer': 'customer_id',
        'amount_usd': 'revenue_usd'
    })

    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    
    # We use if_exists='replace' to ensure a fresh start
    # We specify the table name as 'orders'
    print("Writing to 'orders' table...")
    df.to_sql('orders', conn, if_exists='replace', index=False)

    # Add an index on order_date for performance (Senior move!)
    conn.execute("CREATE INDEX idx_order_date ON orders(order_date)")
    conn.commit()
    conn.close()

    print("Migration Complete: data/warehouse.db created with indexed 'orders' table.")

if __name__ == "__main__":
    migrate_to_sqlite()
