import duckdb
import os

db_path = "dev.duckdb"
output_dir = "data/exports"

tables = {
    "fct_payment_operations": "main_finance",
    "fct_customer_kpis":      "main_finance",
    "fct_daily_revenue":      "main_core",
    "dim_customers":          "main_core",
}

os.makedirs(output_dir, exist_ok=True)

conn = duckdb.connect(db_path)

for table, schema in tables.items():
    output_path = f"{output_dir}/{table}.csv"
    conn.execute(f"""
        COPY {schema}.{table} 
        TO '{output_path}' 
        (FORMAT CSV, HEADER)
    """)
    row_count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    print(f"Exported {table}.csv — {row_count:,} rows to {output_path}")

conn.close()
print("\nAll exports complete. Upload files from data/exports/ to Google Sheets.")