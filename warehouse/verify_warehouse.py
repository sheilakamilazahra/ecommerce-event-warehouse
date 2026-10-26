
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "ecommerce.duckdb"

TABLES = [
    "raw_orders",
    "stg_orders",
    "dim_customers",
    "dim_product_categories",
    "dim_payment_methods",
    "fact_orders",
    "daily_sales_mart",
]


def main():
    con = duckdb.connect(str(DB_FILE), read_only=True)

    try:
        print("=" * 55)
        print("VERIFIKASI DATA WAREHOUSE")
        print("=" * 55)

        for table in TABLES:
            count = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            print(f"{table:<30} {count:>8} baris")

        print("\nRINGKASAN FAKTA TRANSAKSI")
        summary = con.execute("""
            SELECT
                COUNT(*) AS total_orders,
                COUNT(DISTINCT order_id) AS unique_orders,
                ROUND(SUM(gross_amount), 2) AS gross_sales,
                ROUND(SUM(discount_amount), 2) AS total_discount,
                ROUND(SUM(net_amount), 2) AS net_sales
            FROM fact_orders
        """).fetchone()

        print(f"Total order       : {summary[0]}")
        print(f"Order unik        : {summary[1]}")
        print(f"Penjualan bruto   : {summary[2]}")
        print(f"Total diskon      : {summary[3]}")
        print(f"Penjualan neto    : {summary[4]}")

        print("\nPREVIEW DAILY SALES MART")
        result = con.execute("""
            SELECT *
            FROM daily_sales_mart
            ORDER BY order_date
        """).df()

        print(result.to_string(index=False))

    finally:
        con.close()


if __name__ == "__main__":
    main()
