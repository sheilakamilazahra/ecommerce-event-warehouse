
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent

PARQUET_FILE = BASE_DIR / "data" / "valid" / "orders_valid.parquet"
WAREHOUSE_DIR = BASE_DIR / "warehouse"
DB_FILE = WAREHOUSE_DIR / "ecommerce.duckdb"


def main():
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    if not PARQUET_FILE.exists():
        raise FileNotFoundError(
            f"File data valid tidak ditemukan: {PARQUET_FILE}"
        )

    con = duckdb.connect(str(DB_FILE))

    try:
        parquet_path = PARQUET_FILE.as_posix().replace("'", "''")

        con.execute(f"""
            CREATE OR REPLACE TABLE raw_orders AS
            SELECT *
            FROM read_parquet('{parquet_path}')
        """)

        result = con.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT order_id) AS unique_orders,
                MIN(order_ts) AS first_order_ts,
                MAX(order_ts) AS last_order_ts
            FROM raw_orders
        """).fetchone()

        print("=" * 45)
        print("HASIL LOAD DATA KE DUCKDB")
        print("=" * 45)
        print(f"Total baris       : {result[0]}")
        print(f"Order unik        : {result[1]}")
        print(f"Transaksi pertama : {result[2]}")
        print(f"Transaksi terakhir: {result[3]}")
        print(f"Database          : {DB_FILE}")

        print("\nPreview data:")
        print(
            con.execute("""
                SELECT *
                FROM raw_orders
                ORDER BY order_id
                LIMIT 5
            """).df().to_string(index=False)
        )

    finally:
        con.close()


if __name__ == "__main__":
    main()
