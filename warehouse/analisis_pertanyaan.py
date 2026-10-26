
import duckdb

# Membuka database DuckDB yang sudah dibuat
con = duckdb.connect(
    "warehouse/ecommerce.duckdb",
    read_only=True
)

queries = {
    "2. Penjualan Bersih per Kategori": """
        SELECT
            product_category,
            ROUND(SUM(net_amount), 2) AS total_penjualan_bersih
        FROM main.stg_orders
        GROUP BY product_category
        ORDER BY total_penjualan_bersih DESC
    """,

    "3. Jumlah dan Nilai Pesanan per Status": """
        SELECT
            status,
            COUNT(*) AS jumlah_pesanan,
            ROUND(SUM(net_amount), 2) AS total_penjualan_bersih
        FROM main.stg_orders
        GROUP BY status
        ORDER BY jumlah_pesanan DESC
    """,

    "4. Metode Pembayaran Terpopuler": """
        SELECT
            payment_method,
            COUNT(*) AS jumlah_pesanan
        FROM main.stg_orders
        GROUP BY payment_method
        ORDER BY jumlah_pesanan DESC
    """
}

for judul, query in queries.items():
    print("\n" + "=" * 60)
    print(judul)
    print("=" * 60)

    hasil = con.execute(query).df()
    print(hasil.to_string(index=False))

con.close()
