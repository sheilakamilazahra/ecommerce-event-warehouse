
from pathlib import Path
import time

import pandas as pd
import polars as pl


# =========================================================
# 1. KONFIGURASI
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "source"
RESULT_DIR = BASE_DIR / "benchmarks"

DATA_DIR.mkdir(parents=True, exist_ok=True)

N_ROWS = 200_000
N_CUSTOMERS = 1_000
N_REPEATS = 5
DOMINANT_CUSTOMER = "CUST_DOMINANT"


# =========================================================
# 2. MEMBUAT DATA BALANCED DAN SKEWED
# =========================================================
def create_datasets():
    # Data balanced: pesanan tersebar ke banyak customer.
    balanced = pd.DataFrame({
        "order_id": [f"ORD{i:07d}" for i in range(N_ROWS)],
        "customer_id": [
            f"CUST{i % N_CUSTOMERS:04d}"
            for i in range(N_ROWS)
        ],
        "net_amount": [
            float((i % 100) + 1)
            for i in range(N_ROWS)
        ],
    })

    # Data skewed: 95% pesanan dimiliki satu customer.
    dominant_count = int(N_ROWS * 0.95)
    other_count = N_ROWS - dominant_count

    skewed_customer_ids = (
        [DOMINANT_CUSTOMER] * dominant_count
        + [
            f"CUST{i % (N_CUSTOMERS - 1):04d}"
            for i in range(other_count)
        ]
    )

    skewed = pd.DataFrame({
        "order_id": [f"ORD{i:07d}" for i in range(N_ROWS)],
        "customer_id": skewed_customer_ids,
        "net_amount": [
            float((i % 100) + 1)
            for i in range(N_ROWS)
        ],
    })

    balanced_path = DATA_DIR / "customer_balanced.parquet"
    skewed_path = DATA_DIR / "customer_skewed.parquet"

    balanced.to_parquet(balanced_path, index=False)
    skewed.to_parquet(skewed_path, index=False)

    print(f"Data balanced: {len(balanced):,} baris")
    print(f"Data skewed  : {len(skewed):,} baris")
    print(f"Customer dominan: {DOMINANT_CUSTOMER}")
    print(f"Proporsi dominan: {dominant_count / N_ROWS:.0%}")

    return balanced_path, skewed_path


# =========================================================
# 3. BENCHMARK PANDAS, POLARS EAGER, DAN POLARS LAZY
# =========================================================
def benchmark_file(path, distribution):
    results = []

    for method in ["Pandas Eager", "Polars Eager", "Polars Lazy"]:
        times = []

        for _ in range(N_REPEATS):
            start = time.perf_counter()

            if method == "Pandas Eager":
                df = pd.read_parquet(path)
                result = (
                    df.groupby("customer_id", as_index=False)
                    .agg(
                        total_orders=("order_id", "count"),
                        total_net_sales=("net_amount", "sum"),
                    )
                )

            elif method == "Polars Eager":
                df = pl.read_parquet(path)
                result = (
                    df.group_by("customer_id")
                    .agg(
                        pl.col("order_id").count().alias("total_orders"),
                        pl.col("net_amount").sum().alias("total_net_sales"),
                    )
                )

            else:
                result = (
                    pl.scan_parquet(path)
                    .group_by("customer_id")
                    .agg(
                        pl.col("order_id").count().alias("total_orders"),
                        pl.col("net_amount").sum().alias("total_net_sales"),
                    )
                    .collect()
                )

            elapsed = time.perf_counter() - start
            times.append(elapsed)

        results.append({
            "distribution": distribution,
            "method": method,
            "rows": N_ROWS,
            "groups": len(result),
            "mean_seconds": sum(times) / len(times),
            "min_seconds": min(times),
            "max_seconds": max(times),
        })

    return results


# =========================================================
# 4. PERIKSA DISTRIBUSI CUSTOMER
# =========================================================
def show_distribution(path, distribution):
    df = pd.read_parquet(path)

    counts = df["customer_id"].value_counts()
    total_rows = len(df)

    print(f"\n--- Distribusi {distribution} ---")
    print(f"Jumlah baris: {total_rows:,}")
    print(f"Jumlah customer unik: {df['customer_id'].nunique():,}")
    print(f"Customer dengan pesanan terbanyak: {counts.index[0]}")
    print(f"Jumlah pesanan terbanyak: {counts.iloc[0]:,}")
    print(f"Proporsi pesanan terbanyak: {counts.iloc[0] / total_rows:.2%}")

    print("\nLima customer teratas:")
    print(counts.head(5).to_string())


# =========================================================
# 5. JALANKAN EKSPERIMEN DAN SIMPAN HASIL
# =========================================================
def main():
    balanced_path, skewed_path = create_datasets()

    show_distribution(balanced_path, "Balanced")
    show_distribution(skewed_path, "Skewed")

    results = []
    results.extend(benchmark_file(balanced_path, "Balanced"))
    results.extend(benchmark_file(skewed_path, "Skewed"))

    results_df = pd.DataFrame(results)
    output_path = RESULT_DIR / "customer_skew_results.csv"
    results_df.to_csv(output_path, index=False)

    print("\n--- HASIL BENCHMARK ---")
    print(
        results_df[
            ["distribution", "method", "rows", "groups", "mean_seconds"]
        ].to_string(index=False)
    )

    print(f"\nHasil tersimpan di: {output_path}")


if __name__ == "__main__":
    main()
