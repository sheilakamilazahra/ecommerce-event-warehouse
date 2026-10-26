
from pathlib import Path
from time import perf_counter
import gc
import pandas as pd
import polars as pl

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "raw" / "orders_raw.parquet"
RESULT_FILE = BASE_DIR / "benchmarks" / "benchmark_results.csv"

REPEATS = 10


def benchmark(name, function):
    # Pemanasan agar pengukuran awal tidak terlalu mendominasi.
    function()

    times = []

    for _ in range(REPEATS):
        gc.collect()
        start = perf_counter()
        result = function()
        elapsed = perf_counter() - start
        times.append(elapsed)
        del result

    return {
        "method": name,
        "repeats": REPEATS,
        "mean_seconds": sum(times) / len(times),
        "min_seconds": min(times),
        "max_seconds": max(times),
    }


def pandas_eager():
    df = pd.read_parquet(DATA_FILE)
    result = (
        df.groupby("product_category", as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_orders=("order_id", "count"),
            average_price=("unit_price", "mean"),
        )
    )
    return result


def polars_eager():
    df = pl.read_parquet(DATA_FILE)
    result = (
        df.group_by("product_category")
        .agg(
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("order_id").count().alias("total_orders"),
            pl.col("unit_price").mean().alias("average_price"),
        )
    )
    return result


def polars_lazy():
    query = (
        pl.scan_parquet(DATA_FILE)
        .group_by("product_category")
        .agg(
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("order_id").count().alias("total_orders"),
            pl.col("unit_price").mean().alias("average_price"),
        )
    )
    return query.collect()


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Dataset tidak ditemukan: {DATA_FILE}")

    print(f"Dataset: {DATA_FILE}")
    print(f"Jumlah baris: {len(pd.read_parquet(DATA_FILE))}")
    print(f"Pengulangan per metode: {REPEATS}")
    print("\nMenjalankan benchmark...\n")

    results = [
        benchmark("Pandas Eager", pandas_eager),
        benchmark("Polars Eager", polars_eager),
        benchmark("Polars Lazy", polars_lazy),
    ]

    results_df = pd.DataFrame(results)
    results_df.to_csv(RESULT_FILE, index=False)

    print(results_df.to_string(index=False))
    print(f"\nHasil tersimpan di: {RESULT_FILE}")


if __name__ == "__main__":
    main()
