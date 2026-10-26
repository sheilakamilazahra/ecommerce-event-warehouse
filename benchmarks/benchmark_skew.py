
from pathlib import Path
from time import perf_counter
import gc

import numpy as np
import pandas as pd
import polars as pl

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "source"
RESULT_FILE = BASE_DIR / "benchmarks" / "skew_results.csv"

N_ROWS = 200_000
N_CATEGORIES = 20
REPEATS = 5
SEED = 42

CATEGORIES = [f"category_{i:02d}" for i in range(N_CATEGORIES)]


def generate_datasets():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)

    # Dataset balanced: peluang kategori relatif merata.
    balanced_categories = rng.choice(CATEGORIES, size=N_ROWS)

    # Dataset skewed: 95% kategori utama, sisanya tersebar
    # pada 19 kategori lainnya.
    skewed_categories = np.empty(N_ROWS, dtype=object)
    main_mask = rng.random(N_ROWS) < 0.95
    skewed_categories[main_mask] = CATEGORIES[0]
    skewed_categories[~main_mask] = rng.choice(
        CATEGORIES[1:],
        size=(~main_mask).sum(),
    )

    for name, categories in [
        ("balanced", balanced_categories),
        ("skewed", skewed_categories),
    ]:
        df = pd.DataFrame({
            "order_id": [f"{name}_{i:07d}" for i in range(N_ROWS)],
            "product_category": categories,
            "quantity": rng.integers(1, 6, size=N_ROWS),
            "unit_price": rng.uniform(10_000, 500_000, size=N_ROWS),
        })

        output = DATA_DIR / f"orders_{name}.parquet"
        df.to_parquet(output, index=False)

        proportions = df["product_category"].value_counts(normalize=True)
        print(f"\nDataset {name}:")
        print(f"Jumlah baris       : {len(df):,}")
        print(f"Jumlah kategori    : {df['product_category'].nunique()}")
        print(f"Proporsi kategori terbesar: {proportions.max():.2%}")
        print(f"File: {output}")


def pandas_eager(path):
    df = pd.read_parquet(path)
    return (
        df.groupby("product_category", as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_orders=("order_id", "count"),
            average_price=("unit_price", "mean"),
        )
    )


def polars_eager(path):
    df = pl.read_parquet(path)
    return (
        df.group_by("product_category")
        .agg(
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("order_id").count().alias("total_orders"),
            pl.col("unit_price").mean().alias("average_price"),
        )
    )


def polars_lazy(path):
    return (
        pl.scan_parquet(path)
        .group_by("product_category")
        .agg(
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("order_id").count().alias("total_orders"),
            pl.col("unit_price").mean().alias("average_price"),
        )
        .collect()
    )


def measure(method_name, dataset_name, function):
    # Warm-up tidak dimasukkan ke rata-rata.
    function()

    times = []
    for _ in range(REPEATS):
        gc.collect()
        start = perf_counter()
        result = function()
        times.append(perf_counter() - start)
        del result

    return {
        "dataset": dataset_name,
        "method": method_name,
        "rows": N_ROWS,
        "repeats": REPEATS,
        "mean_seconds": sum(times) / len(times),
        "min_seconds": min(times),
        "max_seconds": max(times),
    }


def main():
    generate_datasets()
    results = []

    methods = [
        ("Pandas Eager", pandas_eager),
        ("Polars Eager", polars_eager),
        ("Polars Lazy", polars_lazy),
    ]

    for dataset_name in ["balanced", "skewed"]:
        path = DATA_DIR / f"orders_{dataset_name}.parquet"

        for method_name, function in methods:
            print(f"Mengukur {method_name} pada {dataset_name}...")
            results.append(
                measure(
                    method_name,
                    dataset_name,
                    lambda fn=function, p=path: fn(p),
                )
            )

    result_df = pd.DataFrame(results)
    result_df.to_csv(RESULT_FILE, index=False)

    print("\n=== HASIL EKSPERIMEN SKEW ===")
    print(
        result_df[
            ["dataset", "method", "mean_seconds",
             "min_seconds", "max_seconds"]
        ].to_string(index=False)
    )
    print(f"\nHasil tersimpan di: {RESULT_FILE}")


if __name__ == "__main__":
    main()
