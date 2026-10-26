from pathlib import Path
from datetime import datetime, timezone
import json
import time

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent

SOURCE_FILE = BASE_DIR / "data" / "source" / "orders.csv"
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_FILE = RAW_DIR / "orders_raw.parquet"
WATERMARK_FILE = RAW_DIR / "watermark.json"

PAGE_SIZE = 10
MAX_RETRIES = 3


def read_source_page(source_df, page, page_size):
    """
    Simulasi endpoint API dengan pagination.
    Page dimulai dari 1.
    """
    if page < 1:
        raise ValueError("Nomor page harus >= 1")

    start = (page - 1) * page_size
    end = start + page_size

    return source_df.iloc[start:end].copy()


def fetch_page_with_retry(source_df, page, page_size):
    """
    Mengambil satu halaman dengan retry exponential backoff.

    Untuk demo, kegagalan sementara disimulasikan satu kali
    pada halaman kedua.
    """
    simulated_failure_done = False

    for attempt in range(MAX_RETRIES):
        try:
            # Simulasi satu kegagalan sementara agar retry terlihat.
            if page == 2 and not simulated_failure_done:
                simulated_failure_done = True
                raise ConnectionError(
                    "Simulasi kegagalan sementara saat mengambil page 2"
                )

            return read_source_page(source_df, page, page_size)

        except ConnectionError as error:
            if attempt == MAX_RETRIES - 1:
                raise

            wait_seconds = 2 ** attempt
            print(
                f"[RETRY] Page {page}, percobaan {attempt + 1}: "
                f"{error}. Menunggu {wait_seconds} detik."
            )
            time.sleep(wait_seconds)

    return pd.DataFrame(columns=source_df.columns)


def load_watermark():
    """Membaca watermark sebelumnya jika tersedia."""
    if not WATERMARK_FILE.exists():
        return None

    with open(WATERMARK_FILE, "r", encoding="utf-8") as file:
        state = json.load(file)

    return state.get("watermark")


def save_watermark(value):
    """Menyimpan watermark terakhir yang berhasil diproses."""
    state = {
        "watermark": value,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(WATERMARK_FILE, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2)


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Sumber tidak ditemukan: {SOURCE_FILE}")

    source_df = pd.read_csv(SOURCE_FILE)

    required_columns = {"order_id", "order_ts"}
    if not required_columns.issubset(source_df.columns):
        raise ValueError(
            f"Kolom wajib tidak ditemukan: {required_columns}"
        )

    # Baca watermark dan data raw sebelumnya.
    previous_watermark = load_watermark()

    if RAW_FILE.exists():
        existing_df = pd.read_parquet(RAW_FILE)
    else:
        existing_df = pd.DataFrame(columns=source_df.columns)

    # Ambil data per halaman, bukan membaca seluruh sumber sekaligus.
    fetched_pages = []
    page = 1

    while True:
        page_df = fetch_page_with_retry(
            source_df,
            page=page,
            page_size=PAGE_SIZE,
        )

        if page_df.empty:
            break

        fetched_pages.append(page_df)
        print(f"[INGEST] Page {page}: {len(page_df)} baris")

        if len(page_df) < PAGE_SIZE:
            break

        page += 1

    if fetched_pages:
        fetched_df = pd.concat(fetched_pages, ignore_index=True)
    else:
        fetched_df = pd.DataFrame(columns=source_df.columns)

    # Normalisasi timestamp untuk perbandingan watermark.
    fetched_df["_watermark_ts"] = pd.to_datetime(
        fetched_df["order_ts"],
        utc=True,
        errors="coerce",
    )

    if previous_watermark:
        watermark_ts = pd.to_datetime(previous_watermark, utc=True)
        incremental_df = fetched_df[
            fetched_df["_watermark_ts"] > watermark_ts
        ].copy()
    else:
        incremental_df = fetched_df.copy()

    # Hindari duplikasi order_id terhadap data yang sudah tersimpan.
    existing_ids = set(existing_df["order_id"].dropna().astype(str))
    incremental_df = incremental_df[
        ~incremental_df["order_id"].astype(str).isin(existing_ids)
    ].copy()

    # Jangan menyimpan kolom bantu ke output.
    incremental_df = incremental_df.drop(
        columns=["_watermark_ts"],
        errors="ignore",
    )

    if not incremental_df.empty:
        combined_df = pd.concat(
            [existing_df, incremental_df],
            ignore_index=True,
        )

        # Pertahankan satu baris per order_id.
        combined_df = combined_df.drop_duplicates(
            subset=["order_id"],
            keep="last",
        )

        combined_df.to_parquet(RAW_FILE, index=False)

        # Perbarui watermark berdasarkan timestamp valid tertinggi
        # dari data yang benar-benar masuk pada proses ini.
        new_timestamps = pd.to_datetime(
            incremental_df["order_ts"],
            utc=True,
            errors="coerce",
        ).dropna()

        if not new_timestamps.empty:
            latest_timestamp = new_timestamps.max().isoformat()
            save_watermark(latest_timestamp)

    print("\n" + "=" * 50)
    print("HASIL BATCH INGESTION")
    print("=" * 50)
    print(f"Baris sumber       : {len(source_df)}")
    print(f"Baris diambil      : {len(fetched_df)}")
    print(f"Watermark awal     : {previous_watermark}")
    print(f"Baris baru         : {len(incremental_df)}")

    if RAW_FILE.exists():
        final_df = pd.read_parquet(RAW_FILE)
        print(f"Total raw tersimpan: {len(final_df)}")
        print(f"Duplikat order_id  : {final_df['order_id'].duplicated().sum()}")
    else:
        print("Total raw tersimpan: 0")

    print(f"File raw           : {RAW_FILE.relative_to(BASE_DIR)}")
    print(f"File watermark     : {WATERMARK_FILE.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()
