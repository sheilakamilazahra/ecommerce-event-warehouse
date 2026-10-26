
from pathlib import Path
import json
import time

import pandas as pd
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from validate_orders import OrderSchema, format_validation_errors


BASE_DIR = Path(__file__).resolve().parent.parent
VALID_DIR = BASE_DIR / "data" / "valid"
REJECTED_DIR = BASE_DIR / "data" / "rejected"

VALID_FILE = VALID_DIR / "orders_kafka_valid.parquet"
REJECTED_FILE = REJECTED_DIR / "orders_kafka_rejected.parquet"
REJECTED_CSV = REJECTED_DIR / "orders_kafka_rejected.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
CONSUMER_GROUP = "orders-validation-group"

# Berhenti jika tidak ada pesan baru selama 5 detik.
IDLE_TIMEOUT_SECONDS = 5


def load_existing(path):
    """Membaca hasil sebelumnya agar output tidak hilang saat rerun."""
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def normalize_values(row):
    """Mengubah nilai NaN menjadi None."""
    return {
        key: None if pd.isna(value) else value
        for key, value in row.items()
    }


def main():
    VALID_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC])

    new_valid = []
    new_rejected = []
    received_count = 0
    last_message_time = time.monotonic()

    print(f"Mendengarkan topic '{TOPIC}'...")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print("Menunggu pesan; akan berhenti setelah 5 detik idle.")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                if time.monotonic() - last_message_time >= IDLE_TIMEOUT_SECONDS:
                    break
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"[KAFKA ERROR] {message.error()}")
                continue

            last_message_time = time.monotonic()
            received_count += 1

            partition = message.partition()
            offset = message.offset()

            try:
                raw = json.loads(message.value().decode("utf-8"))

                if not isinstance(raw, dict):
                    raise ValueError("Event JSON harus berupa objek")

                clean_raw = normalize_values(raw)
                order = OrderSchema.model_validate(clean_raw)
                record = order.model_dump()

                record["order_ts"] = pd.to_datetime(
                    record["order_ts"], utc=True
                )

                record["kafka_partition"] = partition
                record["kafka_offset"] = offset
                new_valid.append(record)

                print(
                    f"[VALID] {record['order_id']} "
                    f"(partition={partition}, offset={offset})"
                )

            except (ValidationError, ValueError, json.JSONDecodeError) as error:
                rejected = {
                    **(raw if "raw" in locals() and isinstance(raw, dict) else {}),
                    "kafka_partition": partition,
                    "kafka_offset": offset,
                    "rejection_reason": (
                        format_validation_errors(error)
                        if isinstance(error, ValidationError)
                        else str(error)
                    ),
                }
                new_rejected.append(rejected)

                print(
                    f"[REJECTED] order_id={rejected.get('order_id')} "
                    f"partition={partition} offset={offset}: "
                    f"{rejected['rejection_reason']}"
                )

            finally:
                # Mencegah variabel event sebelumnya terbawa ke event berikutnya.
                if "raw" in locals():
                    del raw

        # Baca sink lama agar hasil sebelumnya tetap dipertahankan.
        old_valid = load_existing(VALID_FILE)
        old_rejected = load_existing(REJECTED_FILE)

        valid_columns = [
            "order_id", "customer_id", "order_ts", "product_category",
            "quantity", "unit_price", "discount_pct", "payment_method",
            "status", "kafka_partition", "kafka_offset",
        ]

        rejected_columns = [
            "order_id", "customer_id", "order_ts", "product_category",
            "quantity", "unit_price", "discount_pct", "payment_method",
            "status", "kafka_partition", "kafka_offset", "rejection_reason",
        ]

        new_valid_df = pd.DataFrame(new_valid)
        new_rejected_df = pd.DataFrame(new_rejected)

        if not old_valid.empty or not new_valid_df.empty:
            combined_valid = pd.concat(
                [old_valid, new_valid_df], ignore_index=True
            )
            combined_valid = combined_valid.drop_duplicates(
                subset=["order_id"], keep="first"
            )
            combined_valid.to_parquet(VALID_FILE, index=False)
        else:
            combined_valid = pd.DataFrame(columns=valid_columns)

        if not old_rejected.empty or not new_rejected_df.empty:
            combined_rejected = pd.concat(
                [old_rejected, new_rejected_df], ignore_index=True
            )

            # Satu hasil karantina per order_id untuk sink ini.
            # Untuk event tanpa order_id, gunakan lokasi event Kafka.
            if "order_id" not in combined_rejected.columns:
                combined_rejected["order_id"] = None

            combined_rejected["_dedupe_key"] = combined_rejected.apply(
                lambda row: (
                    f"order:{row['order_id']}"
                    if pd.notna(row.get("order_id"))
                    and str(row.get("order_id")).strip()
                    else (
                        f"kafka:{row.get('kafka_partition')}:"
                        f"{row.get('kafka_offset')}"
                    )
                ),
                axis=1,
            )

            combined_rejected = combined_rejected.drop_duplicates(
                subset=["_dedupe_key"], keep="first"
            ).drop(columns=["_dedupe_key"])

            combined_rejected.to_parquet(REJECTED_FILE, index=False)
            combined_rejected.to_csv(REJECTED_CSV, index=False)
        else:
            combined_rejected = pd.DataFrame(columns=rejected_columns)

        # Commit setelah hasil sink ditulis.
        # Jika terjadi crash sebelum commit, pesan dapat dibaca ulang.
        if received_count > 0:
            consumer.commit(asynchronous=False)

        print("\n" + "=" * 50)
        print("HASIL KAFKA CONSUMER")
        print("=" * 50)
        print(f"Pesan diterima sesi ini : {received_count}")
        print(f"Event valid baru        : {len(new_valid)}")
        print(f"Event rejected baru     : {len(new_rejected)}")
        print(f"Total valid di sink     : {len(combined_valid)}")
        print(f"Total rejected di sink  : {len(combined_rejected)}")
        print(f"Valid Parquet           : {VALID_FILE.relative_to(BASE_DIR)}")
        print(f"Rejected Parquet        : {REJECTED_FILE.relative_to(BASE_DIR)}")
        print(f"Rejected CSV            : {REJECTED_CSV.relative_to(BASE_DIR)}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
