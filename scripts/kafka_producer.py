
from pathlib import Path
import json
import time

import pandas as pd
from confluent_kafka import Producer


BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_FILE = BASE_DIR / "data" / "source" / "orders.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def delivery_report(error, message):
    """Melaporkan hasil pengiriman setiap event."""
    if error is not None:
        print(f"[GAGAL] Event tidak terkirim: {error}")
    else:
        print(
            f"[TERKIRIM] order_id={message.key().decode('utf-8')} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


def main():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"File sumber tidak ditemukan: {SOURCE_FILE}"
        )

    df = pd.read_csv(SOURCE_FILE)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "ecommerce-order-producer",
        }
    )

    success_count = 0

    print(f"Mengirim {len(df)} event ke topic '{TOPIC}'...")

    for row in df.to_dict(orient="records"):
        order_id = str(row.get("order_id", ""))

        # Ubah NaN dari pandas menjadi None agar JSON valid.
        clean_row = {
            key: None if pd.isna(value) else value
            for key, value in row.items()
        }

        payload = json.dumps(clean_row, ensure_ascii=False)

        # Key order_id menjaga event untuk order yang sama
        # diarahkan ke partisi yang sama.
        producer.produce(
            TOPIC,
            key=order_id.encode("utf-8"),
            value=payload.encode("utf-8"),
            callback=delivery_report,
        )

        success_count += 1

        # Layani callback dan batasi antrean producer.
        producer.poll(0)

    # Tunggu sampai seluruh pesan selesai dikirim.
    remaining = producer.flush(30)

    if remaining > 0:
        raise RuntimeError(
            f"{remaining} event belum selesai dikirim setelah timeout"
        )

    print("\n" + "=" * 45)
    print("PENGIRIMAN PRODUCER SELESAI")
    print("=" * 45)
    print(f"Event yang diantrekan : {success_count}")
    print(f"Event belum terkirim  : {remaining}")
    print(f"Topic                 : {TOPIC}")


if __name__ == "__main__":
    main()
