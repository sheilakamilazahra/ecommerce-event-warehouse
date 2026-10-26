
from pathlib import Path
import json

import pandas as pd
from confluent_kafka import Producer


BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_FILE = BASE_DIR / "data" / "source" / "orders.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def delivery_report(error, message):
    if error is not None:
        print(f"[GAGAL] {error}")
    else:
        print(
            f"[TERKIRIM] order_id={message.key().decode('utf-8')} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


def main():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {SOURCE_FILE}")

    df = pd.read_csv(SOURCE_FILE)

    # Ambil satu transaksi asli agar payload tetap valid.
    row = df.iloc[0].to_dict()
    order_id = str(row["order_id"])

    # Ubah NaN menjadi None agar dapat dikirim sebagai JSON.
    clean_row = {
        key: None if pd.isna(value) else value
        for key, value in row.items()
    }

    payload = json.dumps(clean_row, ensure_ascii=False).encode("utf-8")
    key = order_id.encode("utf-8")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "duplicate-test-producer",
    })

    print(f"Menguji duplikasi order_id: {order_id}")

    # Kirim payload identik dua kali dengan key yang sama.
    for attempt in range(1, 3):
        producer.produce(
            TOPIC,
            key=key,
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)
        print(f"Event duplikat ke-{attempt} dimasukkan ke antrean")

    remaining = producer.flush(30)

    if remaining > 0:
        raise RuntimeError(f"{remaining} event belum selesai terkirim")

    print("\nPengujian producer selesai.")
    print("Dua event dengan order_id yang sama telah dikirim.")


if __name__ == "__main__":
    main()
