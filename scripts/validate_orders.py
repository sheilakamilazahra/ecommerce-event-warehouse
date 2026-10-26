from pathlib import Path
from typing import Literal
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, StrictInt, field_validator


# Lokasi proyek berdasarkan folder kerja saat ini
BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_FILE = BASE_DIR / "data" / "source" / "orders.csv"
VALID_DIR = BASE_DIR / "data" / "valid"
REJECTED_DIR = BASE_DIR / "data" / "rejected"

VALID_FILE = VALID_DIR / "orders_valid.parquet"
REJECTED_FILE = REJECTED_DIR / "orders_rejected.parquet"
REJECTED_CSV = REJECTED_DIR / "orders_rejected.csv"


class OrderSchema(BaseModel):
    """Kontrak data untuk satu transaksi e-commerce."""

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: StrictInt = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

    @field_validator(
        "order_id",
        "customer_id",
        "product_category",
        "payment_method",
        mode="before",
    )
    @classmethod
    def validate_non_empty_string(cls, value):
        if not isinstance(value, str):
            raise ValueError("Nilai wajib diisi dengan teks")
        value = value.strip()
        if not value:
            raise ValueError("Nilai tidak boleh kosong")
        return value

    @field_validator("order_ts", mode="before")
    @classmethod
    def validate_timestamp(cls, value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("order_ts wajib berupa timestamp yang valid")
        return value.strip()


def normalize_missing_values(row: dict) -> dict:
    """Mengubah NaN dari pandas menjadi None agar mudah divalidasi."""
    normalized = {}

    for key, value in row.items():
        if pd.isna(value):
            normalized[key] = None
        else:
            normalized[key] = value

    return normalized


def format_validation_errors(error: ValidationError) -> str:
    """Menggabungkan seluruh alasan penolakan untuk satu baris."""
    messages = []

    for item in error.errors():
        field = ".".join(str(part) for part in item["loc"])
        messages.append(f"{field}: {item['msg']}")

    return "; ".join(messages)


def main():
    VALID_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"File sumber tidak ditemukan: {SOURCE_FILE}"
        )

    df = pd.read_csv(SOURCE_FILE)

    required_columns = [
        "order_id",
        "customer_id",
        "order_ts",
        "product_category",
        "quantity",
        "unit_price",
        "discount_pct",
        "payment_method",
        "status",
    ]

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Kolom wajib tidak ditemukan: {missing_columns}"
        )

    valid_rows = []
    rejected_rows = []

    for row_number, row in enumerate(
        df[required_columns].to_dict(orient="records"),
        start=2,  # Header CSV berada pada baris 1
    ):
        raw_row = normalize_missing_values(row)

        try:
            order = OrderSchema.model_validate(raw_row)

            clean_row = order.model_dump()
            valid_rows.append(clean_row)

        except ValidationError as error:
            rejected_row = raw_row.copy()
            rejected_row["source_row"] = row_number
            rejected_row["rejection_reason"] = format_validation_errors(error)
            rejected_rows.append(rejected_row)

    valid_df = pd.DataFrame(valid_rows, columns=required_columns)
    rejected_df = pd.DataFrame(
        rejected_rows,
        columns=required_columns + ["source_row", "rejection_reason"],
    )

    # Menyimpan timestamp yang valid dalam format datetime UTC.
    if not valid_df.empty:
        valid_df["order_ts"] = pd.to_datetime(
            valid_df["order_ts"], utc=True
        )

    valid_df.to_parquet(VALID_FILE, index=False)
    rejected_df.to_parquet(REJECTED_FILE, index=False)
    rejected_df.to_csv(REJECTED_CSV, index=False)

    print("=" * 55)
    print("HASIL VALIDASI DATA TRANSAKSI")
    print("=" * 55)
    print(f"Total baris sumber : {len(df)}")
    print(f"Data valid         : {len(valid_df)}")
    print(f"Data ditolak       : {len(rejected_df)}")
    print(f"Total diproses     : {len(valid_df) + len(rejected_df)}")
    print()
    print(f"Parquet valid      : {VALID_FILE.relative_to(BASE_DIR)}")
    print(f"Parquet rejected   : {REJECTED_FILE.relative_to(BASE_DIR)}")
    print(f"CSV rejected       : {REJECTED_CSV.relative_to(BASE_DIR)}")

    if not rejected_df.empty:
        print("\nDETAIL BARIS YANG DITOLAK:")
        print(
            rejected_df[
                ["order_id", "source_row", "rejection_reason"]
            ].to_string(index=False)
        )

    print("\nValidasi selesai.")


if __name__ == "__main__":
    main()