# End-to-End Event-to-Warehouse Pipeline: Analisis Transaksi E-Commerce

## 1. Deskripsi Proyek

Proyek ini merupakan Mini Project Praktikum Data Processing Workflow yang mengimplementasikan pipeline pemrosesan data transaksi e-commerce dari proses ingestion hingga penyimpanan dan analisis di data warehouse.

Pipeline menggabungkan batch ingestion, event streaming menggunakan Apache Kafka, validasi data menggunakan Pydantic, pemrosesan data menggunakan Pandas dan Polars, serta pemodelan data warehouse menggunakan DuckDB dan dbt.

Tujuan proyek ini adalah membangun alur pemrosesan data yang terstruktur, dapat diuji, mendukung penanganan data tidak valid, dan menghasilkan ringkasan penjualan harian.

## 2. Tujuan

1. Mengimplementasikan batch ingestion dengan pagination, retry menggunakan exponential backoff, dan incremental loading berbasis watermark.
2. Membangun pipeline event streaming menggunakan Kafka producer dan consumer.
3. Memvalidasi kontrak data transaksi dan mengarantina data yang tidak valid.
4. Membandingkan performa Pandas dan Polars, termasuk Polars Lazy API.
5. Mengeksplorasi pengaruh distribusi data skew terhadap waktu pemrosesan.
6. Membangun data warehouse menggunakan DuckDB dan transformasi data menggunakan dbt.
7. Menghasilkan daily sales mart serta melakukan pengujian kualitas data.

## 3. Teknologi yang Digunakan

| Teknologi | Fungsi |
|---|---|
| Python | Bahasa pemrograman utama |
| Pandas | Pemrosesan dan analisis data |
| Polars | Pemrosesan data eager dan lazy |
| Pydantic | Validasi kontrak data |
| Apache Kafka | Pengiriman dan konsumsi event transaksi |
| Docker Compose | Menjalankan layanan Kafka |
| PyArrow / Parquet | Penyimpanan data berbasis kolom |
| DuckDB | Database analitik lokal |
| dbt-duckdb | Transformasi, pengujian, dan dokumentasi model data |
| Requests | Dukungan HTTP untuk ingestion |
| Matplotlib / library visualisasi tambahan | Dapat digunakan untuk visualisasi hasil analisis jika diperlukan |

## 4. Arsitektur Pipeline

```text
                 CSV / Simulated API
                          |
                          v
                Batch Ingestion
          Pagination, Retry, Watermark
                          |
                          v
                 Raw Parquet Data
                          |
              +-----------+-----------+
              |                       |
              v                       v
       Pydantic Validation       Kafka Producer
              |                       |
        +-----+-----+                 v
        |           |           Kafka Topic
        v           v             orders
      Valid       Rejected            |
        |           |                 v
        |           |          Kafka Consumer
        |           |          + Validation
        |           |                 |
        v           v                 v
        Valid / Rejected Parquet Files
                          |
                          v
                    DuckDB
                    raw_orders
                          |
                          v
                    dbt Staging
                     stg_orders
                          |
                          v
                  Dimension Tables
                          |
                          v
                     fact_orders
                          |
                          v
                  daily_sales_mart
```

## 5. Struktur Folder

```text
ecommerce-event-warehouse/
├── data/
│   ├── source/
│   │   └── orders.csv
│   ├── raw/
│   │   ├── orders_raw.parquet
│   │   └── watermark.json
│   ├── valid/
│   │   ├── orders_valid.parquet
│   │   └── orders_kafka_valid.parquet
│   └── rejected/
│       ├── orders_rejected.parquet
│       ├── orders_rejected.csv
│       ├── orders_kafka_rejected.parquet
│       └── orders_kafka_rejected.csv
├── scripts/
│   ├── batch_ingest.py
│   ├── validate_orders.py
│   ├── kafka_producer.py
│   └── kafka_consumer.py
├── benchmarks/
│   ├── benchmark_pandas_polars.py
│   ├── benchmark_skew.py
│   ├── benchmark_results.csv
│   └── skew_results.csv
├── warehouse/
│   ├── load_warehouse.py
│   ├── verify_warehouse.py
│   ├── ecommerce.duckdb
│   └── dbt_ecommerce/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── models/
│       └── target/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

Struktur di atas merupakan gambaran artefak utama. File tambahan dapat muncul selama proses pengujian dan pembuatan dokumentasi dbt.

## 6. Persiapan Lingkungan

Persyaratan lingkungan:

- Python 3.10 atau versi yang kompatibel dengan dependensi proyek.
- Docker Desktop dengan Docker Compose.
- Visual Studio Code atau editor kode lainnya.

### 6.1. Membuat dan mengaktifkan virtual environment

Jalankan dari folder utama proyek:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 6.2. Menginstal dependensi

```powershell
python -m pip install -r requirements.txt
```

### 6.3. Menjalankan Kafka

Pastikan Docker Desktop sedang berjalan, kemudian jalankan:

```powershell
docker compose up -d
docker compose ps
```

Pastikan container Kafka berjalan dalam kondisi sehat sebelum producer dan consumer digunakan.

## 7. Implementasi Pipeline

### 7.1. Batch Ingestion

Batch ingestion diimplementasikan melalui `scripts/batch_ingest.py`.

Fitur yang diterapkan:

- Pagination dengan ukuran halaman 10 data.
- Simulasi kegagalan sementara pada salah satu halaman.
- Retry dengan exponential backoff.
- Incremental loading berdasarkan watermark timestamp.
- Penyimpanan data dalam format Parquet.
- Deduplikasi berdasarkan `order_id`.

Data sumber berisi 36 baris. Pada eksekusi pertama, 36 data berhasil dimuat ke raw Parquet. Pada eksekusi kedua terhadap sumber statis yang sama, tidak ada data baru yang ditambahkan.

Hasil ini menunjukkan bahwa mekanisme deduplikasi mencegah penambahan ulang transaksi yang sama pada pengujian tersebut.

### 7.2. Validasi Data Menggunakan Pydantic

Validasi dilakukan melalui `scripts/validate_orders.py`.

Aturan validasi meliputi:

- `order_id` dan `customer_id` wajib terisi.
- `quantity` harus berupa integer positif.
- `unit_price` tidak boleh negatif.
- `discount_pct` harus berada pada rentang 0 sampai 1.
- `order_ts` harus dapat diparse sebagai timestamp.
- `status` harus termasuk `paid`, `shipped`, `cancelled`, atau `refunded`.

Data valid dan data yang ditolak disimpan secara terpisah. File rejected juga menyimpan alasan penolakan agar kesalahan data dapat ditelusuri.

### 7.3. Event Streaming dengan Kafka

Kafka digunakan untuk mengirim event transaksi melalui topic `orders`.

Producer:

```powershell
python scripts/kafka_producer.py
```

Consumer:

```powershell
python scripts/kafka_consumer.py
```

Topic `orders` dikonfigurasi menggunakan tiga partisi. `order_id` digunakan sebagai key pesan untuk membantu pengelompokan pesan berdasarkan kunci.

Consumer menggunakan consumer group `orders-validation-group`, melakukan validasi event, menyimpan data valid dan rejected, serta melakukan commit offset setelah proses penyimpanan.

Pipeline ini menggunakan pendekatan at-least-once dengan sink yang dirancang idempoten untuk mengurangi duplikasi saat event diproses ulang.

Konfigurasi Kafka lokal menggunakan satu broker dan replication factor 1. Konfigurasi ini ditujukan untuk praktikum, bukan untuk menjamin ketersediaan tinggi pada lingkungan produksi.

### 7.4. Benchmark Pandas dan Polars

Benchmark dilakukan melalui:

```powershell
python benchmarks/benchmark_pandas_polars.py
```

Pengujian membandingkan:

- Pandas eager.
- Polars eager.
- Polars lazy.

Hasil benchmark disimpan pada `benchmarks/benchmark_results.csv`.

Pengujian tambahan terhadap distribusi data seimbang dan skew dilakukan melalui:

```powershell
python benchmarks/benchmark_skew.py
```

Dataset eksperimen berisi 200.000 baris untuk masing-masing distribusi. Hasil disimpan pada `benchmarks/skew_results.csv`.

Hasil pengukuran perlu diinterpretasikan sesuai ukuran dataset, kondisi perangkat, jumlah pengulangan, dan overhead pembacaan file.

### 7.5. DuckDB Data Warehouse

Data valid dimuat ke DuckDB melalui:

```powershell
python warehouse/load_warehouse.py
```

Database disimpan pada `warehouse/ecommerce.duckdb`, dengan tabel awal `raw_orders`.

Pemeriksaan tabel dan ringkasan transaksi dilakukan menggunakan:

```powershell
python warehouse/verify_warehouse.py
```

### 7.6. Transformasi Data Menggunakan dbt

Masuk ke direktori proyek dbt:

```powershell
cd warehouse/dbt_ecommerce
```

Periksa konfigurasi koneksi:

```powershell
dbt debug --profiles-dir .
```

Jalankan transformasi:

```powershell
dbt run --profiles-dir .
```

Jalankan pengujian kualitas data:

```powershell
dbt test --profiles-dir .
```

Buat dokumentasi:

```powershell
dbt docs generate --profiles-dir .
```

Jalankan server dokumentasi:

```powershell
dbt docs serve --profiles-dir .
```

Dokumentasi dapat diakses melalui `http://localhost:8080` selama server berjalan.

Model dbt mencakup:

- `stg_orders`: staging dan standardisasi data transaksi.
- `dim_customers`: dimensi pelanggan.
- `dim_product_categories`: dimensi kategori produk.
- `dim_payment_methods`: dimensi metode pembayaran.
- `fact_orders`: fakta transaksi.
- `daily_sales_mart`: ringkasan penjualan harian.

Pengujian mencakup pemeriksaan `unique`, `not_null`, dan `relationships`.

## 8. Hasil dan Evaluasi

### 8.1. Hasil Ingestion dan Validasi

| Tahap | Jumlah data |
|---|---:|
| Data sumber | 36 |
| Data valid | 30 |
| Data rejected | 6 |
| Data raw setelah ingestion pertama | 36 |
| Data baru pada ingestion kedua | 0 |

Enam data rejected mencakup kasus quantity nol, harga negatif, diskon di luar rentang, timestamp tidak valid, status tidak dikenal, dan customer ID kosong.

### 8.2. Hasil Benchmark Pandas vs. Polars

Pengukuran pada dataset raw berukuran 36 baris menghasilkan rata-rata waktu berikut:

| Metode | Rata-rata waktu |
|---|---:|
| Pandas eager | 0,008101 detik |
| Polars eager | 0,001927 detik |
| Polars lazy | 0,001804 detik |

Dalam pengujian ini, Polars eager sekitar 4,2 kali lebih cepat dan Polars lazy sekitar 4,5 kali lebih cepat dibandingkan Pandas eager.

Namun, dataset hanya berisi 36 baris sehingga hasil tersebut tidak dapat digeneralisasikan sebagai perbandingan performa pada semua ukuran data.

### 8.3. Hasil Eksperimen Data Skew

Eksperimen tambahan menggunakan dataset 200.000 baris dengan 20 kategori.

| Distribusi | Proporsi kategori terbesar |
|---|---:|
| Balanced | 5,08% |
| Skewed | 95,04% |

Pada pengujian ini, waktu pemrosesan tidak selalu meningkat pada data skew. Hasil pengukuran dapat dipengaruhi oleh overhead I/O, implementasi agregasi, dan variasi lingkungan eksekusi.

### 8.4. Hasil Data Warehouse

| Tabel | Jumlah baris |
|---|---:|
| `raw_orders` | 30 |
| `stg_orders` | 30 |
| `dim_customers` | 25 |
| `dim_product_categories` | 4 |
| `dim_payment_methods` | 3 |
| `fact_orders` | 30 |
| `daily_sales_mart` | 10 |

### 8.5. Ringkasan Penjualan

| Metrik | Hasil |
|---|---:|
| Total transaksi | 30 |
| Transaksi unik | 30 |
| Penjualan bruto | 4.245,83 |
| Total diskon | 297,37 |
| Penjualan neto | 3.948,48 |

Seluruh enam model dbt berhasil dibuat dan 24 data test dbt berhasil lulus pada eksekusi pengujian yang dilakukan.

## 9. Keterbatasan

1. Sumber batch menggunakan CSV dan simulasi pagination API, bukan integrasi dengan API eksternal produksi.
2. Retry diuji menggunakan kegagalan yang disimulasikan.
3. Watermark timestamp perlu dikembangkan lebih lanjut untuk menangani late-arriving events dan perubahan data historis.
4. Kafka berjalan pada satu broker dengan replication factor 1.
5. Benchmark awal menggunakan dataset yang sangat kecil, sedangkan eksperimen skew menggunakan dataset buatan.
6. Hasil waktu benchmark bergantung pada perangkat dan kondisi eksekusi.

## 10. Kesimpulan

Proyek ini berhasil membangun pipeline pemrosesan transaksi e-commerce dari ingestion, validasi, event streaming, hingga data warehouse dan analisis penjualan harian.

Pengujian menunjukkan bahwa mekanisme validasi dapat memisahkan data valid dan tidak valid, ingestion kedua tidak menambahkan duplikasi pada sumber statis yang diuji, serta model warehouse berhasil dibangun dan melewati pengujian kualitas data dbt.

Pipeline ini menjadi dasar untuk pengembangan lanjutan seperti integrasi API sesungguhnya, pemantauan pipeline, penanganan late-arriving data, dan penerapan arsitektur Kafka multi-broker.
