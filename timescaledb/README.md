# TimescaleDB

Thiết lập **TimescaleDB** (PostgreSQL + time-series) để lưu trữ, tính toán average và forecast cho dữ liệu IoT sensor (DEBS 2014).

## Mục tiêu

Mục tiêu chính là **load trước toàn bộ dữ liệu** vào database sao cho:

- Mỗi lần chạy `docker compose up` đều có sẵn dữ liệu giống nhau.
- Tránh phải chạy lại toàn bộ pipeline tính toán (average, forecast) mỗi lần khởi động.

## Cấu trúc thư mục

```
timescaledb/
├── README.md
├── dump/
│   ├── iotdata.dump          # Backup nhị phân (pg_dump -Fc)
│   └── restore.sh            # Script restore khi khởi động container
└── init/
    ├── 01-init.sql            # Tạo extension, tables, hypertables
    ├── 02-load-data.sql       # Import CSV → measurements
    ├── 03-generate-average.sql # Tính plug_average, house_average
    ├── 04-generate-forecast.sql # Tính plug_forecast, house_forecast
    └── 99-finish.sql          # Đánh dấu import hoàn tất
```

## Luồng xử lý tổng quan

```
┌─────────────────────────────────────────────────────────┐
│  Data Preprocess (data-preprocess/)                     │
│                                                         │
│  merge.py  →  house-0-9.csv                             |
|  generate_full_day.py  →  house-0-9_full.csv            |
│  generate_historical_data.py  →                         │
│      house-0-9_full_day-4.csv                           │
│      historical_house-0-9_full_day-1-3.csv              │
└──────────────────────┬──────────────────────────────────┘
                       │  (mounted vào /import)
                       ▼
┌─────────────────────────────────────────────────────────┐
│  timescaledb/init/  (docker-entrypoint-initdb.d)        │
│                                                         │
│  01-init.sql           → Tạo schema (8 tables)          │
│  02-load-data.sql      → COPY CSV → measurements        │
│  03-generate-average.sql → Tính average (8 window)      │
│  04-generate-forecast.sql → Tính forecast (8 window)    │
│  99-finish.sql         → Đánh dấu hoàn tất              │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│  pg_dump  →  iotdata.dump                               │
│  (backup nhị phân, dùng cho các lần khởi động sau)      │
└─────────────────────────────────────────────────────────┘
```

## Bảng tổng quan các bảng dữ liệu

| Bảng | Mô tả | Dữ liệu |
|------|-------|---------|
| `measurements_raw` | UNLOGGED, tạm thời dùng để COPY CSV | Xóa sau 02-load-data.sql |
| `measurements` | Dữ liệu thô (property=1) | Xóa sau 03-generate-average.sql |
| `plug_average` | Trung bình điện năng theo plug (train) | Dữ liệu trước train_end |
| `plug_average_expected` | Trung bình điện năng theo plug (test) | Dữ liệu sau train_end |
| `house_average` | Trung bình điện năng theo house (train) | Tổng plug_average |
| `house_average_expected` | Trung bình điện năng theo house (test) | Tổng plug_average_expected |
| `plug_forecast` | Dự báo theo plug (của Storm) | Được Storm topology ghi |
| `plug_forecast_expected` | Dự báo theo plug (expected) | Tính từ average + median |
| `house_forecast` | Dự báo theo house (của Storm) | Được Storm topology ghi |
| `house_forecast_expected` | Dự báo theo house (expected) | Tính từ average + median |
| `import_status` | Đánh dấu import hoàn tất | Dùng cho healthcheck |

## Chi tiết từng bước init

### 01-init.sql — Khởi tạo schema

- Bật extension `timescaledb`.
- Tạo 8 hypertables (phân partition theo `timestamp` với interval 1 ngày):
  - `plug_average`, `plug_average_expected`
  - `house_average`, `house_average_expected`
  - `plug_forecast`, `plug_forecast_expected`
  - `house_forecast`, `house_forecast_expected`
- Tạo `measurements_raw` (UNLOGGED, tạm thời) và `measurements` (hypertable).

### 02-load-data.sql — Import dữ liệu CSV

- **COPY** 2 file CSV từ `/import/` vào `measurements_raw`:
  - `house-0-9_day-4.csv` — dữ liệu gốc đã dịch timestamp sang ngày 4.
  - `historical_house-0-9_day-1-3.csv` — 3 ngày lịch sử (Day3 → Day1).
- **INSERT** vào `measurements` chỉ các dòng có `property = 1` (power consumption), chuyển timestamp từ `BIGINT` → `TIMESTAMPTZ`.
- **DROP** `measurements_raw` để giải phóng bộ nhớ.

### 03-generate-average.sql — Tính average

- **`train_end()`** = `2013-09-07 00:00:01 UTC` (2013-09-04 + 3 ngày).
  - Dữ liệu **trước** `train_end()` → bảng `_average` (dùng làm historical/train).
  - Dữ liệu **sau** `train_end()` → bảng `_average_expected` (dùng làm test/expected).

- **Plug average**: Dùng `time_bucket()` để aggregation theo 8 window sizes:
  1, 5, 10, 15, 20, 30, 60, 120 phút.

- **House average**: Tổng hợp từ plug_average theo `(window_size, timestamp, house_id)`.

- **Cleanup**: Xóa function `train_end()`, procedures, và bảng `measurements`.

### 04-generate-forecast.sql — Tính forecast expected

Với mỗi window size, tính forecast bằng cách:

1. Lấy **median** của historical plug_average theo `(house_id, household_id, plug_id, slice_time)` — `slice_time` là phần time của timestamp.
2. Forecast = `(average_expected + median_historical) / 2` tại thời điểm `timestamp + 2 × interval`.
3. Tương tự cho house_forecast_expected.

### 99-finish.sql — Đánh dấu hoàn tất

Tạo bảng `import_status` với `finished = TRUE`. Healthcheck của container sẽ chờ giá trị này.

## Sử dụng

### Cách 1: Chạy lần đầu (init từ CSV)

Dùng khi chưa có file `iotdata.dump`, cần chạy toàn bộ pipeline từ CSV.

**Bước 1 — Chuẩn bị dữ liệu**

```bash
cd data-preprocess/

# Ghép 10 file house-0.csv → house-9.csv
python merge.py -n 10

# Sinh 3 ngày lịch sử + 1 ngày gốc (ngày 4)
python generate_historical_data.py \
    --input ../mqtt-publisher/data-file/house-0-9.csv \
    --days 3 \
    --output-dir ./data-file
```

Kết quả trong `data-preprocess/data-file/`:

```
house-0-9_day-4.csv
historical_house-0-9_day-1-3.csv
```

**Bước 2 — Khởi động container với init scripts**

```yaml
# Trong docker-compose.yaml, dùng cấu hình init:
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg17
    container_name: timescaledb
    networks:
      storm_network:
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: iotdata
    ports:
      - 5432:5432
    volumes:
      - ./timescaledb/init:/docker-entrypoint-initdb.d
      - ./data-preprocess/data-file:/import
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "psql -U postgres -d iotdata -tAc \"SELECT finished FROM import_status LIMIT 1\" | grep -q t"
        ]
      interval: 20s
      timeout: 5s
      retries: 60
      start_period: 180s
      start_interval: 180s
```

```bash
docker compose up -d timescaledb
```

**Bước 3 — Chờ hoàn tất**

Healthcheck sẽ kiểm tra liên tục (mỗi 20s, tối đa 60 lần = ~20 phút) cho đến khi `import_status.finished = true`. Toàn bộ quá trình init (import CSV + tính average + forecast) có thể mất **15–30 phút** tùy dữ liệu.

```bash
# Kiểm tra trạng thái
docker compose ps timescaledb

# Xem logs
docker compose logs -f timescaledb
```

**Bước 4 — Dump database**

Sau khi container đã healthy, dump dữ liệu ra file:

```bash
docker exec timescaledb pg_dump -U postgres -d iotdata -Fc -f /tmp/iotdata.dump
docker cp timescaledb:/tmp/iotdata.dump ./timescaledb/dump/iotdata.dump
```

### Cách 2: Chạy từ dump (khuyến nghị cho các lần sau)

Sau khi đã có file `timescaledb/dump/iotdata.dump`, dùng cấu hình này:

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg17
    container_name: timescaledb
    networks:
      storm_network:
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: iotdata
    ports:
      - 5432:5432
    volumes:
      - ./timescaledb/dump/iotdata.dump:/backup/iotdata.dump
      - ./timescaledb/dump/restore.sh:/docker-entrypoint-initdb.d/restore.sh
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "psql -U postgres -d iotdata -tAc \"SELECT EXISTS (SELECT 1 FROM plug_forecast_expected LIMIT 1)\" | grep -q t"
        ]
      interval: 5s
      timeout: 5s
      retries: 20
```

```bash
docker compose up -d timescaledb
```

- `restore.sh` sẽ chạy `pg_restore` tự động khi container khởi động lần đầu.
- Healthcheck kiểm tra `plug_forecast_expected` có dữ liệu (nhanh hơn nhiều so với init).
