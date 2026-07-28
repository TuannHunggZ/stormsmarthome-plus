# Data Preprocess

Các công cụ Python dùng để xử lý bộ dữ liệu **DEBS 2014** (`house-*.csv`):

| Script | Mô tả |
|--------|------|
| `merge.py` | Ghép nhiều file `house-*.csv` thành một file CSV, giữ thứ tự theo `timestamp` |
| `generate_historical_data.py` | Sinh dữ liệu lịch sử nhiều ngày từ một file `house-*.csv` |

## Yêu cầu

- Python 3.8 trở lên
- Không cần cài thêm thư viện ngoài

## Cấu trúc dữ liệu

```text
data-file/
├── house-0.csv
├── house-1.csv
├── ...
└── house-39.csv
```

Mỗi dòng CSV có dạng:

```text
id,timestamp,value,property,plug_id,household_id,house_id
```

---

## 1. `merge.py`

Ghép nhiều file `house-*.csv` thành một file duy nhất, sắp xếp theo `timestamp`.

### Cách sử dụng

Ghép 5 file đầu tiên (`house-0.csv` → `house-4.csv`):

```bash
python merge.py -n 5
```

Ghép toàn bộ 40 file:

```bash
python merge.py -n 40
```

Chỉ định thư mục chứa dữ liệu:

```bash
python merge.py -n 40 --input-dir ../mqtt-publisher/data-file
```

Chỉ định tên file đầu ra:

```bash
python merge.py -n 40 --output merged.csv
```

Chỉ định cả thư mục dữ liệu và file đầu ra:

```bash
python merge.py -n 40 \
    --input-dir ../mqtt-publisher/data-file \
    --output merged.csv
```

### Tham số

| Tham số | Mô tả |
|---------|------|
| `-n`, `--num-files` | Số lượng file cần ghép (`house-0.csv` đến `house-(n-1).csv`) |
| `-i`, `--input-dir` | Thư mục chứa các file đầu vào (mặc định: `../mqtt-publisher/data-file`) |
| `-o`, `--output` | Tên file đầu ra (mặc định: `merged.csv`) |

---

## 2. `generate_historical_data.py`

Sinh dữ liệu lịch sử từ một file `house-*.csv`.

Với `--days N`, script tạo **2 file** trong `--output-dir`:

1. **Ngày gốc đã dịch timestamp** — `{basename}_day-{N+1}.csv`  
   Timestamp gốc được cộng thêm `N * 86400` giây (đẩy sang ngày `N+1`).

2. **Dữ liệu lịch sử** — `historical_{basename}_day-1-{N}.csv`  
   Sinh `N` ngày trước đó (`DayN` → `Day1`), mỗi giá trị dao động ngẫu nhiên trong khoảng `±2.0` so với ngày gốc (không âm). ID giảm dần từ `max_id - 1`.

### Cách sử dụng

Sinh 1 ngày lịch sử từ `house-0.csv`:

```bash
python generate_historical_data.py \
    --input ../mqtt-publisher/data-file/house-0.csv \
    --days 1 \
    --output-dir ./data-file
```

Kết quả:

```text
data-file/
├── house-0-day-2.csv                 # ngày gốc đã dịch timestamp
└── historical_house-0_day-1-1.csv     # dữ liệu lịch sử Day1
```

Sinh 7 ngày lịch sử:

```bash
python generate_historical_data.py \
    --input ../mqtt-publisher/data-file/house-0.csv \
    --days 7 \
    --output-dir ./data-file
```

Kết quả:

```text
data-file/
├── house-0_day-8.csv                 # ngày gốc đã dịch timestamp
└── historical_house-0_day-1-7.csv     # dữ liệu lịch sử Day7 → Day1
```

### Tham số

| Tham số | Mô tả |
|---------|------|
| `--input` | Đường dẫn file CSV đầu vào (bắt buộc) |
| `--days` | Số ngày lịch sử cần sinh (bắt buộc) |
| `--output-dir` | Thư mục chứa các file đầu ra (bắt buộc) |
