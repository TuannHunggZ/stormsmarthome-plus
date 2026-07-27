# DEBS 2014 CSV Merger

Công cụ Python dùng để ghép nhiều file dữ liệu của bộ **DEBS 2014** (`house-*.csv`) thành một file CSV duy nhất, vẫn giữ thứ tự theo `timestamp`.

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

## Cách sử dụng

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

## Tham số

| Tham số | Mô tả |
|---------|------|
| `-n`, `--num-files` | Số lượng file cần ghép (`house-0.csv` đến `house-(n-1).csv`) |
| `-i`, `--input-dir` | Thư mục chứa các file đầu vào (mặc định: `../mqtt-publisher/data-file`) |
| `-o`, `--output` | Tên file đầu ra (mặc định: `merged.csv`) |
