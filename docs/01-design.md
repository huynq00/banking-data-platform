# 01 — Tài liệu thiết kế: Banking Data Platform

> Tài liệu này ghi lại **vì sao** hệ thống được thiết kế như hiện tại: từ phân tích yêu cầu, nguồn dữ liệu, đến từng quyết định chọn công cụ và các đánh đổi.

## Mục lục
0. [Tư duy tiếp cận](#0-tư-duy-tiếp-cận)
1. [Phân tích đề bài](#1-phân-tích-đề-bài)
2. [Phân tích nguồn dữ liệu](#2-phân-tích-nguồn-dữ-liệu)
3. [Quyết định kiến trúc](#3-quyết-định-kiến-trúc)
4. [Kiến trúc tổng thể](#4-kiến-trúc-tổng-thể)
5. [Mô hình dữ liệu](#5-mô-hình-dữ-liệu)
6. [Bài toán đặc thù ngân hàng](#6-bài-toán-đặc-thù-ngân-hàng)
7. [Môi trường chạy](#7-môi-trường-chạy)
8. [Lộ trình](#8-lộ-trình)

---

## 0. Tư duy tiếp cận

Mọi project DE lần lượt trả lời 5 câu hỏi. Công cụ chỉ là câu trả lời cho câu hỏi thứ 4:

1. **Ai cần dữ liệu và để làm gì?** (yêu cầu nghiệp vụ)
2. **Dữ liệu nằm ở đâu, trông như thế nào?** (nguồn dữ liệu)
3. **Cần nhanh, đúng, an toàn đến mức nào?** (yêu cầu phi chức năng)
4. **Dùng kiến trúc và công cụ gì để đáp ứng?** (thiết kế)
5. **Làm sao biết pipeline chạy đúng?** (kiểm thử, giám sát)

---

## 1. Phân tích đề bài

### 1.1. Bối cảnh
Ngân hàng giả định **VNBank** có hệ thống Core Banking (database vận hành, nơi app ngân hàng ghi giao dịch). Ngân hàng cần một **nền tảng dữ liệu** phục vụ các phòng ban mà không làm ảnh hưởng tới Core Banking.

### 1.2. Các bên liên quan

| Bộ phận | Câu hỏi cần trả lời | Độ trễ chấp nhận được |
|---|---|---|
| Ban điều hành | Tổng huy động, dư nợ, số khách hàng mới, tỷ lệ nợ xấu (NPL)? | T+1 (sáng hôm sau) |
| Rủi ro / chống gian lận | Có tài khoản nào đang giao dịch bất thường **ngay lúc này**? | Vài giây |
| Kế toán / vận hành | Số liệu cuối ngày có khớp, có mất giao dịch nào không? | Cuối ngày (EOD) |
| Tuân thủ | Ai được xem PII? Thông tin khách hàng thay đổi thế nào theo thời gian? | Bài toán kiểm soát, không phải tốc độ |

### 1.3. Nhận xét then chốt
Có **hai nhóm yêu cầu độ trễ khác nhau**: vài giây (gian lận) và T+1 (báo cáo). Vì vậy kiến trúc có **hai luồng**:
- **Realtime (streaming)** cho chống gian lận.
- **Batch** cho báo cáo và đối soát.

Chỉ có batch thì không bắt được gian lận kịp thời. Làm tất cả bằng streaming thì phức tạp và tốn kém không cần thiết cho báo cáo hằng ngày.

### 1.4. Yêu cầu phi chức năng

| # | Yêu cầu | Cách đáp ứng |
|---|---|---|
| NFR1 | **Đúng tuyệt đối**, lệch 1 đồng là sự cố | Đối soát tự động, sai thì dừng pipeline |
| NFR2 | **Không mất, không trùng** giao dịch | Idempotency, ghi đè theo partition, Kafka offset |
| NFR3 | **Không ảnh hưởng Core Banking** | CDC đọc WAL, không query phân tích vào OLTP |
| NFR4 | **Bảo vệ PII** | Hash/mask ở tầng silver, phân quyền theo role |
| NFR5 | **Lưu lịch sử, truy vết được** | Bronze giữ mọi thay đổi; SCD Type 2 |
| NFR6 | **Tái tạo được môi trường** | Docker Compose, `.env.example`, README |

### 1.5. Ngoài phạm vi
Không làm Kubernetes, cluster nhiều node, xác thực người dùng thật hay high availability. Toàn bộ hệ thống chạy **đơn node trên laptop**.

---

## 2. Phân tích nguồn dữ liệu

### 2.1. Core Banking (PostgreSQL OLTP)

```
customers (1) ──< accounts (n) ──< transactions (n)
     │                 │
     │                 └──< loans (n)
     └── PII: họ tên, CCCD, SĐT, địa chỉ; hạng khách hàng
```

- **Dữ liệu lịch sử:** bộ **Berka** (ngân hàng Séc, dữ liệu thật đã ẩn danh, khoảng 1 triệu giao dịch, 8 bảng quan hệ).
- **Dữ liệu phát sinh:** **script giả lập** liên tục sinh giao dịch mới, thỉnh thoảng cập nhật thông tin khách hàng, và cố tình chèn các mẫu gian lận.
- **Vì sao cần giả lập:** Berka là dữ liệu tĩnh. Không có dữ liệu thay đổi thì CDC và streaming không có gì để xử lý.

### 2.2. Tỷ giá Vietcombank (API bên ngoài)
Dữ liệu thật, cập nhật hằng ngày. Nguồn này có thể lỗi, timeout hoặc đổi định dạng, nên cần retry và lưu bản gốc vào bronze.

### 2.3. Phát hiện khi profiling Berka và cách xử lý

| Phát hiện | Cách xử lý khi nạp |
|---|---|
| Ngày dạng `YYMMDD`, năm 2 chữ số | Tự parse với tiền tố `19`, không để pandas đoán thế kỷ |
| Dữ liệu năm 1993–1998 | Dời **đúng 28 năm** (lịch lặp lại: cùng thứ, cùng năm nhuận, cuối tháng vẫn là cuối tháng); cắt bỏ phần rơi vào tương lai (sau 2026-09-28) |
| `type` có 3 giá trị | `PRIJEM → CREDIT`; `VYDAJ`, `VYBER → DEBIT` |
| 14 giao dịch có số tiền bằng 0 | Loại ra, ghi log số lượng |
| Giá trị trống không nhất quán (`''` và `' '`) | Strip, chuyển thành `NULL` |
| Giao dịch lãi không có `operation` | Thêm kênh `SYSTEM` |
| Người được ủy quyền (DISPONENT) | `accounts.customer_id` = OWNER; người ủy quyền vẫn được nạp vào `customers` nhưng không lưu liên kết (đánh đổi có chủ đích) |
| Giới tính mã hóa trong `birth_number` (tháng + 50 với nữ) | Giải mã khi nạp |
| Không có giờ giao dịch, thứ tự trong ngày không rõ | Dựng lại thứ tự bằng cách nối chuỗi số dư (`số dư trước + số tiền = số dư sau`), sai số cho phép 0,2 CZK do lãi đã làm tròn; khoảng 0,017% giao dịch không nối được |
| Không có PII | Tự sinh họ tên, SĐT, email, địa chỉ tiếng Việt với seed cố định |
| Tiền tệ là CZK | Nhân 1.000 và coi là VND (giả định đơn giản hóa) |
| Khoản vay không có lãi suất và dư nợ | Lãi suất theo kỳ hạn; dư nợ và số ngày quá hạn được ước lượng (dữ liệu tổng hợp) |

### 2.4. Ước lượng dung lượng
Khoảng 5 nghìn khách hàng, 5 nghìn tài khoản, 1 triệu giao dịch lịch sử, cộng vài giao dịch mới mỗi giây. Chừng này đủ lớn để gặp vấn đề hiệu năng thật mà vẫn chạy được trên laptop.

---

## 3. Quyết định kiến trúc

Mỗi quyết định ghi theo mẫu: **vấn đề, các phương án, lựa chọn, cái giá phải trả**.

### ADR-1. Lấy dữ liệu khỏi Core Banking → CDC với Debezium

| Phương án | Nhược điểm |
|---|---|
| Dump toàn bộ bảng mỗi đêm | Nặng cho database, không realtime |
| Query theo `updated_at` | Không bắt được DELETE, mất các thay đổi trung gian, vẫn query vào OLTP |
| **CDC: đọc WAL** ✅ | Cài đặt phức tạp hơn |

**Lý do:** không tạo tải query lên OLTP (NFR3), ghi nhận mọi INSERT, UPDATE, DELETE gần như tức thì, và lưu đủ lịch sử thay đổi (NFR5).

### ADR-2. Message broker → Kafka (chế độ KRaft)
- **Tách rời hệ thống:** bộ xử lý phía sau bị sập thì dữ liệu vẫn nằm an toàn trong Kafka.
- **Nhiều bên cùng đọc:** bộ phát hiện gian lận và bộ ghi data lake đọc cùng một topic, độc lập với nhau (mỗi bên là một consumer group).
- **Đọc lại:** tua offset để xử lý lại khi có bug.
- **Cái giá:** thêm một hệ thống cần vận hành. KRaft giúp bỏ được Zookeeper.

### ADR-3. Data lake → RustFS (S3-compatible) + Parquet, chia partition theo ngày
- **Lưu bản gốc** để tính lại được khi logic biến đổi sai.
- **Object storage tương thích S3:** code chuyển sang AWS S3 gần như không phải sửa.
- **Vì sao RustFS, không phải MinIO:** MinIO từng là lựa chọn phổ biến nhất, nhưng từ năm 2025 đã ngừng phát hành Docker image miễn phí (`minio/minio` không còn trên Docker Hub). RustFS là lựa chọn thay thế mã nguồn mở (Apache 2.0), tương thích S3, có giao diện web và có bản ARM64.
- **Parquet** lưu theo cột, nén tốt, có schema, truy vấn phân tích nhanh hơn nhiều so với CSV/JSON.
- **Partition `date=YYYY-MM-DD/`:** chạy lại một ngày chỉ ghi đè đúng partition đó, đáp ứng idempotency (NFR2).

### ADR-4. Stream processing → Spark Structured Streaming

| Phương án | Ưu | Nhược |
|---|---|---|
| Python consumer tự viết | Đơn giản | Tự xử lý state, window, lỗi |
| Flink | Độ trễ thấp nhất | Khó học, ít xuất hiện trong JD ở Việt Nam |
| **Spark Structured Streaming** ✅ | Một engine cho cả batch và stream; có window, watermark; phổ biến trong JD | Micro-batch, trễ vài giây (vẫn đủ yêu cầu) |

Luật "hơn 5 giao dịch trong 1 phút" cần **window** và **state**, nên phải dùng stream engine thật.

### ADR-5. Warehouse → PostgreSQL, instance riêng
- **Tách khỏi OLTP:** OLTP tối ưu cho ghi nhỏ, nhiều; OLAP tối ưu cho đọc và tổng hợp lớn. Dùng chung thì báo cáo sẽ làm chậm giao dịch.
- **Chọn Postgres:** miễn phí, quen thuộc, đủ cho vài triệu dòng. Ở doanh nghiệp thật sẽ là Snowflake, BigQuery hoặc ClickHouse, và code dbt gần như giữ nguyên khi chuyển.

### ADR-6. Transformation → dbt (ELT)
- **ELT:** nạp dữ liệu thô vào warehouse trước, rồi biến đổi bằng SQL ngay trong đó.
- **dbt mang lại:** quản lý SQL bằng Git, test tự động, lineage, snapshots (SCD2), incremental models, tài liệu tự sinh.

### ADR-7. Orchestration → Airflow
Batch cuối ngày gồm nhiều bước phụ thuộc nhau. Airflow lo thứ tự chạy, retry, backfill và giao diện theo dõi. Dagster và Prefect hiện đại hơn nhưng Airflow là tiêu chuẩn trong JD.

### ADR-8. Các công cụ còn lại
- **Metabase:** dựng dashboard nhanh.
- **Telegram bot:** gửi cảnh báo miễn phí, dễ demo khi phỏng vấn.
- **Docker Compose:** tái tạo môi trường (NFR6).
- **GitHub Actions:** CI chạy lint, test và `dbt build`.

---

## 4. Kiến trúc tổng thể

```
                         ┌──────────────── LUỒNG REALTIME (vài giây) ───────────────┐
Simulator ──► Core Banking ──► Debezium ──► Kafka ──► Spark Streaming ──► Telegram alert
             (Postgres OLTP)                  │        (luật gian lận)   └─► bảng fraud_alerts
                                              │
                                              ▼  Spark ghi Parquet
                                      Lake (RustFS): BRONZE (thô, đủ lịch sử thay đổi)
                         ┌──────────────── LUỒNG BATCH (hằng đêm, Airflow) ─────────┐
API tỷ giá VCB ──────► BRONZE ──► load vào Warehouse ──► dbt: SILVER ──► GOLD ──► Metabase
                                                           │
                                                  Đối soát + data tests ──► lỗi: cảnh báo, dừng
```

### Kiến trúc Medallion

| Tầng | Chứa gì | Mục đích |
|---|---|---|
| **Bronze** | Nguyên bản từ nguồn, kể cả bản ghi lỗi hoặc trùng | Nguồn sự thật, dùng để tính lại |
| **Silver** | Đã làm sạch, loại trùng, chuẩn hóa kiểu, mask PII, SCD2 | Dữ liệu đáng tin, dùng lại được |
| **Gold** | Star schema, bảng KPI | Phục vụ dashboard và người dùng |

---

## 5. Mô hình dữ liệu (tầng Gold)

Khái niệm quan trọng nhất là **grain**: mỗi dòng trong bảng đại diện cho cái gì. Luôn xác định grain trước khi thiết kế bảng.

| Bảng | Loại | Grain (1 dòng = ?) |
|---|---|---|
| `fact_transactions` | Transaction fact | 1 giao dịch |
| `fact_daily_balance` | Periodic snapshot | 1 tài khoản × 1 ngày |
| `fact_loan_daily` | Periodic snapshot | 1 khoản vay × 1 ngày |
| `dim_customer` | SCD Type 2 | 1 phiên bản thông tin khách hàng (`valid_from`, `valid_to`) |
| `dim_account` | Dimension | 1 tài khoản |
| `dim_date` | Dimension | 1 ngày |
| `dim_fx_rate` | Dimension | 1 loại tiền × 1 ngày |

**Ví dụ về SCD2:** khách hàng A lên hạng *Priority* vào tháng 6. Khi phân tích khoản vay A ký vào tháng 3, cần biết **tại thời điểm vay** A thuộc hạng *Standard*. SCD1 (ghi đè) làm mất thông tin này.

---

## 6. Bài toán đặc thù ngân hàng

1. **Đối soát:** với **từng tài khoản**, kiểm tra
   `Số dư cuối ngày = Số dư đầu ngày + Tổng tiền vào − Tổng tiền ra`.
   Lệch thì pipeline dừng, gửi cảnh báo và không cập nhật gold. Thà báo cáo trễ còn hơn báo cáo sai.
2. **Mask PII:** CCCD được hash và chỉ hiển thị 4 số cuối; số điện thoại được mask; phân quyền bằng role trong Postgres.
3. **Idempotency:** mọi job batch nhận tham số ngày chạy (`logical_date`) và ghi đè đúng partition. Chạy lại bao nhiêu lần cũng cho cùng kết quả.
4. **Dữ liệu đến muộn:** streaming dùng **watermark**; batch tính theo **event time** (thời điểm giao dịch), không theo processing time.
5. **Đa tiền tệ:** quy đổi theo tỷ giá của **ngày giao dịch**.

---

## 7. Môi trường chạy

| Thành phần | Giá trị |
|---|---|
| Máy | Apple M1 Pro (ARM64), 8 CPU, 16 GB RAM |
| Docker | 29.x, Compose v5; Docker Desktop cấp khoảng 11.7 GB RAM, 8 CPU |
| Python cho project | **3.11** (trong virtualenv) |

**Lưu ý:**
- **Chip ARM64:** mọi Docker image phải có bản `linux/arm64`. Khi chọn image cần kiểm tra điều này.
- **RAM:** toàn bộ stack cần khoảng 10–12 GB. Dùng **Docker Compose profiles** để chỉ bật phần đang làm (ví dụ `core`, `streaming`, `batch`).
- **Python:** máy đang có 3.14, quá mới nên PySpark, dbt và Airflow chưa hỗ trợ ổn định. Project dùng 3.11 trong virtualenv riêng.

---

## 8. Lộ trình

| Tuần | Nội dung | Phải giải thích được |
|---|---|---|
| 1 | Docker Compose, schema OLTP, load Berka | Container, volume, network; OLTP khác OLAP |
| 2 | Simulator, ingest tỷ giá | Retry, timeout, lưu bản gốc |
| 3 | Debezium + Kafka → data lake | WAL, topic, partition, offset, consumer group; Parquet |
| 4 | dbt silver: làm sạch, mask PII, SCD2 | ELT, snapshot, incremental |
| 5 | dbt gold: star schema, tests | Grain, fact và dimension, periodic snapshot |
| 6 | Airflow EOD DAG + đối soát | DAG, dependency, idempotency, backfill |
| 7 | Spark Streaming phát hiện gian lận | Window, watermark, state, checkpoint |
| 8 | Dashboard, CI, README | Trình bày project khi phỏng vấn |
