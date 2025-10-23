# project_HDFS — Chạy HDFS/Hive/Presto bằng VS Code Tasks

> Mục tiêu: upload CSV → dựng **landing** (CSV external) → build **source** (Parquet, partition) bằng **Hive *hoặc* Spark** → kiểm tra nhanh bằng **Presto**. Tất cả thao tác bằng **Tasks** trong VS Code.

---

## 1) Yêu cầu

* Windows 10/11, Docker Desktop (WSL2/Hyper‑V), VS Code.
* Bộ compose đã có tại `C:\HadoopDocker\docker-hive` (namenode, datanode, hive‑server, metastore, postgres, presto…).

> Tip: Nên dùng PowerShell 7 làm shell mặc định của VS Code.

---

## 2) Cấu trúc thư mục

```
project_HDFS/
├─ .vscode/tasks.json           # Toàn bộ quy trình 1‑click
├─ scripts/                     # Script sinh dữ liệu (nếu có)
├─ 01_ProdToHdfs/ingest_to_hdfs_parquet.py
├─ 02_HdfsToSource/
│  ├─ csv_to_parquet.sql
│  ├─ create_src_tables.sql
│  └─ source_from_spark.sql
├─ 03_SourceToCurated/curated.sql
├─ 04_CuratedToConsumption/consumption.sql
├─ 05_Queries/ (nếu dùng)
├─ data/                        # CSV nguồn để upload
└─ out/                         # Nơi lưu report tải về (tuỳ chọn)
```

---

## 3) Chạy đúng thứ tự (Tasks → Run Task)

1. **Docker: Start Desktop & wait**
2. **Docker: Up (Hadoop+Hive)**
3. **YARN: Up (RM/NM/HistoryServer)**
4. **HDFS: mkdir /raw/**
5. **HDFS: upload raw CSV**
   → chép tất cả CSV trong `data/` lên **HDFS:/raw/**
6. **HDFS: move CSV → /landing**
   → di chuyển từ `/raw/*` sang `/landing/*` (tách landing/raw)
7. **Hive: Create landing (CSV external)**
   → tạo bảng `landing.*` đọc CSV ở `/landing/*`

### 8) Chọn *một* nhánh build "source"

**8a. Hive (không dùng Spark)**

* **Hive: Build source (Parquet partition y/m/d)**
  → tạo Parquet: `source.leads_pq`, `source.messages_pq`, `source.appointments_pq`, `source.ads_spend_pq` (partition theo **y/m/d**).

**8b. Spark (nếu muốn)**

* **Spark: FullLoad → /source parquet**
  → Spark ghi Parquet vào **HDFS:/source/***
* **Hive: Source from Spark (copy)**
* **Hive: run Source from Spark**
  → đăng ký bảng Hive trỏ vào **/source/***

### (Tuỳ chọn) các tầng sau

* **Hive: run Curated** – build tầng curated
* **Hive: run Consumption** – build tầng consumption

### Kiểm tra nhanh

* **Presto: SHOW TABLES (source)** – xem bảng trong schema `source`.
* **Presto: COUNT messages_pq** – đếm thử dữ liệu.
* **Open: Web UIs** – mở nhanh:

  * HDFS NameNode: [http://localhost:9870](http://localhost:9870) (hoặc `50070` với Hadoop 2.x)
  * YARN ResourceManager: [http://localhost:8088](http://localhost:8088)
  * Presto: [http://localhost:8080](http://localhost:8080)

---

## 4) Kết quả mong đợi

* **Nhánh Hive (8a):** Parquet nằm tại
  `hdfs://namenode:8020/user/hive/warehouse/source.db/*`
* **Nhánh Spark (8b):** Parquet nằm tại
  `hdfs://namenode:8020/source/*`
* Trên Presto (catalog `hive`, schema `source`):

  * `SHOW TABLES` thấy `leads_pq`, `messages_pq`, `appointments_pq`, `ads_spend_pq`.
  * `SELECT COUNT(*) FROM messages_pq` trả về số **> 0**.

---

## 5) Một số lệnh kiểm tra (chạy qua Docker)

```powershell
# Liệt kê HDFS
docker compose -p hadoop exec namenode bash -lc \
  "/opt/hadoop-2.7.4/bin/hdfs dfs -ls -R /landing /raw | head -n 50"

# Dung lượng thư mục
docker compose -p hadoop exec namenode bash -lc \
  "/opt/hadoop-2.7.4/bin/hdfs dfs -du -h /landing /raw"

# Beeline xem nhanh bảng
docker compose -p hadoop exec hive-server bash -lc \
  "/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e 'show tables;'"
```

---

## 6) Troubleshooting nhanh

* **Không thấy job trên YARN** → bảo đảm đã chạy task **YARN: Up (RM/NM/HistoryServer)**.
* **Presto CLI lỗi tham số** → chạy task có sẵn “Presto: …” trong `tasks.json` (đã cấu hình đúng cho CLI 0.181).
* **NameNode UI không mở** → thử cả **9870** (Hadoop 3.x) và **50070** (Hadoop 2.x).
* **Chạy lại từ đầu**:

  * Với Spark: dùng task **HDFS: rm -r /source (reset Spark output)**.
  * Với Hive: chạy lại task build (các script đã `DROP TABLE IF EXISTS …`).

---

## 7) Ghi chú

* CSV nguồn để trong thư mục **`data/`** của dự án.
* Toàn bộ quy trình đã gói trong **.vscode/tasks.json** – chỉ cần bấm chạy theo thứ tự ở trên.
* Khi dùng nhánh Spark, *đừng quên* hai task **Source from Spark** để Hive nhận partition từ **/source/**.
