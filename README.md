# project_HDFS — Pipeline Big Data End-to-End bằng VS Code Tasks  
(HDFS → Hive/Spark → Curated → Consumption KPI → Presto)

## Mục tiêu

Pipeline này cho phép chạy full luồng dữ liệu theo kiến trúc data lake / data warehouse gồm 4 tầng rõ ràng:

1. **Landing layer**  
   CSV thô được đưa vào HDFS (`/landing`) và Hive đọc trực tiếp CSV đó qua bảng external `landing.*`.

2. **Source layer**  
   Dữ liệu được chuyển sang Parquet (partition theo ngày), đăng ký thành bảng `source.*` để phân tích.

3. **Curated layer**  
   Làm sạch và chuẩn hoá thành các bảng fact/dim có ý nghĩa kinh doanh (booking, messages, campaign...).

4. **Consumption / KPI layer**  
   Tổng hợp KPI cuối (booking theo ngày, volume tin nhắn theo kênh, v.v.) để đưa thẳng vào báo cáo.  
   Các KPI này được query trực tiếp bằng Presto để chụp màn hình và bỏ vào slide.

Toàn bộ các bước đều được đóng gói thành `Tasks` trong VS Code. Không cần SSH vào container hay gõ lệnh thủ công dài dòng.

---

## 1. Yêu cầu môi trường

- Windows 10/11
- Docker Desktop (WSL2 / Hyper-V)
- VS Code
- PowerShell 7 (khuyến nghị đặt làm default shell trong VS Code)
- Bộ docker compose Hadoop/Hive/Presto đã setup sẵn tại:
  `C:\HadoopDocker\docker-hive`

Cụm dịch vụ chạy bao gồm:
- HDFS: `namenode`, `datanode`
- Hive: `hive-server`, `hive-metastore` (và Postgres metastore)
- Presto: `presto-coordinator`
- YARN stack: `resourcemanager`, `nodemanager`, `historyserver`

> Lưu ý: Các VS Code Task đã được chuẩn hoá để gọi `docker compose -p hadoop ...`,
> nên chỉ cần chạy task là container sẽ lên / chạy query / tạo bảng cho bạn.

---

## 2. Cấu trúc thư mục dự án

```text
project_HDFS/
├─ .vscode/tasks.json                     # Toàn bộ pipeline được định nghĩa ở đây
├─ data/                                  # CSV gốc để upload lên HDFS
├─ 01_ProdToHdfs/
│    └─ ingest_to_hdfs_parquet.py         # (tuỳ chọn) Spark job đẩy dữ liệu vào HDFS
├─ 02_HdfsToSource/
│    ├─ csv_to_parquet.sql                # build parquet từ CSV
│    ├─ create_src_tables.sql             # đăng ký bảng source.*
│    └─ source_from_spark.sql             # đăng ký source.* nếu dùng Spark output
├─ 03_SourceToCurated/
│    └─ curated.sql                       # build curated layer (fact/dim business)
├─ 04_CuratedToConsumption/
│    └─ consumption.sql                   # build consumption / KPI layer
├─ 05_Queries/                            # (tuỳ chọn) câu query demo
└─ out/                                   # (tuỳ chọn) export / screenshot / báo cáo
```

---

## 3. Quy trình chạy (theo thứ tự trong VS Code)

Trong VS Code:  
**Ctrl + Shift + P → "Tasks: Run Task" → chọn task tương ứng.**  
Chạy đúng thứ tự dưới đây để build pipeline end-to-end.

### B0. Khởi động cụm Hadoop / Hive / Presto

1. **Docker: Start Desktop & wait**  
   - Mở Docker Desktop và chờ cho engine sẵn sàng.

2. **Docker: Up (Hadoop+Hive)**  
   - Task này chạy `docker compose -p hadoop up -d` trong thư mục
     `C:\HadoopDocker\docker-hive`.
   - Bật các container core: namenode, datanode, hive-server, metastore, presto-coordinator, v.v.

3. **YARN: Up (RM/NM/HistoryServer)**  
   - Task này đảm bảo 3 service YARN (`resourcemanager`, `nodemanager`, `historyserver`) đang chạy.
   - Cần cho Hive-on-MR và Spark jobs.


---

### B1. Landing layer

**Goal:** CSV thô → HDFS `/landing` → Hive external table `landing.*`

4. **HDFS: mkdir /raw/**
   - Tạo thư mục HDFS `/raw` và `/landing` nếu chưa tồn tại.

5. **HDFS: upload raw CSV**
   - Copy tất cả file CSV từ `project_HDFS/data/*.csv` vào HDFS: `/raw`.

6. **HDFS: move CSV → /landing**
   - Di chuyển dữ liệu từ `/raw/...` sang `/landing/...` để cố định snapshot.
   - `/raw` = staging tạm.  
   - `/landing` = “ảnh chụp gốc” readonly sẽ được Hive đọc.

7. **Hive: Create landing (CSV external)**
   

---

### B2. Source layer

**Goal:** Chuẩn hoá raw CSV thành Parquet, partition theo ngày ⇒ bảng `source.*`

Ở bước này bạn tạo layer phân tích cơ bản từ dữ liệu landing. Có **2 chiến lược**. Bạn chỉ chạy MỘT trong hai.

#### 8a. Nhánh Hive (không dùng Spark)
- **Hive: Build source (Parquet partition y/m/d)**  
  - Từ `/landing/...` (hoặc từ bảng `landing.*`), script tạo các bảng Parquet như:
    - `source.leads_pq`
    - `source.messages_pq`
    - `source.appointments_pq`
    - (nếu có) `source.ads_spend_pq`
  - Các bảng `*_pq` có partition theo `y/m/d` (event_date breakdown), lưu dạng Parquet.


#### Ưu tiên chạy nhánh này
## 8b. Nhánh Spark (dùng Spark để ghi parquet)
1. **Spark: FullLoad → /source parquet**
   - Spark đọc CSV, chuẩn hoá schema và ghi parquet ra `hdfs://namenode:8020/source/...`.
2. **Hive: run Source from Spark**
   - Chạy beeline để CREATE TABLE `source.*` (external/managed) trỏ vào thư mục parquet `/source/...`.


#### Kiểm tra Source layer bằng Presto

- Chạy task **Presto: SHOW TABLES (source)**  
  Task này chạy Presto CLI trong 1 container tạm:
  ```sql
  SHOW TABLES FROM hive.source;
  ```

- Bạn nên thấy danh sách kiểu:
  - `"leads_pq"`
  - `"messages_pq"`
  - `"appointments_pq"`
  - v.v.

- Có thể thêm task Presto đếm thử:
  ```sql
  SELECT COUNT(*) FROM hive.source.messages_pq;
  ```

**Ảnh/screenshot cần giữ cho báo cáo:**
- Kết quả `SHOW TABLES FROM hive.source;`

> Ý nghĩa: Source layer đã chuyển data từ CSV sang parquet partitioned và có thể truy vấn bằng Presto.

---

### B3. Curated layer

**Goal:** Chuẩn hoá thành các fact/dim business (1 dòng = 1 sự kiện kinh doanh)

Curated layer lấy từ `source.*_pq` và sinh ra các bảng rõ nghĩa nghiệp vụ, ví dụ:

- `curated.f_bookings`  
  - booking_id  
  - lead_id  
  - status / service  
  - revenue  
  - booked_ts (timestamp đặt lịch)  
  - event_date (ngày booking)

- `curated.f_messages`  
  - message_date (ngày gửi message)  
  - channel (kênh giao tiếp / ID kênh)  
  - from_side (ID người gửi / phía gửi)  
  - ...

- `curated.d_campaign`  
  - danh sách campaign duy nhất (dimension)

- (tuỳ thêm) `curated.f_interactions`, `curated.f_bookings`, v.v.

#### Cách chạy

1. **Hive: Curated layer (build curated)  **  
   

#### Kiểm tra Curated layer bằng Presto

Sau khi chạy curated:
- Task Presto `SHOW TABLES FROM hive.curated;` để xem danh sách bảng trong `curated`.
- Task Presto `DESCRIBE hive.curated.f_bookings;` để in schema business của bảng fact.
  Ví dụ kết quả mô tả schema sẽ giống:
  ```text
  booking_id        varchar
  lead_id           varchar
  status            varchar
  service           varchar
  revenue           double
  booked_ts         timestamp
  event_date        date
  ```

- Task Presto `SELECT COUNT(*) FROM hive.curated.f_bookings;`
  - Nếu trả `"0"` => bảng tồn tại, query được, nhưng dataset demo hiện không có bản ghi booking (OK cho báo cáo).
---

### B4. Consumption / KPI layer

**Goal:** Tạo các bảng KPI cuối cùng để dùng cho báo cáo nội bộ / dashboard BI.  
Layer này sống trong database `consumption`.

File `04_CuratedToConsumption/consumption.sql` (phiên bản chuẩn chỉnh) làm các việc sau:

> Nếu bạn chưa có `curated.f_ad_spend`, bỏ hẳn block `fact_daily_spend` ra khỏi file để tránh lỗi.

4. Cuối file `consumption.sql` luôn nên có:


#### Cách chạy Consumption layer

1. **Hive: Consumption**  
   - Task copy `04_CuratedToConsumption/consumption.sql`
     vào container:  
     `hive-server:/tmp/consumption.sql`

2. **Hive: run Consumption**  
   - Task chạy beeline với `/tmp/consumption.sql`
   - Hive sẽ:
     - `CREATE DATABASE IF NOT EXISTS consumption;`
     - Tạo/ghi đè `dim_campaign`
     - Tạo/ghi đè `fact_daily_bookings`
     - Tạo/ghi đè `fact_channel_messages`
     - (các bảng cũ như `f_marketing` có thể vẫn tồn tại)
     - In ra `SHOW TABLES IN consumption;`


#### Kiểm tra KPI bằng Presto

Sau khi Consumption layer build xong, chạy task kiểu **Presto: KPI preview (consumption)**.  
Task này gọi Presto CLI và thực hiện chuỗi lệnh như:


Bạn sẽ thấy:

- Danh sách bảng `dim_campaign`, `fact_daily_bookings`, `fact_channel_messages`, …  
- Các dòng KPI, ví dụ:
  - `(dt, total_bookings, total_revenue)` → tổng số booking và doanh thu theo từng ngày.
  - `(dt, channel, from_side, total_messages)` → tổng số tin nhắn theo ngày, theo kênh, theo sender.

Ý nghĩa các cột trong `fact_channel_messages`:
- `dt`: ngày gửi (nếu null trong data gốc thì bạn sẽ thấy ô trống `""`).
- `channel`: mã kênh / mã conversation (ví dụ `27`, `18011`).
- `from_side`: ID người gửi (thường là user_id/lead_id thay vì chuỗi "agent"/"client").
- `total_messages`: tổng số tin nhắn gửi ra trong nhóm đó.

> Đây chính là screenshot KPI cuối dùng để bỏ vào slide “KPI / Báo cáo cuối ngày/tuần”.

---

## 4. (Tuỳ chọn) Kiểm tra layout HDFS để show kiến trúc

Để cho phần Evidence / Health Check trong báo cáo,
có thể chạy lệnh liệt kê HDFS từ `namenode`:

```powershell
docker compose -p hadoop exec namenode bash -lc "/opt/hadoop-2.7.4/bin/hdfs dfs -ls -R /landing /source /user/hive/warehouse | head -n 200"
```

Trong output bạn sẽ thấy 4 lớp vật lý tồn tại trong HDFS:
- `/landing/...`               (CSV external layer)
- `/source/...` hoặc `/user/hive/warehouse/source.db/...`    (Parquet source layer)
- `/user/hive/warehouse/curated.db/...`                      (Curated layer)
- `/user/hive/warehouse/consumption.db/...`                  (Consumption / KPI layer)

Chụp màn hình cái này để chứng minh pipeline không chỉ là SQL logic,
mà thực sự đã ghi dữ liệu thành từng tầng trong HDFS.

---

## 5. Tóm tắt đường chạy end-to-end để demo / báo cáo

Khi đi trình bày / nộp báo cáo, bạn chỉ cần chứng minh 4 bước:

1. **Landing layer**  
   - CSV được upload vào HDFS `/landing`.  
   - Hive tạo các bảng external `landing.*` đọc trực tiếp CSV.  
   - Screenshot: `SHOW TABLES IN landing;` và (tuỳ chọn) `COUNT(*)`.

2. **Source layer**  
   - Dữ liệu được convert sang Parquet partition (bảng `source.*_pq`).  
   - Screenshot: Presto `SHOW TABLES FROM hive.source;`.

3. **Curated layer**  
   - Tạo các fact/dim mang ý nghĩa business (`curated.f_bookings`, `curated.f_messages`, `curated.d_campaign`, ...).  
   - Screenshot:
     - `SHOW TABLES FROM hive.curated;`
     - `DESCRIBE hive.curated.f_bookings;`
     - (tuỳ chọn) `SELECT COUNT(*) FROM hive.curated.f_bookings;`

4. **Consumption / KPI layer**  
   - Tổng hợp KPI cuối cho báo cáo:
     - `consumption.fact_daily_bookings` (KPI booking & revenue theo ngày)
     - `consumption.fact_channel_messages` (KPI volume message theo ngày/kênh/sender)
     - `consumption.dim_campaign`
   - Screenshot:
     - Hive `SHOW TABLES IN consumption;`
     - Presto KPI preview:
       - `SELECT dt, total_bookings, total_revenue FROM hive.consumption.fact_daily_bookings ...`
       - `SELECT dt, channel, from_side, total_messages FROM hive.consumption.fact_channel_messages ...`

> Kết luận: Pipeline chạy full từ CSV thô → HDFS → Parquet source → Curated fact/dim → KPI Consumption.  
> Presto có thể query trực tiếp KPI final để đưa vào dashboard / báo cáo.

---

## 6. Ghi chú thêm / lưu ý

- Các task trong `.vscode/tasks.json` lo hết chuyện:
  - copy file SQL (curated.sql, consumption.sql) vào container `hive-server:/tmp/...`
  - chạy beeline trong container
  - chạy Presto CLI trong container tạm
- `dependsOn` trong tasks.json cho phép bạn chỉ cần chạy 1 task “run X”,
  VS Code sẽ tự động copy file và sau đó thực thi.
- Nếu thấy lỗi Presto kiểu `Table hive.consumption.fact_daily_spend does not exist`:
  - Nghĩa là bạn đang query một bảng KPI (ví dụ spend) mà bạn CHƯA build,
    hoặc đã comment nó khỏi `consumption.sql`. Điều này bình thường.
- Mỗi script curated / consumption đều dùng `DROP TABLE IF EXISTS ...`
  nên bạn có thể rerun để rebuild sạch bảng cùng tên.

---

### Kết luận

Bạn đã có một pipeline data lakehouse mini:
- **Landing:** raw CSV vào HDFS, Hive external đọc trực tiếp.
- **Source:** parquet hoá + partition hoá để tối ưu truy vấn.
- **Curated:** chuẩn hoá business grain thành fact/dim.
- **Consumption:** tổng hợp KPI cuối cùng cho báo cáo và BI.

Tầng cuối cùng (`consumption.*`) đã được query bằng Presto, và trả ra số liệu KPI như:
- tổng booking + revenue theo ngày,
- volume tin nhắn theo kênh và sender.

Đó chính là output business-ready để đưa vào slide KPI / báo cáo cuối cùng.
