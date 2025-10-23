-- 05_Queries/report.sql
-- Truy vấn tổng hợp theo ngày & campaign từ dữ liệu trên HDFS
USE default;

-- (tuỳ chọn) ép 1 reducer để giảm số part files khi ghi ra HDFS
SET mapreduce.job.reduces=1;
SET mapred.reduce.tasks=1;

WITH spend AS (
  SELECT to_date(dt) AS dt, campaign, SUM(spend) AS total_spend
  FROM src_ads_spend
  GROUP BY to_date(dt), campaign
),
leads AS (
  SELECT to_date(created_at) AS dt, campaign, COUNT(*) AS leads
  FROM src_leads
  GROUP BY to_date(created_at), campaign
),
msgs AS (
  SELECT to_date(msg_ts) AS dt, COUNT(*) AS messages
  FROM src_messages
  GROUP BY to_date(msg_ts)
)
-- Ghi KẾT QUẢ ra HDFS (thư mục /reports/daily_summary)
INSERT OVERWRITE DIRECTORY 'hdfs://namenode:8020/reports/daily_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
  s.dt,
  s.campaign,
  s.total_spend,
  COALESCE(l.leads, 0)    AS leads,
  COALESCE(m.messages, 0) AS messages
FROM spend s
LEFT JOIN leads l ON l.dt = s.dt AND l.campaign = s.campaign
LEFT JOIN msgs  m ON m.dt = s.dt
ORDER BY s.dt, s.campaign;
