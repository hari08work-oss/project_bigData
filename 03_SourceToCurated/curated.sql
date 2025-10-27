CREATE DATABASE IF NOT EXISTS curated;
USE curated;

-- 1. d_campaign: danh sách campaign từ leads
CREATE TABLE IF NOT EXISTS d_campaign (
  campaign STRING
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE d_campaign
SELECT DISTINCT campaign
FROM source.leads_pq
WHERE campaign IS NOT NULL AND campaign <> '';


-- 2. f_interactions: mỗi lead + metadata cơ bản từ leads_pq
DROP TABLE IF EXISTS f_interactions;
CREATE TABLE f_interactions
STORED AS PARQUET AS
SELECT
  lead_id,
  created_at,     -- cột thật trong leads_pq
  channel,
  source,
  campaign,
  event_date      -- ngày chuẩn hóa trong leads_pq
FROM source.leads_pq;


-- 3. f_bookings: bảng giao dịch/booking/appointment thực tế
DROP TABLE IF EXISTS f_bookings;
CREATE TABLE f_bookings
STORED AS PARQUET AS
SELECT
  booking_id,
  lead_id,
  status,
  service,
  revenue,
  booked_ts,
  event_date      -- ngày thực hiện booking
FROM source.appointments_pq;


-- 4. f_messages: log tương tác tin nhắn
--    dùng đúng schema thật của source.messages_pq
--    (msg_id, lead_id, channel, msg_ts, from_side, event_date, y,m,d)
DROP TABLE IF EXISTS f_messages;
CREATE TABLE f_messages
STORED AS PARQUET AS
SELECT
  msg_id         AS message_id,
  lead_id,
  channel,
  msg_ts         AS message_ts,
  from_side,     -- ai gửi? 'lead' hay 'agent' / 'system' v.v.
  event_date     AS message_date
FROM source.messages_pq;


-- 5. f_ad_spend: chi tiêu marketing
--    (nếu bảng ads_spend_pq khác schema thì bước này có thể fail,
--     khi đó mình sẽ DESCRIBE ads_spend_pq rồi chỉnh lại)
DROP TABLE IF EXISTS f_ad_spend;
CREATE TABLE f_ad_spend
STORED AS PARQUET AS
SELECT
  campaign,
  channel,
  spend_amount,
  currency,
  spend_date_ts,
  dt AS spend_date
FROM source.ads_spend_pq
WHERE spend_amount > 0;