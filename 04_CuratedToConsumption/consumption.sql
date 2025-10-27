CREATE DATABASE IF NOT EXISTS consumption;
USE consumption;

-- dim_campaign
DROP TABLE IF EXISTS dim_campaign;
CREATE TABLE dim_campaign (
  campaign STRING,
  sk INT
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE dim_campaign
SELECT
  campaign,
  ROW_NUMBER() OVER (ORDER BY campaign) AS sk
FROM (
  SELECT DISTINCT campaign
  FROM curated.d_campaign
  WHERE campaign IS NOT NULL AND campaign <> ''
) t;

-- fact_daily_bookings
DROP TABLE IF EXISTS fact_daily_bookings;
CREATE TABLE fact_daily_bookings
STORED AS PARQUET AS
SELECT
  b.event_date       AS dt,
  COUNT(*)           AS total_bookings,
  SUM(b.revenue)     AS total_revenue
FROM curated.f_bookings b
GROUP BY b.event_date;

-- fact_channel_messages
DROP TABLE IF EXISTS fact_channel_messages;
CREATE TABLE fact_channel_messages
STORED AS PARQUET AS
SELECT
  m.message_date     AS dt,
  m.channel,
  m.from_side,
  COUNT(*)           AS total_messages
FROM curated.f_messages m
GROUP BY
  m.message_date,
  m.channel,
  m.from_side;

SHOW TABLES IN consumption;