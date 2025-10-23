SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS source;

/* leads → parquet */
DROP TABLE IF EXISTS source.leads_pq;
CREATE TABLE source.leads_pq(
  lead_id STRING, created_at TIMESTAMP, channel STRING, source STRING, campaign STRING, event_date DATE
)
PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
TBLPROPERTIES('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE source.leads_pq PARTITION (y,m,d)
SELECT lead_id, created_at, channel, source, campaign,
       DATE(created_at) AS event_date,
       YEAR(DATE(created_at)), MONTH(DATE(created_at)), DAY(DATE(created_at))
FROM landing.leads;

/* messages → parquet */
DROP TABLE IF EXISTS source.messages_pq;
CREATE TABLE source.messages_pq(
  msg_id STRING, lead_id STRING, channel STRING, msg_ts TIMESTAMP, from_side STRING, event_date DATE
)
PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
TBLPROPERTIES('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE source.messages_pq PARTITION (y,m,d)
SELECT msg_id, lead_id, channel, msg_ts, from_side,
       DATE(msg_ts) AS event_date,
       YEAR(DATE(msg_ts)), MONTH(DATE(msg_ts)), DAY(DATE(msg_ts))
FROM landing.messages;

/* appointments → parquet */
DROP TABLE IF EXISTS source.appointments_pq;
CREATE TABLE source.appointments_pq(
  booking_id STRING, lead_id STRING, booked_ts TIMESTAMP, status STRING, service STRING, revenue DOUBLE, event_date DATE
)
PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
TBLPROPERTIES('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE source.appointments_pq PARTITION (y,m,d)
SELECT booking_id, lead_id, booked_ts, status, service, revenue,
       DATE(booked_ts) AS event_date,
       YEAR(DATE(booked_ts)), MONTH(DATE(booked_ts)), DAY(DATE(booked_ts))
FROM landing.appointments;

/* ads_spend → parquet */
DROP TABLE IF EXISTS source.ads_spend_pq;
CREATE TABLE source.ads_spend_pq(
  campaign STRING, dt DATE, spend DOUBLE, event_date DATE
)
PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
TBLPROPERTIES('parquet.compress'='SNAPPY');

INSERT OVERWRITE TABLE source.ads_spend_pq PARTITION (y,m,d)
SELECT campaign, dt, spend,
       dt AS event_date,
       YEAR(dt), MONTH(dt), DAY(dt)
FROM landing.ads_spend;
