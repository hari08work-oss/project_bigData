-- CSV -> PARQUET (Hive MR trên YARN)
CREATE DATABASE IF NOT EXISTS landing;
USE landing;

CREATE EXTERNAL TABLE IF NOT EXISTS leads_csv(
  lead_id STRING, created_at STRING, channel STRING, source STRING, campaign STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'hdfs://namenode:8020/landing/leads'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS messages_csv(
  msg_id STRING, lead_id STRING, channel STRING, msg_ts STRING, from_side STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'hdfs://namenode:8020/landing/messages'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS appointments_csv(
  booking_id STRING, lead_id STRING, booked_ts STRING, status STRING, service STRING, revenue STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'hdfs://namenode:8020/landing/appointments'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ads_spend_csv(
  campaign STRING, dt STRING, spend DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'hdfs://namenode:8020/landing/ads_spend'
TBLPROPERTIES ('skip.header.line.count'='1');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

USE default;

CREATE EXTERNAL TABLE IF NOT EXISTS src_leads(
  lead_id STRING, created_at STRING, channel STRING, source STRING, campaign STRING
) PARTITIONED BY (dt DATE) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/raw/leads';

INSERT OVERWRITE TABLE src_leads PARTITION (dt)
SELECT lead_id, created_at, channel, source, campaign, to_date(created_at)
FROM landing.leads_csv;

CREATE EXTERNAL TABLE IF NOT EXISTS src_messages(
  msg_id STRING, lead_id STRING, channel STRING, msg_ts STRING, from_side STRING
) PARTITIONED BY (dt DATE) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/raw/messages';

INSERT OVERWRITE TABLE src_messages PARTITION (dt)
SELECT msg_id, lead_id, channel, msg_ts, from_side, to_date(msg_ts)
FROM landing.messages_csv;

CREATE EXTERNAL TABLE IF NOT EXISTS src_appointments(
  booking_id STRING, lead_id STRING, booked_ts STRING, status STRING, service STRING, revenue STRING
) PARTITIONED BY (dt DATE) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/raw/appointments';

INSERT OVERWRITE TABLE src_appointments PARTITION (dt)
SELECT booking_id, lead_id, booked_ts, status, service, revenue, to_date(booked_ts)
FROM landing.appointments_csv;

CREATE EXTERNAL TABLE IF NOT EXISTS src_ads_spend(
  campaign STRING, spend DOUBLE
) PARTITIONED BY (dt DATE) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/raw/ads_spend';

INSERT OVERWRITE TABLE src_ads_spend PARTITION (dt)
SELECT campaign, spend, to_date(dt) FROM landing.ads_spend_csv;

MSCK REPAIR TABLE src_leads;
MSCK REPAIR TABLE src_messages;
MSCK REPAIR TABLE src_appointments;
MSCK REPAIR TABLE src_ads_spend;
