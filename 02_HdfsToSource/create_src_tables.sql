CREATE DATABASE IF NOT EXISTS default; USE default;

CREATE EXTERNAL TABLE IF NOT EXISTS src_leads(
  lead_id STRING, created_at STRING, channel STRING, source STRING, campaign STRING
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/raw/leads';

CREATE EXTERNAL TABLE IF NOT EXISTS src_messages(
  msg_id STRING, lead_id STRING, channel STRING, msg_ts STRING, from_side STRING
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/raw/messages';

CREATE EXTERNAL TABLE IF NOT EXISTS src_appointments(
  booking_id STRING, lead_id STRING, booked_ts STRING, status STRING, service STRING, revenue STRING
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/raw/appointments';

CREATE EXTERNAL TABLE IF NOT EXISTS src_ads_spend(
  campaign STRING, dt DATE, spend DOUBLE
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/raw/ads_spend';

MSCK REPAIR TABLE src_leads;
MSCK REPAIR TABLE src_messages;
MSCK REPAIR TABLE src_appointments;
MSCK REPAIR TABLE src_ads_spend;
