CREATE DATABASE IF NOT EXISTS source;

DROP TABLE IF EXISTS source.leads_src;
CREATE EXTERNAL TABLE source.leads_src(
  lead_id STRING,
  created_at TIMESTAMP,
  channel STRING,
  source STRING,
  campaign STRING,
  event_date DATE
) PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/source/leads';
MSCK REPAIR TABLE source.leads_src;

DROP TABLE IF EXISTS source.messages_src;
CREATE EXTERNAL TABLE source.messages_src(
  msg_id STRING,
  lead_id STRING,
  channel STRING,
  msg_ts TIMESTAMP,
  from_side STRING,
  event_date DATE
) PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/source/messages';
MSCK REPAIR TABLE source.messages_src;

DROP TABLE IF EXISTS source.appointments_src;
CREATE EXTERNAL TABLE source.appointments_src(
  booking_id STRING,
  lead_id STRING,
  booked_ts TIMESTAMP,
  status STRING,
  service STRING,
  revenue DOUBLE,
  event_date DATE
) PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/source/appointments';
MSCK REPAIR TABLE source.appointments_src;

DROP TABLE IF EXISTS source.ads_spend_src;
CREATE EXTERNAL TABLE source.ads_spend_src(
  campaign STRING,
  dt DATE,
  spend DOUBLE,
  event_date DATE
) PARTITIONED BY (y INT, m INT, d INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/source/ads_spend';
MSCK REPAIR TABLE source.ads_spend_src;
