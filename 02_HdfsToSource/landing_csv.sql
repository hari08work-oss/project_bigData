CREATE DATABASE IF NOT EXISTS landing;

DROP TABLE IF EXISTS landing.leads;
CREATE EXTERNAL TABLE landing.leads(
  lead_id STRING, created_at TIMESTAMP, channel STRING, source STRING, campaign STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'= ',')
STORED AS TEXTFILE
LOCATION '/landing/leads'
TBLPROPERTIES('skip.header.line.count'='1');

DROP TABLE IF EXISTS landing.messages;
CREATE EXTERNAL TABLE landing.messages(
  msg_id STRING, lead_id STRING, channel STRING, msg_ts TIMESTAMP, from_side STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'= ',')
STORED AS TEXTFILE
LOCATION '/landing/messages'
TBLPROPERTIES('skip.header.line.count'='1');

DROP TABLE IF EXISTS landing.appointments;
CREATE EXTERNAL TABLE landing.appointments(
  booking_id STRING, lead_id STRING, booked_ts TIMESTAMP, status STRING, service STRING, revenue DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'= ',')
STORED AS TEXTFILE
LOCATION '/landing/appointments'
TBLPROPERTIES('skip.header.line.count'='1');

DROP TABLE IF EXISTS landing.ads_spend;
CREATE EXTERNAL TABLE landing.ads_spend(
  campaign STRING, dt DATE, spend DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'= ',')
STORED AS TEXTFILE
LOCATION '/landing/ads_spend'
TBLPROPERTIES('skip.header.line.count'='1');
