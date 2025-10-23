CREATE DATABASE IF NOT EXISTS curated; USE curated;

CREATE TABLE IF NOT EXISTS d_campaign (campaign STRING) STORED AS PARQUET;
INSERT OVERWRITE TABLE d_campaign SELECT DISTINCT campaign FROM default.src_leads;

CREATE TABLE IF NOT EXISTS f_interactions STORED AS PARQUET AS
SELECT
  l.lead_id,
  CAST(from_unixtime(unix_timestamp(l.created_at)) AS timestamp) AS created_at_ts,
  l.channel, l.source, l.campaign
FROM default.src_leads l;
