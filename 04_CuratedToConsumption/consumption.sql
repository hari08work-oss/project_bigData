CREATE DATABASE IF NOT EXISTS consumption; USE consumption;

CREATE TABLE IF NOT EXISTS dim_campaign (campaign STRING, sk INT) STORED AS PARQUET;
INSERT OVERWRITE TABLE dim_campaign
SELECT campaign, ROW_NUMBER() OVER (ORDER BY campaign) AS sk
FROM (SELECT DISTINCT campaign FROM curated.d_campaign) t;

CREATE TABLE IF NOT EXISTS fact_daily_spend STORED AS PARQUET AS
SELECT a.dt, c.sk AS campaign_sk, SUM(a.spend) AS total_spend
FROM default.src_ads_spend a
JOIN dim_campaign c ON a.campaign=c.campaign
GROUP BY a.dt, c.sk;
