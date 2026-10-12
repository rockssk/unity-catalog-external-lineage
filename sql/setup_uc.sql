-- Databricks-side tables. bronze_* are landed by external tools (lineage arrives via OpenLineage);
-- silver/gold are built here, so UC captures that lineage natively.
CREATE SCHEMA IF NOT EXISTS ${catalog}.ext_lineage_demo
  COMMENT 'Demo: external lineage (OpenLineage via Kafka) stitched into Unity Catalog';

CREATE VOLUME IF NOT EXISTS ${catalog}.ext_lineage_demo.landing;

CREATE OR REPLACE TABLE ${catalog}.ext_lineage_demo.bronze_trades (
  trade_id STRING, account_id STRING, fund_id STRING, side STRING, quantity BIGINT, price DOUBLE, trade_ts TIMESTAMP
) COMMENT 'Landed from Kafka topic trades.executions by Airflow';
INSERT INTO ${catalog}.ext_lineage_demo.bronze_trades VALUES
  ('T1','A100','EQIDX','BUY',120,512.4,now()), ('T2','A101','TOTMK','BUY',300,128.9,now()),
  ('T3','A100','BNDAG','SELL',50,9.8,now()),   ('T4','A102','EQIDX','BUY',75,513.1,now());

CREATE OR REPLACE TABLE ${catalog}.ext_lineage_demo.bronze_holdings (
  fund_id STRING, cusip STRING, shares_held BIGINT, as_of_date DATE
) COMMENT 'Landed from Oracle PORTFOLIO.FUND_HOLDINGS by Informatica';
INSERT INTO ${catalog}.ext_lineage_demo.bronze_holdings VALUES
  ('EQIDX','037833100',1500000,current_date()), ('EQIDX','594918104',1200000,current_date()),
  ('TOTMK','037833100',2100000,current_date()), ('BNDAG','912828YK0',9000000,current_date());

CREATE OR REPLACE TABLE ${catalog}.ext_lineage_demo.bronze_prices (
  cusip STRING, close_price DOUBLE, price_date DATE
) COMMENT 'Synced from Snowflake REFDATA.MARKET.SECURITY_PRICES by Airflow';
INSERT INTO ${catalog}.ext_lineage_demo.bronze_prices VALUES
  ('037833100',229.87,current_date()), ('594918104',415.26,current_date()), ('912828YK0',98.42,current_date());

CREATE OR REPLACE TABLE ${catalog}.ext_lineage_demo.silver_positions
COMMENT 'Fund positions = book-of-record holdings + intraday trade flow' AS
SELECT h.fund_id, h.cusip, h.as_of_date,
       h.shares_held + coalesce(sum(CASE WHEN t.side = 'BUY' THEN t.quantity ELSE -t.quantity END), 0) AS shares
FROM ${catalog}.ext_lineage_demo.bronze_holdings h
LEFT JOIN ${catalog}.ext_lineage_demo.bronze_trades t ON h.fund_id = t.fund_id
GROUP BY h.fund_id, h.cusip, h.as_of_date, h.shares_held;

CREATE OR REPLACE TABLE ${catalog}.ext_lineage_demo.gold_fund_nav
COMMENT 'Daily fund NAV / AUM — consumed by Tableau, Power BI and Salesforce' AS
SELECT p.fund_id, p.as_of_date AS nav_date,
       round(sum(p.shares * px.close_price), 2) AS total_aum,
       round(sum(p.shares * px.close_price) / 1e7, 4) AS nav_per_share
FROM ${catalog}.ext_lineage_demo.silver_positions p
JOIN ${catalog}.ext_lineage_demo.bronze_prices px ON p.cusip = px.cusip
GROUP BY p.fund_id, p.as_of_date;

-- raw OpenLineage audit table (written by the Databricks ingest notebook)
CREATE TABLE IF NOT EXISTS ${catalog}.ext_lineage_demo.openlineage_events (
  event_time TIMESTAMP, job STRING, run_id STRING, producer STRING, raw STRING, ingested_at TIMESTAMP
) COMMENT 'Raw OpenLineage RunEvents consumed from Kafka (audit trail)';
