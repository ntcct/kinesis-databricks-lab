-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Setup: tables, comments, tags and grants (idempotent)

-- COMMAND ----------
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS kinesis_lab COMMENT 'Kinesis streaming lab: bronze/silver/gold';
USE SCHEMA kinesis_lab;
CREATE VOLUME IF NOT EXISTS checkpoints COMMENT 'Structured Streaming checkpoints';

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS bronze_events (
  body STRING COMMENT 'Raw Kinesis record payload',
  partition_key STRING, shard_id STRING, sequence_number STRING,
  arrival_ts TIMESTAMP COMMENT 'Kinesis approximate arrival time',
  ingest_ts TIMESTAMP COMMENT 'Time Databricks wrote the row'
) COMMENT 'Bronze: raw Kinesis lab-events, append-only, exactly-once via checkpoint';

CREATE TABLE IF NOT EXISTS silver_events (
  event_id STRING NOT NULL, device_id STRING, site STRING, event_ts TIMESTAMP,
  temperature_c DOUBLE, humidity_pct DOUBLE, status STRING,
  arrival_ts TIMESTAMP, sequence_number STRING,
  source STRING COMMENT 'stream | batch_backfill'
) CLUSTER BY (site) COMMENT 'Silver: validated, typed, de-duplicated events';

CREATE TABLE IF NOT EXISTS silver_quarantine (
  body STRING, dq_reason STRING, sequence_number STRING, arrival_ts TIMESTAMP, ingest_ts TIMESTAMP
) COMMENT 'Records that failed data-quality rules, kept for inspection and replay';

CREATE TABLE IF NOT EXISTS gold_site_minute (
  window_start TIMESTAMP, window_end TIMESTAMP, site STRING,
  events BIGINT, avg_temperature_c DOUBLE, max_temperature_c DOUBLE, avg_humidity_pct DOUBLE,
  warn_count BIGINT, fail_count BIGINT, updated_at TIMESTAMP
) CLUSTER BY (site) COMMENT 'Gold: per-site 1-minute telemetry aggregates (upserted)';

CREATE TABLE IF NOT EXISTS gold_device_status (
  device_id STRING, site STRING, last_event_ts TIMESTAMP, temperature_c DOUBLE, status STRING, updated_at TIMESTAMP
) COMMENT 'Gold: latest known state per device (MERGE upsert)';

-- COMMAND ----------
ALTER TABLE bronze_events SET TAGS ('layer' = 'bronze', 'source' = 'kinesis:lab-events');
ALTER TABLE silver_events SET TAGS ('layer' = 'silver');
ALTER TABLE silver_quarantine SET TAGS ('layer' = 'silver', 'data_quality' = 'quarantine');
ALTER TABLE gold_site_minute SET TAGS ('layer' = 'gold');
ALTER TABLE gold_device_status SET TAGS ('layer' = 'gold');

-- COMMAND ----------
-- Consumers get read access to gold only; bronze and silver stay with the pipeline owner
GRANT USE SCHEMA ON SCHEMA workspace.kinesis_lab TO `account users`;
GRANT SELECT ON TABLE gold_site_minute TO `account users`;
GRANT SELECT ON TABLE gold_device_status TO `account users`;
