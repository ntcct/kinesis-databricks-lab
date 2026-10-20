-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Table maintenance
-- MAGIC Streaming writes many small files. OPTIMIZE compacts them (and applies liquid clustering on `site`).

-- COMMAND ----------
USE workspace.kinesis_lab;
OPTIMIZE bronze_events;
OPTIMIZE silver_events;
OPTIMIZE gold_site_minute;
OPTIMIZE gold_device_status;

-- COMMAND ----------
-- File count and size after compaction (DESCRIBE DETAIL can't be used as a subquery, so one per cell)
DESCRIBE DETAIL silver_events;
