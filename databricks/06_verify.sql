-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 - Verify: pipeline consistency checks (every row should show ok = true)
-- MAGIC Single statement so `run.ps1 verify` can also run it through the SQL Statement API.

-- COMMAND ----------
WITH s AS (SELECT date_trunc('minute', event_ts) ws, site, count(*) n
           FROM workspace.kinesis_lab.silver_events GROUP BY 1, 2),
mismatch AS (
  SELECT count(*) c FROM workspace.kinesis_lab.gold_site_minute g FULL JOIN s ON g.window_start = s.ws AND g.site = s.site
  WHERE coalesce(g.events, 0) <> coalesce(s.n, 0))
SELECT 'bronze sequence numbers unique' AS check_name,
       (SELECT count(*) = count(DISTINCT sequence_number) FROM workspace.kinesis_lab.bronze_events) AS ok
UNION ALL SELECT 'silver event_id unique',
       (SELECT count(*) = count(DISTINCT event_id) FROM workspace.kinesis_lab.silver_events)
UNION ALL SELECT 'gold windows match silver per (window, site)', (SELECT c = 0 FROM mismatch)
UNION ALL SELECT 'gold total = silver total',
       (SELECT sum(events) FROM workspace.kinesis_lab.gold_site_minute) = (SELECT count(*) FROM workspace.kinesis_lab.silver_events)
UNION ALL SELECT 'every device in silver has a gold status',
       (SELECT count(DISTINCT device_id) FROM workspace.kinesis_lab.silver_events) = (SELECT count(*) FROM workspace.kinesis_lab.gold_device_status)
