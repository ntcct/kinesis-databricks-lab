# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: parse, validate, de-duplicate
# MAGIC * Valid rows -> `silver_events`, de-duplicated on `event_id` with `dropDuplicatesWithinWatermark`.
# MAGIC   The watermark is on **arrival time**, not event time: late events (up to 30 min old) are kept,
# MAGIC   while re-sent duplicates (which arrive close together) are removed, with bounded state.
# MAGIC * Invalid rows -> `silver_quarantine` with the failing rule in `dq_reason`.

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Users/{{DATABRICKS_USER}}/kinesis-lab")
from pyspark.sql import functions as F
from lab_transforms import parse_events, valid_events, quarantined

T = "workspace.kinesis_lab"
CK = "/Volumes/workspace/kinesis_lab/checkpoints"

parsed = parse_events(spark.readStream.table(f"{T}.bronze_events"))

# COMMAND ----------
silver = (valid_events(parsed)
          .withWatermark("arrival_ts", "10 minutes")
          .dropDuplicatesWithinWatermark(["event_id"])
          .select("event_id", "device_id", "site", "event_ts", "temperature_c", "humidity_pct", "status",
                  "arrival_ts", "sequence_number", F.lit("stream").alias("source")))

q1 = (silver.writeStream.queryName("silver_events")
      .option("checkpointLocation", f"{CK}/silver_events")
      .trigger(availableNow=True).toTable(f"{T}.silver_events"))

q2 = (quarantined(parsed)
      .select("body", "dq_reason", "sequence_number", "arrival_ts", "ingest_ts")
      .writeStream.queryName("silver_quarantine")
      .option("checkpointLocation", f"{CK}/silver_quarantine")
      .trigger(availableNow=True).toTable(f"{T}.silver_quarantine"))

q1.awaitTermination(); q2.awaitTermination()

# COMMAND ----------
display(spark.sql(f"""
  SELECT (SELECT count(*) FROM {T}.bronze_events) AS bronze,
         (SELECT count(*) FROM {T}.silver_events) AS silver,
         (SELECT count(DISTINCT event_id) FROM {T}.silver_events) AS silver_distinct,
         (SELECT count(*) FROM {T}.silver_quarantine) AS quarantine"""))
