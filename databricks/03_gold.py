# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: windowed aggregates and latest device state
# MAGIC Both streams use `foreachBatch` + `MERGE`, so gold is idempotent and always current:
# MAGIC * `gold_site_minute` - for each micro-batch, find the (1-minute window, site) keys it touches and
# MAGIC   **recompute those windows from silver**, then upsert. No streaming state or watermark, so late events,
# MAGIC   batch backfills and deletes in silver are all reflected correctly (incremental counts can't subtract
# MAGIC   deleted rows, and drop or double-count rows that arrive via backfill).
# MAGIC * `gold_device_status` - latest event per device, only overwritten by a newer event.

# COMMAND ----------
import sys
ROOT = "/Workspace/Users/{{DATABRICKS_USER}}/kinesis-lab"
sys.path.insert(0, ROOT)
from pyspark.sql import functions as F

T = "workspace.kinesis_lab"
CK = "/Volumes/workspace/kinesis_lab/checkpoints"

# skipChangeCommits: ignore non-append commits on silver (e.g. DELETE/MERGE from maintenance) instead of failing.
# Backfill MERGEs are insert-only (append), so they still flow through and trigger a recompute.
silver = spark.readStream.option("skipChangeCommits", "true").table(f"{T}.silver_events")

# COMMAND ----------
def upsert_site_minute(batch, batch_id):
    import sys
    sys.path.insert(0, ROOT)
    from lab_transforms import site_minute_aggregates, touched_windows
    s = batch.sparkSession
    keys = touched_windows(batch)
    bounds = batch.agg(F.min("event_ts").alias("lo"), F.max("event_ts").alias("hi")).first()
    if bounds["lo"] is None:
        return
    # Time-bounded read of silver so Delta can skip files outside the touched range
    silver_rows = (s.table(f"{T}.silver_events")
                   .filter(F.col("event_ts").between(F.date_trunc("minute", F.lit(bounds["lo"])),
                                                     F.lit(bounds["hi"]) + F.expr("INTERVAL 1 MINUTE"))))
    recomputed = (site_minute_aggregates(silver_rows)
                  .join(keys, ["window_start", "site"], "left_semi")
                  .withColumn("updated_at", F.current_timestamp()))
    recomputed.createOrReplaceTempView("u")
    s.sql(f"""
      MERGE INTO {T}.gold_site_minute t USING u s
      ON t.window_start = s.window_start AND t.site = s.site
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")


# New checkpoint name: the previous version was a stateful aggregation, whose state is incompatible.
# Starting fresh re-reads silver once and recomputes every window, which also repairs earlier drift.
q1 = (silver.writeStream.queryName("gold_site_minute")
      .foreachBatch(upsert_site_minute)
      .option("checkpointLocation", f"{CK}/gold_site_minute_v2")
      .trigger(availableNow=True).start())

# COMMAND ----------
def upsert_device_status(batch, batch_id):
    # foreachBatch runs in a separate server-side process on serverless (Spark Connect),
    # so the shared module must be importable there too
    import sys
    sys.path.insert(0, ROOT)
    from lab_transforms import latest_per_device
    (latest_per_device(batch)
     .select("device_id", "site", F.col("event_ts").alias("last_event_ts"), "temperature_c", "status",
             F.current_timestamp().alias("updated_at"))
     .createOrReplaceTempView("d"))
    batch.sparkSession.sql(f"""
      MERGE INTO {T}.gold_device_status t USING d s ON t.device_id = s.device_id
      WHEN MATCHED AND s.last_event_ts > t.last_event_ts THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")


q2 = (silver.writeStream.queryName("gold_device_status")
      .foreachBatch(upsert_device_status)
      .option("checkpointLocation", f"{CK}/gold_device_status")
      .trigger(availableNow=True).start())

q1.awaitTermination(); q2.awaitTermination()

# COMMAND ----------
display(spark.sql(f"""
  SELECT (SELECT count(*) FROM {T}.gold_site_minute) AS site_minute_rows,
         (SELECT sum(events) FROM {T}.gold_site_minute) AS events_in_windows,
         (SELECT count(*) FROM {T}.gold_device_status) AS devices"""))
