# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: windowed aggregates and latest device state
# MAGIC Both streams use `foreachBatch` + `MERGE`, so gold is idempotent and always current:
# MAGIC * `gold_site_minute` - streaming 1-minute windows per site (update mode), upserted on (window_start, site).
# MAGIC   Watermark 35 min on event time covers the producer's late events (up to 30 min).
# MAGIC * `gold_device_status` - latest event per device, only overwritten by a newer event.

# COMMAND ----------
import sys
ROOT = "/Workspace/Users/{{DATABRICKS_USER}}/kinesis-lab"
sys.path.insert(0, ROOT)
from pyspark.sql import functions as F

T = "workspace.kinesis_lab"
CK = "/Volumes/workspace/kinesis_lab/checkpoints"

# skipChangeCommits: ignore non-append commits on silver (e.g. DELETE/MERGE from maintenance) instead of failing
silver = spark.readStream.option("skipChangeCommits", "true").table(f"{T}.silver_events")

# COMMAND ----------
agg = (silver.withWatermark("event_ts", "35 minutes")
       .groupBy(F.window("event_ts", "1 minute"), "site")
       .agg(F.count("*").alias("events"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
            F.sum(F.when(F.col("status") == "WARN", 1).otherwise(0)).alias("warn_count"),
            F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("fail_count"))
       .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*")
       .drop("window"))


def upsert_site_minute(batch, batch_id):
    batch.withColumn("updated_at", F.current_timestamp()).createOrReplaceTempView("u")
    batch.sparkSession.sql(f"""
      MERGE INTO {T}.gold_site_minute t USING u s
      ON t.window_start = s.window_start AND t.site = s.site
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")


q1 = (agg.writeStream.queryName("gold_site_minute").outputMode("update")
      .foreachBatch(upsert_site_minute)
      .option("checkpointLocation", f"{CK}/gold_site_minute")
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
