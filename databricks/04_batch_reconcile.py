# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Batch reconcile: Firehose S3 archive vs streaming silver
# MAGIC The archive is an independent copy of the stream. This batch job finds events that are in the archive
# MAGIC but missing from silver (e.g. after a bad deploy or checkpoint reset) and backfills them with an
# MAGIC insert-only `MERGE`, so re-running it is safe.
# MAGIC
# MAGIC Widget `simulate_loss` deletes N random silver rows first, to demonstrate recovery.

# COMMAND ----------
import sys, json
sys.path.insert(0, "/Workspace/Users/{{DATABRICKS_USER}}/kinesis-lab")
from pyspark.sql import functions as F
from lab_transforms import parse_events, valid_events

dbutils.widgets.text("simulate_loss", "0")
T = "workspace.kinesis_lab"
RAW = "s3://{{BUCKET_NAME}}/raw/"

# COMMAND ----------
# Text reader handles the GZIP files transparently; one NDJSON record per line
archive = (spark.read.option("recursiveFileLookup", "true").text(RAW)
           .select(F.col("value").alias("body"), F.col("_metadata.file_path").alias("source_file")))
arch_valid = (valid_events(parse_events(archive))
              .dropDuplicates(["event_id"]))
arch_valid.createOrReplaceTempView("archive_valid")

# COMMAND ----------
# Demo only: delete N silver rows that also exist in the archive, so the backfill below should restore exactly N
loss = int(dbutils.widgets.get("simulate_loss"))
if loss > 0:
    # Pick the IDs first: Delta DELETE needs a deterministic condition, so rand() can't be in its subquery
    ids = [r[0] for r in spark.sql(f"SELECT event_id FROM archive_valid ORDER BY rand() LIMIT {loss}").collect()]
    spark.createDataFrame([(i,) for i in ids], "event_id string").createOrReplaceTempView("loss_ids")
    spark.sql(f"DELETE FROM {T}.silver_events WHERE event_id IN (SELECT event_id FROM loss_ids)")

stats = spark.sql(f"""
  SELECT (SELECT count(*) FROM archive_valid) AS archive_valid_distinct,
         (SELECT count(*) FROM archive_valid a LEFT ANTI JOIN {T}.silver_events s USING (event_id)) AS missing_in_silver,
         (SELECT count(*) FROM {T}.silver_events s LEFT ANTI JOIN archive_valid a USING (event_id)) AS only_in_silver
""").first().asDict()

# COMMAND ----------
m = spark.sql(f"""
  MERGE INTO {T}.silver_events t
  USING (SELECT event_id, device_id, site, event_ts, temperature_c, humidity_pct, status,
                CAST(NULL AS TIMESTAMP) AS arrival_ts, CAST(NULL AS STRING) AS sequence_number,
                'batch_backfill' AS source FROM archive_valid) s
  ON t.event_id = s.event_id
  WHEN NOT MATCHED THEN INSERT *""").first().asDict()
stats.update({"simulated_loss": loss, "backfilled": m.get("num_inserted_rows")})
print(stats)
dbutils.notebook.exit(json.dumps(stats))
