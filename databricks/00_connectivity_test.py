# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Connectivity test
# MAGIC Checks that Unity Catalog credentials can (1) stream from Kinesis via the service credential and
# MAGIC (2) batch-read the Firehose archive via the read-only external location. No access keys are used.

# COMMAND ----------
import json
CATALOG, SCHEMA = "workspace", "kinesis_lab"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints COMMENT 'Structured Streaming checkpoints'")
ckpt = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/_probe"
dbutils.fs.rm(ckpt, True)
probe_table = f"{CATALOG}.{SCHEMA}._probe_kinesis"
spark.sql(f"DROP TABLE IF EXISTS {probe_table}")  # idempotent: a failed earlier run may have left rows behind

# COMMAND ----------
# 1) Kinesis -> Delta, one incremental run (serverless supports availableNow, not fixed-interval triggers)
raw = (spark.readStream.format("kinesis")
       .option("streamName", "lab-events")
       .option("region", "us-east-1")
       .option("initialPosition", "trim_horizon")
       .option("serviceCredential", "kinesis_lab_cred")
       .load())
q = (raw.selectExpr("partitionKey", "sequenceNumber", "approximateArrivalTimestamp", "CAST(data AS STRING) AS body")
     .writeStream.option("checkpointLocation", ckpt)
     .trigger(availableNow=True)
     .toTable(probe_table))
q.awaitTermination()
kinesis_count = spark.table(probe_table).count()
kinesis_distinct = spark.table(probe_table).select("sequenceNumber").distinct().count()
kinesis_cols = raw.columns

# COMMAND ----------
# 2) Batch read of the Firehose archive (GZIP NDJSON) through the external location
archive = (spark.read.option("recursiveFileLookup", "true")  # Firehose writes yyyy/MM/dd/HH/ subfolders
           .json("s3://{{BUCKET_NAME}}/raw/"))
archive_count = archive.count()
archive_cols = archive.columns

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {probe_table}")
dbutils.fs.rm(ckpt, True)
dbutils.notebook.exit(json.dumps({
    "kinesis_records": kinesis_count, "kinesis_distinct_sequence_numbers": kinesis_distinct,
    "kinesis_source_columns": kinesis_cols,
    "archive_records": archive_count, "archive_columns": archive_cols,
}))
