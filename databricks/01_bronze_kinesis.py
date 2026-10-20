# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Kinesis `lab-events` -> `bronze_events`
# MAGIC Incremental run (`availableNow`): reads everything since the last checkpoint, then stops, so
# MAGIC you pay only while it runs. The checkpoint stores per-shard sequence numbers, giving exactly-once into Delta.

# COMMAND ----------
import json
from pyspark.sql import functions as F

dbutils.widgets.text("stream_name", "lab-events")
dbutils.widgets.text("region", "us-east-1")
dbutils.widgets.text("credential", "kinesis_lab_cred")
T = "workspace.kinesis_lab"
CKPT = "/Volumes/workspace/kinesis_lab/checkpoints/bronze_events"

# COMMAND ----------
run_start = spark.sql("SELECT current_timestamp() AS t").first()["t"]  # rows written by this run have ingest_ts >= run_start

raw = (spark.readStream.format("kinesis")
       .option("streamName", dbutils.widgets.get("stream_name"))
       .option("region", dbutils.widgets.get("region"))
       .option("serviceCredential", dbutils.widgets.get("credential"))  # Unity Catalog service credential, no keys
       .option("initialPosition", "trim_horizon")  # first run only; later runs resume from the checkpoint
       .option("maxRecordsPerFetch", 10000)
       .load())

bronze = raw.select(
    F.col("data").cast("string").alias("body"),
    F.col("partitionKey").alias("partition_key"),
    F.col("shardId").alias("shard_id"),
    F.col("sequenceNumber").alias("sequence_number"),
    F.col("approximateArrivalTimestamp").alias("arrival_ts"),
    F.current_timestamp().alias("ingest_ts"),
)

q = (bronze.writeStream
     .queryName("bronze_kinesis")
     .option("checkpointLocation", CKPT)
     .trigger(availableNow=True)
     .toTable(f"{T}.bronze_events"))
q.awaitTermination()

# COMMAND ----------
p = json.loads(q.lastProgress.json) if q.lastProgress is not None and hasattr(q.lastProgress, "json") else (q.lastProgress or {})
print({"batch": p.get("batchId"), "input_rows": p.get("numInputRows"), "total_bronze": spark.table(f"{T}.bronze_events").count()})

# COMMAND ----------
# MAGIC %md
# MAGIC ### Publish pipeline health to CloudWatch (namespace `KinesisLab/Pipeline`)
# MAGIC Uses the same Unity Catalog service credential; the IAM role may only write to this namespace.

# COMMAND ----------
import boto3, json
from datetime import datetime, timezone

# Rows: counted from the table, which is reliable even if progress events are missing
# (progress reporting under Spark Connect returned 0 rows for a run that did write data)
rows = spark.table(f"{T}.bronze_events").filter(F.col("ingest_ts") >= F.lit(run_start)).count()
# Lag: Kinesis source metrics from streaming progress (serverless returns progress objects, so convert to dicts)
progress = [json.loads(pr.json) if hasattr(pr, "json") else dict(pr) for pr in (q.recentProgress or [])]
kin = [s.get("metrics") or {} for pr in progress for s in (pr.get("sources") or [])]
print({"progress_events": len(progress), "source_metric_keys": sorted({k for m in kin for k in m})})
ms_behind = max([float(m.get("maxMsBehindLatest") or 0) for m in kin] or [0.0])
last_arrival = spark.table(f"{T}.bronze_events").agg(F.max("arrival_ts")).first()[0]
freshness = (datetime.now(timezone.utc) - last_arrival.replace(tzinfo=timezone.utc)).total_seconds() if last_arrival else 0.0

session = boto3.Session(botocore_session=dbutils.credentials.getServiceCredentialsProvider(dbutils.widgets.get("credential")),
                        region_name=dbutils.widgets.get("region"))
dims = [{"Name": "Pipeline", "Value": "medallion"}, {"Name": "Stage", "Value": "bronze"}]
session.client("cloudwatch").put_metric_data(Namespace="KinesisLab/Pipeline", MetricData=[
    {"MetricName": "RowsIngested", "Dimensions": dims, "Value": rows, "Unit": "Count"},
    {"MetricName": "MaxMsBehindLatest", "Dimensions": dims, "Value": ms_behind, "Unit": "Milliseconds"},
    {"MetricName": "DataFreshnessSeconds", "Dimensions": dims, "Value": freshness, "Unit": "Seconds"},
])
print({"published": {"RowsIngested": rows, "MaxMsBehindLatest": ms_behind, "DataFreshnessSeconds": round(freshness, 1)}})
