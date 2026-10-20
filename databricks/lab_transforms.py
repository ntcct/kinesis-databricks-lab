"""Pure PySpark transforms shared by the notebooks and the pytest suite (no I/O, no Databricks-only APIs)."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("site", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("temperature_c", T.DoubleType()),
    T.StructField("humidity_pct", T.DoubleType()),
    T.StructField("status", T.StringType()),
])
VALID_STATUS = ["OK", "WARN", "FAIL"]
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"  # producer emits ISO-8601 with millis and offset
TEMP_RANGE = (-50.0, 100.0)


def dq_reason(j: Column, ts: Column) -> Column:
    """First failing data-quality rule, or null when the record is valid."""
    return (F.when(j.isNull() | j["event_id"].isNull(), F.lit("malformed_json"))
             .when(j["device_id"].isNull(), F.lit("missing_device_id"))
             .when(ts.isNull(), F.lit("invalid_event_ts"))
             .when(~j["status"].isin(VALID_STATUS), F.lit("invalid_status"))
             .when(~j["temperature_c"].between(*TEMP_RANGE), F.lit("temperature_out_of_range")))


def parse_events(df: DataFrame, body_col: str = "body") -> DataFrame:
    """Parse JSON bodies into typed columns and add `dq_reason` (null = valid). Keeps all input columns."""
    j = F.from_json(F.trim(F.col(body_col)), EVENT_SCHEMA)
    out = df.withColumn("_j", j)
    ts = F.try_to_timestamp(F.col("_j.event_ts"), F.lit(TS_FORMAT))
    return (out.withColumn("_ts", ts)
               .withColumn("dq_reason", dq_reason(F.col("_j"), F.col("_ts")))
               .select("*",
                       F.col("_j.event_id").alias("event_id"),
                       F.col("_j.device_id").alias("device_id"),
                       F.col("_j.site").alias("site"),
                       F.col("_ts").alias("event_ts"),
                       F.col("_j.temperature_c").alias("temperature_c"),
                       F.col("_j.humidity_pct").alias("humidity_pct"),
                       F.col("_j.status").alias("status"))
               .drop("_j", "_ts"))


def valid_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("dq_reason").isNull())


def quarantined(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("dq_reason").isNotNull())


def latest_per_device(df: DataFrame) -> DataFrame:
    """Keep only the most recent event per device (ties broken by event_id for determinism)."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("device_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
