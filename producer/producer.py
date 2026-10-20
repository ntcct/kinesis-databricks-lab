"""Simulated IoT telemetry producer for Kinesis Data Streams.

Modes:
  normal  - random device_id partition keys, spread evenly across shards
  hot     - 90% of records share one partition key (hot-shard / skew demo)
  burst   - same as normal, used with a high --rate to trigger write throttling

Data quality noise (so the Databricks layers have something to clean):
  ~1% duplicates (same event_id re-sent), ~2% late events (event_ts minutes old),
  ~0.5% malformed JSON.
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

SITES = ["canberra", "melbourne", "sydney", "brisbane", "perth"]
MAX_BATCH = 500  # PutRecords limit


def make_event(now: datetime) -> dict:
    late = random.random() < 0.02
    ts = now - timedelta(minutes=random.randint(5, 30)) if late else now
    device = f"dev-{random.randint(1, 200):04d}"
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device,
        "site": random.choice(SITES),
        "event_ts": ts.isoformat(timespec="milliseconds"),
        "temperature_c": round(random.gauss(22, 4), 2),
        "humidity_pct": round(random.uniform(20, 80), 1),
        "status": random.choices(["OK", "WARN", "FAIL"], [0.95, 0.04, 0.01])[0],
    }


def to_record(event: dict, mode: str) -> dict:
    data = json.dumps(event)
    if random.random() < 0.005:
        data = data[: len(data) // 2]  # truncated JSON -> malformed
    key = "dev-HOT" if mode == "hot" and random.random() < 0.9 else event["device_id"]
    # Trailing newline: Firehose concatenates records as-is, so this makes the S3 files NDJSON
    return {"Data": (data + "\n").encode(), "PartitionKey": key}


def put_with_retry(client, stream: str, records: list, stats: dict) -> None:
    """PutRecords is partially successful: retry only failed entries with backoff."""
    attempt = 0
    while records:
        resp = client.put_records(StreamName=stream, Records=records)
        failed = resp["FailedRecordCount"]
        stats["sent"] += len(records) - failed
        if not failed:
            return
        retry = []
        for rec, res in zip(records, resp["Records"]):
            if "ErrorCode" in res:
                stats["throttled" if res["ErrorCode"] == "ProvisionedThroughputExceededException" else "errors"] += 1
                retry.append(rec)
        records = retry
        attempt += 1
        if attempt > 5:
            stats["dropped"] += len(records)
            return
        time.sleep(min(2 ** attempt * 0.1, 3) + random.random() * 0.1)  # exponential backoff + jitter


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--stream", default="lab-events")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--rate", type=int, default=20, help="records per second")
    p.add_argument("--duration", type=int, default=120, help="seconds")
    p.add_argument("--mode", choices=["normal", "hot", "burst"], default="normal")
    a = p.parse_args()

    client = boto3.client("kinesis", region_name=a.region, config=Config(retries={"mode": "standard"}))
    stats = {"sent": 0, "throttled": 0, "errors": 0, "dropped": 0, "duplicates": 0}
    recent = []
    end = time.time() + a.duration
    print(f"Producing to {a.stream}: mode={a.mode} rate={a.rate}/s duration={a.duration}s")
    while time.time() < end:
        tick = time.time()
        now = datetime.now(timezone.utc)
        batch = []
        for _ in range(a.rate):
            if recent and random.random() < 0.01:
                ev = random.choice(recent)  # duplicate delivery
                stats["duplicates"] += 1
            else:
                ev = make_event(now)
                recent = (recent + [ev])[-100:]
            batch.append(to_record(ev, a.mode))
        for i in range(0, len(batch), MAX_BATCH):
            put_with_retry(client, a.stream, batch[i : i + MAX_BATCH], stats)
        print(json.dumps(stats))
        time.sleep(max(0.0, 1 - (time.time() - tick)))
    print("Final:", json.dumps(stats))


if __name__ == "__main__":
    main()
