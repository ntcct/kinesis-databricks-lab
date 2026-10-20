"""Producer tests with a fake Kinesis client (no AWS calls)."""
import json
import random
from datetime import datetime, timezone

import producer


class FakeKinesis:
    """Throttles the first N records of the first call, like a saturated shard."""

    def __init__(self, throttle_first=0):
        self.calls, self.throttle_first = [], throttle_first

    def put_records(self, StreamName, Records):
        self.calls.append(len(Records))
        n = self.throttle_first if len(self.calls) == 1 else 0
        res = [{"ErrorCode": "ProvisionedThroughputExceededException"} if i < n else {"SequenceNumber": str(i)}
               for i in range(len(Records))]
        return {"FailedRecordCount": n, "Records": res}


def new_stats():
    return {"sent": 0, "throttled": 0, "errors": 0, "dropped": 0, "duplicates": 0}


def test_records_are_newline_delimited_for_firehose():
    random.seed(1)
    rec = producer.to_record(producer.make_event(datetime.now(timezone.utc)), "normal")
    assert rec["Data"].endswith(b"\n")


def test_hot_mode_concentrates_partition_keys():
    random.seed(2)
    now = datetime.now(timezone.utc)
    keys = [producer.to_record(producer.make_event(now), "hot")["PartitionKey"] for _ in range(2000)]
    assert 0.85 < keys.count("dev-HOT") / len(keys) < 0.95


def test_event_shape():
    ev = producer.make_event(datetime.now(timezone.utc))
    assert set(ev) == {"event_id", "device_id", "site", "event_ts", "temperature_c", "humidity_pct", "status"}
    json.dumps(ev)


def test_only_failed_records_are_retried(monkeypatch):
    monkeypatch.setattr(producer.time, "sleep", lambda s: None)
    client, stats = FakeKinesis(throttle_first=3), new_stats()
    recs = [{"Data": b"x\n", "PartitionKey": str(i)} for i in range(10)]
    producer.put_with_retry(client, "lab-events", recs, stats)
    assert client.calls == [10, 3]  # second call carries only the 3 throttled records
    assert stats == {**new_stats(), "sent": 10, "throttled": 3}


def test_records_dropped_after_max_retries(monkeypatch):
    monkeypatch.setattr(producer.time, "sleep", lambda s: None)

    class AlwaysThrottle(FakeKinesis):
        def put_records(self, StreamName, Records):
            self.calls.append(len(Records))
            return {"FailedRecordCount": len(Records),
                    "Records": [{"ErrorCode": "ProvisionedThroughputExceededException"}] * len(Records)}

    stats = new_stats()
    producer.put_with_retry(AlwaysThrottle(), "lab-events", [{"Data": b"x", "PartitionKey": "k"}] * 2, stats)
    assert stats["dropped"] == 2 and stats["sent"] == 0
