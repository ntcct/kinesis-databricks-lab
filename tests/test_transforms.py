"""Unit tests for databricks/lab_transforms.py, run on a local Spark session (same Spark version as the workspace)."""
import json
from datetime import datetime

import pytest

from lab_transforms import latest_per_device, parse_events, quarantined, valid_events

GOOD = {"event_id": "e1", "device_id": "dev-0001", "site": "canberra", "event_ts": "2026-10-20T04:16:27.030+00:00",
        "temperature_c": 21.5, "humidity_pct": 40.0, "status": "OK"}


def bodies(spark, *payloads):
    rows = [(p if isinstance(p, str) else json.dumps(p),) for p in payloads]
    return spark.createDataFrame(rows, "body string")


def reason(spark, payload):
    return parse_events(bodies(spark, payload)).first()["dq_reason"]


def test_valid_event_is_typed(spark):
    row = parse_events(bodies(spark, GOOD)).first()
    assert row["dq_reason"] is None
    assert row["event_ts"] == datetime(2026, 10, 20, 4, 16, 27, 30000)
    assert row["temperature_c"] == 21.5 and row["status"] == "OK"


def test_trailing_newline_from_producer_is_accepted(spark):
    assert reason(spark, json.dumps(GOOD) + "\n") is None


def test_input_columns_are_preserved(spark):
    df = spark.createDataFrame([(json.dumps(GOOD), "49679-seq")], "body string, sequence_number string")
    assert parse_events(df).first()["sequence_number"] == "49679-seq"


@pytest.mark.parametrize("payload, expected", [
    ('{"event_id": "e1", "device_id": "dev-0', "malformed_json"),          # truncated, as the producer emits
    ("not json at all", "malformed_json"),
    ({**GOOD, "event_id": None}, "malformed_json"),
    ({**GOOD, "device_id": None}, "missing_device_id"),
    ({**GOOD, "event_ts": "20/10/2026 04:16"}, "invalid_event_ts"),
    ({**GOOD, "status": "BROKEN"}, "invalid_status"),
    ({**GOOD, "temperature_c": 150.0}, "temperature_out_of_range"),
    ({**GOOD, "temperature_c": -60.0}, "temperature_out_of_range"),
])
def test_quarantine_rules(spark, payload, expected):
    assert reason(spark, payload) == expected


def test_late_event_is_still_valid(spark):
    # Silver de-duplicates on arrival time, so an old event_ts must not be rejected
    assert reason(spark, {**GOOD, "event_ts": "2026-10-20T03:46:27.030+00:00"}) is None


def test_valid_and_quarantine_split_is_complete(spark):
    parsed = parse_events(bodies(spark, GOOD, {**GOOD, "event_id": "e2"}, "{bad", {**GOOD, "status": "X"}))
    assert valid_events(parsed).count() == 2
    assert quarantined(parsed).count() == 2
    assert valid_events(parsed).count() + quarantined(parsed).count() == parsed.count()


def test_latest_per_device_keeps_newest_and_breaks_ties(spark):
    df = parse_events(bodies(
        spark,
        {**GOOD, "event_id": "a", "event_ts": "2026-10-20T04:00:00.000+00:00", "status": "OK"},
        {**GOOD, "event_id": "b", "event_ts": "2026-10-20T04:05:00.000+00:00", "status": "FAIL"},
        {**GOOD, "event_id": "c", "device_id": "dev-0002", "event_ts": "2026-10-20T04:01:00.000+00:00"},
        {**GOOD, "event_id": "d", "device_id": "dev-0002", "event_ts": "2026-10-20T04:01:00.000+00:00"},
    ))
    got = {r["device_id"]: (r["event_id"], r["status"]) for r in latest_per_device(valid_events(df)).collect()}
    assert got == {"dev-0001": ("b", "FAIL"), "dev-0002": ("d", "OK")}


def test_site_minute_aggregates_and_touched_windows(spark):
    from lab_transforms import site_minute_aggregates, touched_windows
    df = valid_events(parse_events(bodies(
        spark,
        {**GOOD, "event_id": "a", "event_ts": "2026-10-20T04:00:05.000+00:00", "temperature_c": 20.0, "status": "OK"},
        {**GOOD, "event_id": "b", "event_ts": "2026-10-20T04:00:55.000+00:00", "temperature_c": 30.0, "status": "WARN"},
        {**GOOD, "event_id": "c", "event_ts": "2026-10-20T04:01:00.000+00:00", "temperature_c": 25.0, "status": "FAIL"},
        {**GOOD, "event_id": "d", "site": "perth", "event_ts": "2026-10-20T04:00:10.000+00:00"},
    )))
    rows = {(r["window_start"].strftime("%H:%M"), r["site"]): r for r in site_minute_aggregates(df).collect()}
    w = rows[("04:00", "canberra")]
    assert (w["events"], w["avg_temperature_c"], w["max_temperature_c"], w["warn_count"], w["fail_count"]) == (2, 25.0, 30.0, 1, 0)
    assert w["window_end"].strftime("%H:%M") == "04:01"
    assert rows[("04:01", "canberra")]["fail_count"] == 1
    assert set(rows) == {(r["window_start"].strftime("%H:%M"), r["site"]) for r in touched_windows(df).collect()}
