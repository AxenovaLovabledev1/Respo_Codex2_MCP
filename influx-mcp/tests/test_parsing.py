from datetime import UTC, datetime, timedelta

import pytest

from influx_mcp.utils import build_influx_uri, ensure_limit, make_resource_text, parse_duration, parse_influx_uri, resolve_time_range


def test_parse_influx_uri_basic():
    uri = "influxdb://bucket/measurement?field=temp&start=-1h&aggregate=mean&tag.device=abc"
    parsed = parse_influx_uri(uri)
    assert parsed.target == "bucket"
    assert parsed.measurement == "measurement"
    assert parsed.field == "temp"
    assert parsed.aggregate == "mean"
    assert parsed.tags == {"device": "abc"}
    rebuilt = build_influx_uri(parsed)
    assert rebuilt.startswith("influxdb://bucket/measurement")


def test_resolve_time_range_relative():
    now = datetime(2024, 1, 1, tzinfo=UTC)
    start, stop = resolve_time_range("-1h", None, now=now)
    assert stop == now
    assert stop - start == timedelta(hours=1)


def test_parse_duration_positive_negative():
    assert parse_duration("15m") == timedelta(minutes=15)
    assert parse_duration("-2h") == timedelta(hours=-2)


def test_ensure_limit_cap_warning():
    assert ensure_limit(10, hard_cap=50) == 10
    assert ensure_limit(100, hard_cap=50) == 50
    with pytest.raises(ValueError):
        ensure_limit(0)


def test_make_resource_text_format():
    summary = {"target": "bucket", "measurement": "m", "field": "f", "start": "s", "stop": "e"}
    text = make_resource_text(summary, [{"time_iso": "t", "value": 1}])
    assert "bucket" in text
    assert "time_iso" in text
