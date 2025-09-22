import asyncio

from influx_mcp.queries import BaseBackend, QueryContext, QueryEngine
from influx_mcp.schemas import QueryTimeseriesRequest, WindowStatsRequest, WritePointRequest


class FakeBackend(BaseBackend):
    mode = "2"
    default_target = "sensors"

    def __init__(self) -> None:
        self.last_write: WritePointRequest | None = None

    async def list_buckets_or_dbs(self):
        return [{"name": "sensors", "type": "bucket", "retention": "infinite"}]

    async def list_measurements(self, target: str):
        assert target == "sensors"
        return [{"name": "env"}]

    async def list_fields(self, target: str, measurement: str):
        return [{"name": "temperature", "type": "float"}]

    async def list_tags(self, target: str, measurement: str):
        return [{"key": "device_id", "values": ["abc"]}]

    async def last_point(self, target: str, measurement: str, field, tags):
        return {
            "time_iso": "2024-01-01T00:00:00+00:00",
            "value": 21.5,
            "field": field or "temperature",
            "tags": tags,
        }

    async def query_timeseries(self, ctx: QueryContext):
        series = [
            {"time_iso": ctx.start_iso, "value": 1.0},
            {"time_iso": ctx.stop_iso, "value": 2.0},
        ]
        stats = {
            "points": len(series),
            "start_eff": ctx.start_iso,
            "stop_eff": ctx.stop_iso,
            "aggregate": ctx.aggregate,
            "every": ctx.every,
        }
        return series, stats

    async def write_point(self, request: WritePointRequest) -> int:
        self.last_write = request
        return 1

    async def discover_resources(self, *, limit: int):
        return [{"uri": "influxdb://sensors/env?start=-1h", "title": "env (1h)"}]

def test_engine_endpoints():
    backend = FakeBackend()
    engine = QueryEngine(backend)

    buckets = asyncio_run(engine.list_buckets_or_dbs())
    assert buckets.items[0].name == "sensors"

    measurements = asyncio_run(engine.list_measurements("sensors"))
    assert measurements.items[0].name == "env"

    fields = asyncio_run(engine.list_fields("sensors", "env"))
    assert fields.items[0].name == "temperature"

    tags = asyncio_run(engine.list_tags("sensors", "env"))
    assert tags.items[0].key == "device_id"

    last = asyncio_run(engine.last_point("sensors", "env", "temperature", {"device_id": "abc"}))
    assert last.value == 21.5

    timeseries_request = QueryTimeseriesRequest(
        target="sensors",
        measurement="env",
        field="temperature",
        start="-1h",
    )
    series = asyncio_run(engine.query_timeseries(timeseries_request))
    assert series.stats.points == 2

    window_response = asyncio_run(
        engine.window_stats(WindowStatsRequest(target="sensors", measurement="env", field="temperature", window="1h"))
    )
    assert window_response.count == 2
    assert abs((window_response.mean or 0) - 1.5) < 1e-6

    write_response = asyncio_run(
        engine.write_point(WritePointRequest(target="sensors", measurement="env", fields={"temperature": 22.0}))
    )
    assert write_response.ok

    resource_text = asyncio_run(engine.read_resource("influxdb://sensors/env?field=temperature&start=-1h"))
    assert "Consulta InfluxDB" in resource_text

    resources = asyncio_run(engine.list_resources(limit=1))
    assert resources.items


def asyncio_run(coro):
    return asyncio.run(coro)
