"""InfluxDB client abstractions with auto-detection for v1/v2."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlparse

from .config import AppConfig, ConfigError, InfluxV1Settings, InfluxV2Settings
from .queries import (
    BaseBackend,
    QueryContext,
    QueryEngine,
    build_flux_fields_query,
    build_flux_last_point_query,
    build_flux_measurements_query,
    build_flux_tag_keys_query,
    build_flux_tag_values_query,
    build_flux_timeseries_query,
    build_influxql_field_keys_query,
    build_influxql_last_point_query,
    build_influxql_measurements_query,
    build_influxql_tag_keys_query,
    build_influxql_timeseries_query,
)
from .schemas import WritePointRequest

try:  # pragma: no cover - optional dependency
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.exceptions import InfluxDBError
    from influxdb_client.client.write_api import SYNCHRONOUS as WRITE_SYNCHRONOUS
except Exception:  # pragma: no cover - fallback when dependency missing
    InfluxDBClient = None  # type: ignore[assignment]
    InfluxDBError = Exception  # type: ignore[assignment]
    WRITE_SYNCHRONOUS = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from influxdb import InfluxDBClient as InfluxDBClientV1
except Exception:  # pragma: no cover - fallback when dependency missing
    InfluxDBClientV1 = None  # type: ignore[assignment]


_LOGGER = logging.getLogger(__name__)


class InfluxClientError(RuntimeError):
    """Base error for MCP client failures."""


class InfluxConnectionError(InfluxClientError):
    """Raised when the server cannot reach the Influx endpoint."""


class InfluxClient:
    """Facade that exposes the MCP query engine."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._mode = self._detect_mode(config)
        if self._mode == "2":
            settings = config.v2
            if not settings:
                raise ConfigError("InfluxDB v2 settings missing")
            backend = _InfluxV2Backend(settings, timeout=config.request_timeout_sec)
        else:
            settings = config.v1
            if not settings:
                raise ConfigError("InfluxDB v1 settings missing")
            backend = _InfluxV1Backend(settings, timeout=config.request_timeout_sec)
        self.engine = QueryEngine(backend)

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def default_target(self) -> Optional[str]:
        return self.engine.default_target

    def close(self) -> None:
        backend = self.engine._backend  # noqa: SLF001 - internal wiring
        close_method = getattr(backend, "close", None)
        if close_method:
            close_method()

    def _detect_mode(self, config: AppConfig) -> str:
        if config.influx_version in {"1", "2"}:
            _LOGGER.info("Using configured InfluxDB version %s", config.influx_version)
            return config.influx_version
        errors: List[str] = []
        if config.v2:
            try:
                if _InfluxV2Backend.ping(config.v2, timeout=config.request_timeout_sec):
                    _LOGGER.info("Detected InfluxDB v2 endpoint at %s", config.v2.url)
                    return "2"
            except Exception as exc:  # pragma: no cover - network dependent
                errors.append(f"v2 detection failed: {exc}")
        if config.v1:
            try:
                if _InfluxV1Backend.ping(config.v1, timeout=config.request_timeout_sec):
                    _LOGGER.info("Detected InfluxDB v1 endpoint at %s", config.v1.url)
                    return "1"
            except Exception as exc:  # pragma: no cover - network dependent
                errors.append(f"v1 detection failed: {exc}")
        if errors:
            _LOGGER.warning("; ".join(errors))
        raise InfluxConnectionError("Could not detect InfluxDB version; set INFLUX_VERSION explicitly")


@dataclass
class _InfluxV2Backend(BaseBackend):
    settings: InfluxV2Settings
    timeout: int

    mode: str = "2"

    def __post_init__(self) -> None:
        if InfluxDBClient is None:  # pragma: no cover - import guard
            raise ImportError("influxdb-client package not installed")
        self._client = InfluxDBClient(
            url=self.settings.url,
            token=self.settings.token,
            org=self.settings.org,
            timeout=self.timeout * 1000,
        )
        self._query = self._client.query_api()
        self._write = self._client.write_api(write_options=WRITE_SYNCHRONOUS) if WRITE_SYNCHRONOUS else None

    @property
    def default_target(self) -> Optional[str]:
        return self.settings.default_bucket

    def close(self) -> None:  # pragma: no cover - trivial
        self._client.close()

    @staticmethod
    def ping(settings: InfluxV2Settings, *, timeout: int) -> bool:
        if InfluxDBClient is None:  # pragma: no cover - import guard
            raise ImportError("influxdb-client package not installed")
        client = InfluxDBClient(url=settings.url, token=settings.token, org=settings.org, timeout=timeout * 1000)
        try:
            health = client.health()
            return getattr(health, "status", "") == "pass"
        finally:
            client.close()

    async def list_buckets_or_dbs(self) -> List[Mapping[str, str]]:
        api = self._client.buckets_api()
        buckets = api.find_buckets().buckets or []
        items: List[Mapping[str, str]] = []
        for bucket in buckets:
            retention = None
            if getattr(bucket, "retention_rules", None):
                every = bucket.retention_rules[0].every_seconds
                retention = "infinite" if every in (None, 0) else f"{every}s"
            items.append({"name": bucket.name, "type": "bucket", "retention": retention})
        return items

    async def list_measurements(self, target: str) -> List[Mapping[str, str]]:
        query = build_flux_measurements_query(target)
        records = self._query.query(query, org=self.settings.org)
        results: List[Mapping[str, str]] = []
        for table in records:
            for record in table.records:
                results.append({"name": record.get_value()})
        return results

    async def list_fields(self, target: str, measurement: str) -> List[Mapping[str, str]]:
        query = build_flux_fields_query(target, measurement)
        tables = self._query.query(query, org=self.settings.org)
        items: List[Mapping[str, str]] = []
        for table in tables:
            for record in table.records:
                values = record.values
                items.append({"name": values.get("_value"), "type": values.get("_fieldType")})
        return items

    async def list_tags(self, target: str, measurement: str) -> List[Mapping[str, Any]]:
        tag_keys_query = build_flux_tag_keys_query(target, measurement)
        tables = self._query.query(tag_keys_query, org=self.settings.org)
        tag_keys = []
        for table in tables:
            for record in table.records:
                tag_key = record.get_value()
                tag_keys.append(tag_key)
        results: List[Mapping[str, Any]] = []
        for key in tag_keys:
            values_query = build_flux_tag_values_query(target, measurement, key)
            value_tables = self._query.query(values_query, org=self.settings.org)
            values: List[str] = []
            for value_table in value_tables:
                for record in value_table.records:
                    values.append(str(record.get_value()))
            results.append({"key": key, "values": values})
        return results

    async def last_point(
        self, target: str, measurement: str, field: Optional[str], tags: Mapping[str, str]
    ) -> Mapping[str, Any]:
        query = build_flux_last_point_query(target, measurement, field, tags)
        tables = self._query.query(query, org=self.settings.org)
        for table in tables:
            for record in table.records:
                values = record.values
                time_iso = record.get_time().astimezone().isoformat()
                tag_values = {k: v for k, v in values.items() if k not in {"_time", "_value", "_field", "_measurement"}}
                return {
                    "time_iso": time_iso,
                    "value": record.get_value(),
                    "field": values.get("_field"),
                    "tags": tag_values,
                }
        raise InfluxClientError("No data available for last_point query")

    async def query_timeseries(self, ctx: QueryContext) -> tuple[List[Mapping[str, Any]], Mapping[str, Any]]:
        query = build_flux_timeseries_query(ctx)
        tables = self._query.query(query, org=self.settings.org)
        series: List[Mapping[str, Any]] = []
        for table in tables:
            for record in table.records:
                time_iso = record.get_time().astimezone().isoformat()
                series.append({"time_iso": time_iso, "value": record.get_value()})
        stats = {
            "points": len(series),
            "start_eff": ctx.start_iso,
            "stop_eff": ctx.stop_iso,
            "aggregate": ctx.aggregate,
            "every": ctx.every,
        }
        return series, stats

    async def write_point(self, request: WritePointRequest) -> int:
        if not self._write:
            raise InfluxClientError("Write API not available without influxdb-client write options")
        record = {
            "measurement": request.measurement,
            "tags": dict(request.tags or {}),
            "fields": {key: value for key, value in request.fields.items() if value is not None},
        }
        if request.time_iso:
            record["time"] = request.time_iso
        self._write.write(bucket=request.target, record=record)
        return 1

    async def discover_resources(self, *, limit: int) -> List[Mapping[str, str]]:
        bucket = self.settings.default_bucket
        if not bucket:
            return []
        measurements = await self.list_measurements(bucket)
        resources: List[Mapping[str, str]] = []
        for measurement in measurements[:limit]:
            uri = f"influxdb://{bucket}/{measurement['name']}?start=-1h"
            resources.append({"uri": uri, "title": f"{measurement['name']} (última hora)"})
        return resources


@dataclass
class _InfluxV1Backend(BaseBackend):
    settings: InfluxV1Settings
    timeout: int

    mode: str = "1"

    def __post_init__(self) -> None:
        if InfluxDBClientV1 is None:  # pragma: no cover - import guard
            raise ImportError("influxdb package not installed")
        parsed = urlparse(self.settings.url)
        ssl = parsed.scheme == "https"
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if ssl else 80)
        self._client = InfluxDBClientV1(
            host=host,
            port=port,
            username=self.settings.username,
            password=self.settings.password,
            database=self.settings.default_db,
            ssl=ssl,
            timeout=self.timeout,
        )

    @property
    def default_target(self) -> Optional[str]:
        if self.settings.default_db and self.settings.default_rp:
            return f"{self.settings.default_db}/{self.settings.default_rp}"
        return self.settings.default_db

    def close(self) -> None:  # pragma: no cover - trivial
        self._client.close()

    @staticmethod
    def ping(settings: InfluxV1Settings, *, timeout: int) -> bool:
        if InfluxDBClientV1 is None:  # pragma: no cover - import guard
            raise ImportError("influxdb package not installed")
        parsed = urlparse(settings.url)
        ssl = parsed.scheme == "https"
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if ssl else 80)
        client = InfluxDBClientV1(
            host=host,
            port=port,
            username=settings.username,
            password=settings.password,
            database=settings.default_db,
            ssl=ssl,
            timeout=timeout,
        )
        try:
            client.ping()
            return True
        finally:
            client.close()

    async def list_buckets_or_dbs(self) -> List[Mapping[str, str]]:
        databases = self._client.get_list_database()
        items: List[Mapping[str, str]] = []
        for db in databases:
            db_name = db.get("name")
            items.append({"name": db_name, "type": "db", "retention": None})
            rps = self._client.get_list_retention_policies(database=db_name)
            for rp in rps:
                retention = rp.get("duration")
                items.append({"name": f"{db_name}/{rp['name']}", "type": "db", "retention": retention})
        return items

    async def list_measurements(self, target: str) -> List[Mapping[str, str]]:
        db, rp = _split_target(target)
        query = build_influxql_measurements_query(db, rp)
        result = self._client.query(query, database=db)
        return [{"name": point.get("name") } for point in result.get_points()]

    async def list_fields(self, target: str, measurement: str) -> List[Mapping[str, str]]:
        db, rp = _split_target(target)
        query = build_influxql_field_keys_query(db, rp, measurement)
        result = self._client.query(query, database=db)
        items: List[Mapping[str, str]] = []
        for point in result.get_points():
            items.append({"name": point.get("fieldKey"), "type": point.get("fieldType")})
        return items

    async def list_tags(self, target: str, measurement: str) -> List[Mapping[str, Any]]:
        db, rp = _split_target(target)
        query = build_influxql_tag_keys_query(db, rp, measurement)
        result = self._client.query(query, database=db)
        tag_keys = [point.get("tagKey") for point in result.get_points()]
        items: List[Mapping[str, Any]] = []
        for key in tag_keys:
            from_clause = f'"{measurement}"'
            if rp:
                from_clause = f'"{rp}"."{measurement}"'
            values_query = f'SHOW TAG VALUES FROM {from_clause} WITH KEY = "{key}"'
            values_result = self._client.query(values_query, database=db)
            values = [point.get("value") for point in values_result.get_points()]
            items.append({"key": key, "values": values})
        return items

    async def last_point(
        self, target: str, measurement: str, field: Optional[str], tags: Mapping[str, str]
    ) -> Mapping[str, Any]:
        db, rp = _split_target(target)
        query = build_influxql_last_point_query(db, rp, measurement, field, tags)
        result = self._client.query(query, database=db)
        points = list(result.get_points(measurement=measurement))
        if not points:
            raise InfluxClientError("No data available for last_point query")
        point = points[0]
        tags_out = {key: point.get(key) for key in tags.keys() if key in point}
        if field:
            value = point.get(field)
        else:
            value = {k: v for k, v in point.items() if k != "time"}
        return {
            "time_iso": point.get("time"),
            "value": value,
            "field": field,
            "tags": tags_out,
        }

    async def query_timeseries(self, ctx: QueryContext) -> tuple[List[Mapping[str, Any]], Mapping[str, Any]]:
        db, rp = _split_target(ctx.target)
        query = build_influxql_timeseries_query(ctx, db, rp)
        result = self._client.query(query, database=db)
        series: List[Mapping[str, Any]] = []
        for point in result.get_points():
            value = point.get(ctx.field) if ctx.field else None
            if value is None:
                for key, val in point.items():
                    if key != "time":
                        value = val
                        break
            series.append({"time_iso": point.get("time"), "value": value})
        stats = {
            "points": len(series),
            "start_eff": ctx.start_iso,
            "stop_eff": ctx.stop_iso,
            "aggregate": ctx.aggregate,
            "every": ctx.every,
        }
        return series, stats

    async def write_point(self, request: WritePointRequest) -> int:
        db, rp = _split_target(request.target)
        payload = [{
            "measurement": request.measurement,
            "tags": dict(request.tags or {}),
            "fields": {key: value for key, value in request.fields.items() if value is not None},
            "time": request.time_iso,
        }]
        success = self._client.write_points(payload, database=db, retention_policy=rp)
        return 1 if success else 0

    async def discover_resources(self, *, limit: int) -> List[Mapping[str, str]]:
        target = self.default_target
        if not target:
            return []
        measurements = await self.list_measurements(target)
        resources: List[Mapping[str, str]] = []
        for measurement in measurements[:limit]:
            uri = f"influxdb://{target}/{measurement['name']}?start=-1h"
            resources.append({"uri": uri, "title": f"{measurement['name']} (última hora)"})
        return resources


def _split_target(target: str) -> Tuple[str, Optional[str]]:
    if "/" in target:
        db, rp = target.split("/", 1)
        return db, rp
    return target, None


async def dry_run(config: AppConfig) -> Dict[str, Any]:
    """Validate connectivity without starting the MCP loop."""

    client = InfluxClient(config)
    try:
        engine = client.engine
        try:
            await engine.list_buckets_or_dbs()
            reachable = True
        except Exception as exc:
            _LOGGER.warning("Dry-run query failed: %s", exc)
            reachable = False
        return {
            "version": client.mode,
            "default_target": client.default_target,
            "reachable": reachable,
        }
    finally:
        client.close()
