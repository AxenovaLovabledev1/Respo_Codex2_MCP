"""Query builders and orchestration for InfluxDB v2/v1."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

from . import schemas
from .utils import ensure_limit, make_resource_text, normalize_tags, parse_duration, parse_influx_uri, resolve_time_range

FLUX_AGG_MAP = {
    "mean": "mean",
    "max": "max",
    "min": "min",
    "sum": "sum",
    "count": "count",
    "median": "median",
    "spread": "spread",
    "last": "last",
    "first": "first",
}

INFLUXQL_AGG_MAP = FLUX_AGG_MAP

TAG_VALUE_LIMIT_DEFAULT = 20


@dataclass
class QueryContext:
    target: str
    measurement: str
    field: Optional[str]
    start_iso: str
    stop_iso: str
    tags: Dict[str, str]
    aggregate: Optional[str]
    every: Optional[str]
    limit: Optional[int]
    fill: Optional[str]


def _flux_filter_expression(measurement: str, field: Optional[str], tags: Mapping[str, str]) -> str:
    clauses: List[str] = [f'r["_measurement"] == "{measurement}"']
    if field:
        clauses.append(f'r["_field"] == "{field}"')
    for key, value in tags.items():
        clauses.append(f'r["{key}"] == "{value}"')
    return " and ".join(clauses)


def build_flux_measurements_query(bucket: str) -> str:
    return f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{bucket}")'


def build_flux_fields_query(bucket: str, measurement: str) -> str:
    return (
        'import "influxdata/influxdb/schema"\n'
        f'schema.fieldKeys(bucket: "{bucket}", predicate: (r) => r["_measurement"] == "{measurement}")'
    )


def build_flux_tag_keys_query(bucket: str, measurement: str) -> str:
    return (
        'import "influxdata/influxdb/schema"\n'
        f'schema.tagKeys(bucket: "{bucket}", predicate: (r) => r["_measurement"] == "{measurement}")'
    )


def build_flux_tag_values_query(bucket: str, measurement: str, tag_key: str, *, value_limit: int = TAG_VALUE_LIMIT_DEFAULT) -> str:
    return (
        'import "influxdata/influxdb/schema"\n'
        f'schema.tagValues(bucket: "{bucket}", predicate: (r) => r["_measurement"] == "{measurement}", tag: "{tag_key}")'
        f'\n  |> limit(n: {value_limit})'
    )


def build_flux_last_point_query(bucket: str, measurement: str, field: Optional[str], tags: Mapping[str, str]) -> str:
    filter_expr = _flux_filter_expression(measurement, field, tags)
    return (
        f'from(bucket: "{bucket}")\n'
        '  |> range(start: time(v: "1970-01-01T00:00:00Z"))\n'
        f'  |> filter(fn: (r) => {filter_expr})\n'
        '  |> sort(columns: ["_time"], desc: true)\n'
        '  |> limit(n: 1)'
    )


def build_flux_timeseries_query(ctx: QueryContext) -> str:
    filter_expr = _flux_filter_expression(ctx.measurement, ctx.field, ctx.tags)
    imports: List[str] = []
    if ctx.fill == "linear":
        imports.append('import "experimental"')
    lines = [
        f'from(bucket: "{ctx.target}")',
        f'  |> range(start: time(v: "{ctx.start_iso}"), stop: time(v: "{ctx.stop_iso}"))',
        f'  |> filter(fn: (r) => {filter_expr})',
    ]
    if ctx.aggregate and ctx.every:
        flux_fn = FLUX_AGG_MAP[ctx.aggregate]
        lines.append(f'  |> aggregateWindow(every: {ctx.every}, fn: {flux_fn}, createEmpty: false)')
    elif ctx.aggregate:
        flux_fn = FLUX_AGG_MAP[ctx.aggregate]
        lines.append(f'  |> {flux_fn}()')
    if ctx.fill == "prev":
        lines.append('  |> fill(column: "_value", usePrevious: true)')
    elif ctx.fill == "linear":
        every = ctx.every or "1m"
        lines.append(f'  |> experimental.interpolate.linear(every: {every})')
    if ctx.limit:
        lines.append(f'  |> limit(n: {ctx.limit})')
    return ("\n".join(imports + lines)).strip()


def build_influxql_measurements_query(db: str, rp: Optional[str]) -> str:
    if rp:
        return f'SHOW MEASUREMENTS ON "{db}" WITH MEASUREMENT =~ /.*/'
    return f'SHOW MEASUREMENTS ON "{db}"'


def build_influxql_field_keys_query(db: str, rp: Optional[str], measurement: str) -> str:
    from_clause = f'"{measurement}"'
    if rp:
        from_clause = f'"{rp}"."{measurement}"'
    return f'SHOW FIELD KEYS FROM {from_clause} ON "{db}"'


def build_influxql_tag_keys_query(db: str, rp: Optional[str], measurement: str) -> str:
    from_clause = f'"{measurement}"'
    if rp:
        from_clause = f'"{rp}"."{measurement}"'
    return f'SHOW TAG KEYS FROM {from_clause} ON "{db}"'


def build_influxql_last_point_query(db: str, rp: Optional[str], measurement: str, field: Optional[str], tags: Mapping[str, str]) -> str:
    from_clause = f'"{measurement}"'
    if rp:
        from_clause = f'"{rp}"."{measurement}"'
    select_field = "*" if not field else f'"{field}"'
    where_parts = []
    if tags:
        for key, value in tags.items():
            where_parts.append(f'"{key}" = \'{value}\'')
    where_clause = " AND ".join(where_parts)
    if where_clause:
        where_clause = " WHERE " + where_clause
    return (
        f'SELECT {select_field} FROM {from_clause}{where_clause} ORDER BY time DESC LIMIT 1'
    )


def build_influxql_timeseries_query(ctx: QueryContext, db: str, rp: Optional[str]) -> str:
    from_clause = f'"{ctx.measurement}"'
    if rp:
        from_clause = f'"{rp}"."{ctx.measurement}"'
    field_expr = f'"{ctx.field}"' if ctx.field else '*'
    where_parts = [
        f'time >= \'{ctx.start_iso}\'',
        f'time < \'{ctx.stop_iso}\'',
    ]
    for key, value in ctx.tags.items():
        where_parts.append(f'"{key}" = \'{value}\'')
    where_clause = " AND ".join(where_parts)
    query = f'SELECT {field_expr} FROM {from_clause} WHERE {where_clause}'
    if ctx.aggregate and ctx.every:
        agg_fn = INFLUXQL_AGG_MAP[ctx.aggregate]
        query = f'SELECT {agg_fn}({field_expr}) FROM {from_clause} WHERE {where_clause} GROUP BY time({ctx.every})'
    elif ctx.aggregate:
        agg_fn = INFLUXQL_AGG_MAP[ctx.aggregate]
        query = f'SELECT {agg_fn}({field_expr}) FROM {from_clause} WHERE {where_clause}'
    if ctx.aggregate and ctx.fill:
        fill_clause = {
            "none": "",
            "prev": " fill(previous)",
            "linear": " fill(linear)",
        }[ctx.fill]
        if fill_clause:
            query = f"{query}{fill_clause}"
    if ctx.limit:
        query = f"{query} LIMIT {ctx.limit}"
    return query


class QueryEngine:
    """High level façade used by the MCP tools."""

    def __init__(self, backend: "BaseBackend") -> None:
        self._backend = backend

    @property
    def default_target(self) -> Optional[str]:
        return self._backend.default_target

    @property
    def mode(self) -> str:
        return self._backend.mode

    async def list_buckets_or_dbs(self) -> schemas.ListBucketsResponse:
        records = await self._backend.list_buckets_or_dbs()
        items = [schemas.BucketInfo(**record) for record in records]
        return schemas.ListBucketsResponse(items=items)

    async def list_measurements(self, target: str) -> schemas.ListMeasurementsResponse:
        records = await self._backend.list_measurements(target)
        items = [schemas.MeasurementInfo(**record) for record in records]
        return schemas.ListMeasurementsResponse(items=items)

    async def list_fields(self, target: str, measurement: str) -> schemas.ListFieldsResponse:
        records = await self._backend.list_fields(target, measurement)
        items = [schemas.FieldInfo(**record) for record in records]
        return schemas.ListFieldsResponse(items=items)

    async def list_tags(self, target: str, measurement: str) -> schemas.ListTagsResponse:
        records = await self._backend.list_tags(target, measurement)
        items = [schemas.TagInfo(**record) for record in records]
        return schemas.ListTagsResponse(items=items)

    async def last_point(
        self, target: str, measurement: str, field: Optional[str], tags: Optional[Mapping[str, str]]
    ) -> schemas.LastPointResponse:
        record = await self._backend.last_point(target, measurement, field, normalize_tags(tags))
        return schemas.LastPointResponse(**record)

    async def query_timeseries(self, request: schemas.QueryTimeseriesRequest) -> schemas.QueryTimeseriesResponse:
        start_dt, stop_dt = resolve_time_range(request.start, request.stop)
        limit = ensure_limit(request.limit, default=None)
        ctx = QueryContext(
            target=request.target,
            measurement=request.measurement,
            field=request.field,
            start_iso=start_dt.isoformat(),
            stop_iso=stop_dt.isoformat(),
            tags=normalize_tags(request.tags),
            aggregate=request.aggregate,
            every=request.every,
            limit=limit,
            fill=request.fill,
        )
        series, stats = await self._backend.query_timeseries(ctx)
        points = [schemas.TimeseriesPoint(**point) for point in series]
        stats_model = schemas.TimeseriesStats(**stats)
        return schemas.QueryTimeseriesResponse(series=points, stats=stats_model)

    async def window_stats(self, request: schemas.WindowStatsRequest) -> schemas.WindowStatsResponse:
        duration = parse_duration(request.window)
        stop_dt = datetime.now(UTC)
        if duration.total_seconds() == 0:
            raise ValueError("window duration must be non-zero")
        start_dt = stop_dt - duration if duration.total_seconds() > 0 else stop_dt + duration
        start_iso = start_dt.isoformat()
        stop_iso = stop_dt.isoformat()
        ctx = schemas.QueryTimeseriesRequest(
            target=request.target,
            measurement=request.measurement,
            field=request.field,
            start=start_iso,
            stop=stop_iso,
            tags=request.tags,
            aggregate=None,
            every=None,
        )
        response = await self.query_timeseries(ctx)
        values: List[float] = []
        for point in response.series:
            try:
                values.append(float(point.value))
            except (TypeError, ValueError):
                continue
        mean_value = sum(values) / len(values) if values else None
        min_value = min(values) if values else None
        max_value = max(values) if values else None
        last_value = response.series[-1].value if response.series else None
        return schemas.WindowStatsResponse(
            mean=mean_value,
            min=min_value,
            max=max_value,
            last=last_value,
            count=len(response.series),
            start=start_iso,
            stop=stop_iso,
        )

    async def write_point(self, request: schemas.WritePointRequest) -> schemas.WritePointResponse:
        try:
            written = await self._backend.write_point(request)
        except NotImplementedError as exc:
            raise RuntimeError("Write operations are not supported by this backend") from exc
        return schemas.WritePointResponse(ok=written > 0, written=written)

    async def read_resource(self, uri: str) -> str:
        parsed = parse_influx_uri(uri)
        request = schemas.QueryTimeseriesRequest(
            target=parsed.target,
            measurement=parsed.measurement,
            field=parsed.field,
            start=parsed.start or "-1h",
            stop=parsed.stop,
            tags=parsed.tags,
            aggregate=parsed.aggregate,
            every=parsed.every,
            limit=parsed.limit,
            fill=parsed.fill,
        )
        response = await self.query_timeseries(request)
        summary = {
            "target": request.target,
            "measurement": request.measurement,
            "field": request.field,
            "start": response.stats.start_eff,
            "stop": response.stats.stop_eff,
            "aggregate": response.stats.aggregate,
            "every": response.stats.every,
        }
        return make_resource_text(summary, (point.model_dump() for point in response.series))

    async def list_resources(self, limit: int = 5) -> schemas.ListResourcesResponse:
        items = await self._backend.discover_resources(limit=limit)
        resources = [schemas.ResourceDescription(**item) for item in items]
        return schemas.ListResourcesResponse(items=resources)


class BaseBackend:
    """Protocol-style base class for static type checking."""

    mode: str
    default_target: Optional[str]

    async def list_buckets_or_dbs(self) -> List[Mapping[str, Any]]:  # pragma: no cover - interface definition
        raise NotImplementedError

    async def list_measurements(self, target: str) -> List[Mapping[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    async def list_fields(self, target: str, measurement: str) -> List[Mapping[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    async def list_tags(self, target: str, measurement: str) -> List[Mapping[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    async def last_point(
        self, target: str, measurement: str, field: Optional[str], tags: Mapping[str, str]
    ) -> Mapping[str, str]:  # pragma: no cover
        raise NotImplementedError

    async def query_timeseries(self, ctx: QueryContext) -> tuple[List[Mapping[str, Any]], Mapping[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    async def write_point(self, request: schemas.WritePointRequest) -> int:  # pragma: no cover
        raise NotImplementedError

    async def discover_resources(self, *, limit: int) -> List[Mapping[str, Any]]:  # pragma: no cover
        raise NotImplementedError
