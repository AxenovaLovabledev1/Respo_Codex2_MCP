"""Utility helpers for influx_mcp."""

from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, urlencode, urlparse

try:  # pragma: no cover - optional dependency
    from dateutil import parser as dateutil_parser
except Exception:  # pragma: no cover - runtime fallback
    dateutil_parser = None  # type: ignore[assignment]

_LOGGER = logging.getLogger(__name__)

_RELATIVE_PATTERN = re.compile(r"^(?P<sign>[+-]?)(?P<value>\d+)(?P<unit>[smhdw])$")
_FILL_ALLOWED = {"none", "prev", "linear"}


@dataclass(slots=True)
class ParsedInfluxURI:
    """Structured representation of an `influxdb://` URI."""

    target: str
    measurement: str
    field: str | None = None
    start: str | None = None
    stop: str | None = None
    every: str | None = None
    aggregate: str | None = None
    tags: dict[str, str] | None = None
    limit: int | None = None
    fill: str | None = None


def mask_secret(value: str | None, visible: int = 2) -> str:
    """Return a masked representation of a secret token for logging."""

    if not value:
        return ""
    stripped = value.strip()
    if len(stripped) <= visible:
        return "*" * len(stripped)
    return f"{stripped[:visible]}…{stripped[-visible:]}"


def _ensure_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_iso_datetime(value: str) -> datetime:
    if dateutil_parser is not None:  # pragma: no cover - exercised when dependency available
        dt = dateutil_parser.isoparse(value)
    else:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    return _ensure_datetime(dt)


def _relative_to_timedelta(expression: str) -> timedelta:
    match = _RELATIVE_PATTERN.match(expression)
    if not match:
        raise ValueError(f"Invalid relative duration: {expression}")
    sign_token = match.group("sign")
    if sign_token == "-":
        sign = -1
    else:
        sign = 1
    value = int(match.group("value"))
    unit = match.group("unit")
    factor = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
    }[unit]
    seconds = sign * value * factor
    return timedelta(seconds=seconds)


def parse_duration(value: str) -> timedelta:
    """Parse duration strings like ``5m`` or ``-24h`` into ``timedelta``."""

    value = value.strip()
    delta = _relative_to_timedelta(value)
    return delta


def resolve_time_range(
    start: str,
    stop: str | None,
    *,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Resolve start/stop values expressed as ISO 8601 or relative durations."""

    now = _ensure_datetime(now or datetime.now(UTC))

    def _parse(value: str) -> datetime:
        value = value.strip()
        if _RELATIVE_PATTERN.match(value):
            delta = _relative_to_timedelta(value)
            return now + delta
        return _parse_iso_datetime(value)

    start_dt = _parse(start)
    stop_dt = _parse(stop) if stop else now
    if start_dt >= stop_dt:
        raise ValueError("start must be before stop")
    return start_dt, stop_dt


def ensure_limit(value: int | None, *, default: int | None = None, hard_cap: int = 50_000) -> int | None:
    """Sanitise user-provided limits to avoid runaway queries."""

    if value is None:
        return default
    if value <= 0:
        raise ValueError("limit must be greater than zero")
    if value > hard_cap:
        _LOGGER.warning("Requested limit %s exceeds cap %s; truncating", value, hard_cap)
        return hard_cap
    return value


def normalize_tags(tags: Mapping[str, Any] | None) -> dict[str, str]:
    """Return a sanitized dictionary of tag filters."""

    if not tags:
        return {}
    normalized: dict[str, str] = {}
    for key, value in tags.items():
        if value is None:
            continue
        normalized[str(key)] = str(value)
    return normalized


def format_preview_table(rows: Iterable[Mapping[str, Any]], *, limit: int = 10) -> str:
    """Create a simple human readable table preview."""

    rows_list = list(rows)
    preview_rows = rows_list[:limit]
    if not preview_rows:
        return "<sin datos>"

    headers = list(preview_rows[0].keys())
    col_widths = []
    for header in headers:
        values = [str(header)]
        values.extend(str(row.get(header, "")) for row in preview_rows)
        col_widths.append(max(len(value) for value in values))
    lines = []
    header_line = " | ".join(header.ljust(width) for header, width in zip(headers, col_widths))
    lines.append(header_line)
    lines.append("-+-".join("-" * width for width in col_widths))
    for row in preview_rows:
        lines.append(" | ".join(str(row.get(header, "")).ljust(width) for header, width in zip(headers, col_widths)))
    if len(rows_list) > limit:
        lines.append("…")
    return "\n".join(lines)


def parse_influx_uri(uri: str) -> ParsedInfluxURI:
    """Parse an ``influxdb://`` URI into its components."""

    parsed = urlparse(uri)
    if parsed.scheme != "influxdb":
        raise ValueError("URI must start with influxdb://")
    if not parsed.path or parsed.path == "/":
        raise ValueError("URI must include a measurement name")
    target = parsed.netloc
    measurement = parsed.path.lstrip("/")
    params = parse_qs(parsed.query, keep_blank_values=False)

    tags: dict[str, str] = {}
    other: dict[str, str] = {}
    for key, values in params.items():
        value = values[0]
        if key.startswith("tag."):
            tags[key[4:]] = value
        else:
            other[key] = value

    limit = int(other["limit"]) if "limit" in other else None
    fill = other.get("fill")
    if fill and fill not in _FILL_ALLOWED:
        raise ValueError(f"Invalid fill strategy: {fill}")

    return ParsedInfluxURI(
        target=target,
        measurement=measurement,
        field=other.get("field"),
        start=other.get("start"),
        stop=other.get("stop"),
        every=other.get("every"),
        aggregate=other.get("aggregate"),
        tags=tags or None,
        limit=limit,
        fill=fill,
    )


def build_influx_uri(params: ParsedInfluxURI) -> str:
    """Create a canonical URI from :class:`ParsedInfluxURI`."""

    query: dict[str, str] = {}
    if params.field:
        query["field"] = params.field
    if params.start:
        query["start"] = params.start
    if params.stop:
        query["stop"] = params.stop
    if params.every:
        query["every"] = params.every
    if params.aggregate:
        query["aggregate"] = params.aggregate
    if params.limit is not None:
        query["limit"] = str(params.limit)
    if params.fill:
        query["fill"] = params.fill
    if params.tags:
        for key, value in params.tags.items():
            query[f"tag.{key}"] = value
    encoded = urlencode(query)
    path = f"/{params.measurement}" if not params.measurement.startswith("/") else params.measurement
    return f"influxdb://{params.target}{path}{('?' + encoded) if encoded else ''}"


def make_resource_text(summary: Mapping[str, Any], series: Iterable[Mapping[str, Any]]) -> str:
    """Format resource output with summary and JSON appendix."""

    preview = format_preview_table(series)
    json_payload = json.dumps(summary, indent=2, ensure_ascii=False)
    return (
        "Consulta InfluxDB\n"
        f"Target: {summary.get('target')}\n"
        f"Measurement: {summary.get('measurement')}\n"
        f"Field: {summary.get('field')}\n"
        f"Ventana: {summary.get('start')} → {summary.get('stop')}\n\n"
        "Previsualización:\n"
        f"{preview}\n\n"
        "JSON completo:\n"
        f"{json_payload}\n"
    )


def mean(values: Iterable[float]) -> float:
    data = list(values)
    if not data:
        return math.nan
    return sum(data) / len(data)
