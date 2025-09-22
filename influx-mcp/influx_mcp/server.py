"""MCP server entry-point for InfluxDB queries."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any, Dict, Optional

from .client import InfluxClient, dry_run
from .queries import QueryEngine
from .config import AppConfig, ConfigError, configure_logging, load_config
from .schemas import (
    LastPointRequest,
    QueryTimeseriesRequest,
    WindowStatsRequest,
    WritePointRequest,
)
from . import schemas

try:  # pragma: no cover - optional dependency
    from mcp.server.fastmcp import FastMCP
except Exception:  # pragma: no cover - fallback placeholder
    FastMCP = None  # type: ignore[assignment]

_LOGGER = logging.getLogger(__name__)


def build_app(engine: QueryEngine) -> Any:
    if FastMCP is None:  # pragma: no cover - executed only without dependency
        raise ImportError("mcp package is required to run the server")

    app = FastMCP("influx-mcp")

    @app.tool()
    async def list_buckets_or_dbs() -> Dict[str, Any]:
        response = await engine.list_buckets_or_dbs()
        return response.model_dump()

    @app.tool()
    async def list_measurements(target: str) -> Dict[str, Any]:
        request = schemas.ListMeasurementsRequest(target=target)
        response = await engine.list_measurements(request.target)
        return response.model_dump()

    @app.tool()
    async def list_fields(target: str, measurement: str) -> Dict[str, Any]:
        request = schemas.ListFieldsRequest(target=target, measurement=measurement)
        response = await engine.list_fields(request.target, request.measurement)
        return response.model_dump()

    @app.tool()
    async def list_tags(target: str, measurement: str) -> Dict[str, Any]:
        request = schemas.ListTagsRequest(target=target, measurement=measurement)
        response = await engine.list_tags(request.target, request.measurement)
        return response.model_dump()

    @app.tool()
    async def last_point(
        target: str,
        measurement: str,
        field: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        request = LastPointRequest(target=target, measurement=measurement, field=field, tags=tags)
        response = await engine.last_point(request.target, request.measurement, request.field, request.tags)
        return response.model_dump()

    @app.tool()
    async def query_timeseries(**params: Any) -> Dict[str, Any]:
        request = QueryTimeseriesRequest.model_validate(params)
        response = await engine.query_timeseries(request)
        return response.model_dump()

    @app.tool()
    async def window_stats(**params: Any) -> Dict[str, Any]:
        request = WindowStatsRequest.model_validate(params)
        response = await engine.window_stats(request)
        return response.model_dump()

    @app.tool()
    async def write_point(**params: Any) -> Dict[str, Any]:
        request = WritePointRequest.model_validate(params)
        response = await engine.write_point(request)
        return response.model_dump()

    @app.resource("influxdb://")
    async def influx_resource(uri: str) -> str:
        return await engine.read_resource(uri)

    @app.list_resources("influxdb://")
    async def influx_list_resources(limit: int = 5) -> Dict[str, Any]:
        response = await engine.list_resources(limit=limit)
        return response.model_dump()

    return app


def _load_and_prepare(argv: Optional[list[str]] = None) -> tuple[AppConfig, argparse.Namespace]:
    parser = argparse.ArgumentParser(description="InfluxDB MCP server")
    parser.add_argument("--dry-run", action="store_true", help="Valida la conexión y termina")
    args = parser.parse_args(argv)

    try:
        config = load_config()
    except ConfigError as exc:
        parser.error(str(exc))
    return config, args


def main(argv: Optional[list[str]] = None) -> int:
    config, args = _load_and_prepare(argv)
    configure_logging(config.log_level)

    if args.dry_run:
        result = asyncio.run(dry_run(config))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    client = InfluxClient(config)
    try:
        app = build_app(client.engine)
        _LOGGER.info("Starting MCP server (InfluxDB v%s)", client.mode)
        app.run()
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
