"""Environment configuration loading for influx_mcp."""

from __future__ import annotations

import logging
import os
from typing import Mapping, Optional

try:  # pragma: no cover - optional dependency shim
    from pydantic import BaseModel, ConfigDict, Field, ValidationError
except Exception:  # pragma: no cover - fallback minimal shim
    from .schemas import BaseModel, ConfigDict, Field  # type: ignore[assignment]

    class ValidationError(Exception):
        pass


_LOGGER = logging.getLogger(__name__)


class ConfigError(RuntimeError):
    """Raised when configuration values are invalid."""


class InfluxV2Settings(BaseModel):
    url: str
    org: str
    token: str
    default_bucket: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class InfluxV1Settings(BaseModel):
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    default_db: Optional[str] = None
    default_rp: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class AppConfig(BaseModel):
    influx_version: str = Field(default="auto")
    request_timeout_sec: int = Field(default=30, ge=1)
    log_level: str = Field(default="INFO")
    v2: Optional[InfluxV2Settings] = None
    v1: Optional[InfluxV1Settings] = None

    model_config = ConfigDict(extra="forbid")


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def load_config(environ: Mapping[str, str] | None = None) -> AppConfig:
    env = {key: value for key, value in (environ or os.environ).items() if value is not None}
    version = _clean(env.get("INFLUX_VERSION")) or "auto"
    if version not in {"1", "2", "auto"}:
        raise ConfigError("INFLUX_VERSION must be one of '1', '2' or 'auto'")

    log_level = _clean(env.get("MCP_LOG_LEVEL")) or "INFO"
    timeout_raw = _clean(env.get("INFLUX_REQUEST_TIMEOUT_SEC"))
    try:
        timeout = int(timeout_raw) if timeout_raw else 30
    except ValueError as exc:
        raise ConfigError("INFLUX_REQUEST_TIMEOUT_SEC must be an integer") from exc

    v2_settings: Optional[InfluxV2Settings] = None
    v1_settings: Optional[InfluxV1Settings] = None

    url = _clean(env.get("INFLUX_URL"))

    if version in {"2", "auto"}:
        org = _clean(env.get("INFLUX_ORG"))
        token = _clean(env.get("INFLUX_TOKEN"))
        if url and org and token:
            try:
                v2_settings = InfluxV2Settings(
                    url=url,
                    org=org,
                    token=token,
                    default_bucket=_clean(env.get("INFLUX_DEFAULT_BUCKET")),
                )
            except ValidationError as exc:  # pragma: no cover - validation depends on pydantic
                raise ConfigError(str(exc)) from exc
        elif version == "2":
            raise ConfigError("INFLUX_URL, INFLUX_ORG and INFLUX_TOKEN are required for InfluxDB v2")

    if version in {"1", "auto"}:
        if url:
            try:
                v1_settings = InfluxV1Settings(
                    url=url,
                    username=_clean(env.get("INFLUX_USERNAME")),
                    password=_clean(env.get("INFLUX_PASSWORD")),
                    default_db=_clean(env.get("INFLUX_DEFAULT_DB")),
                    default_rp=_clean(env.get("INFLUX_DEFAULT_RP")),
                )
            except ValidationError as exc:  # pragma: no cover
                raise ConfigError(str(exc)) from exc
        elif version == "1":
            raise ConfigError("INFLUX_URL is required for InfluxDB v1")

    if not v1_settings and not v2_settings:
        raise ConfigError("No InfluxDB configuration found; set INFLUX_URL/ORG/TOKEN or credentials")

    return AppConfig(
        influx_version=version,
        request_timeout_sec=timeout,
        log_level=log_level,
        v2=v2_settings,
        v1=v1_settings,
    )


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    _LOGGER.debug("Logging configured to %s", level)
