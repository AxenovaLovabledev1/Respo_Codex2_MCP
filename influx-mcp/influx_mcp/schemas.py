"""Pydantic models that describe tool inputs and outputs."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional

try:  # pragma: no cover - optional dependency shim
    from pydantic import BaseModel, Field, ConfigDict, field_validator
except Exception:  # pragma: no cover - fallback for environments without pydantic
    class BaseModel:  # type: ignore[override]
        model_config: Dict[str, Any] = {}

        def __init__(self, **data: Any) -> None:
            for key, value in data.items():
                setattr(self, key, value)

        @classmethod
        def model_validate(cls, data: Mapping[str, Any]) -> "BaseModel":
            return cls(**dict(data))

        def model_dump(self, **_: Any) -> Dict[str, Any]:
            return {key: value for key, value in self.__dict__.items()}

    def Field(default: Any = None, **_: Any) -> Any:  # type: ignore[override]
        return default

    def field_validator(*_args: Any, **_kwargs: Any):  # type: ignore[override]
        def decorator(func):
            return func

        return decorator

    class ConfigDict(dict):  # type: ignore[override]
        pass


class BucketInfo(BaseModel):
    name: str
    type: Literal["bucket", "db"]
    retention: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class ListBucketsResponse(BaseModel):
    items: List[BucketInfo]

    model_config = ConfigDict(extra="forbid")


class MeasurementInfo(BaseModel):
    name: str

    model_config = ConfigDict(extra="forbid")


class ListMeasurementsResponse(BaseModel):
    items: List[MeasurementInfo]

    model_config = ConfigDict(extra="forbid")


class ListMeasurementsRequest(BaseModel):
    target: str

    model_config = ConfigDict(extra="forbid")


class FieldInfo(BaseModel):
    name: str
    type: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class ListFieldsResponse(BaseModel):
    items: List[FieldInfo]

    model_config = ConfigDict(extra="forbid")


class ListFieldsRequest(BaseModel):
    target: str
    measurement: str

    model_config = ConfigDict(extra="forbid")


class TagInfo(BaseModel):
    key: str
    values: List[str]

    model_config = ConfigDict(extra="forbid")


class ListTagsResponse(BaseModel):
    items: List[TagInfo]

    model_config = ConfigDict(extra="forbid")


class ListTagsRequest(BaseModel):
    target: str
    measurement: str

    model_config = ConfigDict(extra="forbid")


class LastPointResponse(BaseModel):
    time_iso: str
    value: Any
    field: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")


class LastPointRequest(BaseModel):
    target: str
    measurement: str
    field: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")


class TimeseriesPoint(BaseModel):
    time_iso: str
    value: Any

    model_config = ConfigDict(extra="forbid")


class TimeseriesStats(BaseModel):
    points: int
    start_eff: str
    stop_eff: str
    aggregate: Optional[str] = None
    every: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class QueryTimeseriesResponse(BaseModel):
    series: List[TimeseriesPoint]
    stats: TimeseriesStats

    model_config = ConfigDict(extra="forbid")


class QueryTimeseriesRequest(BaseModel):
    target: str
    measurement: str
    field: Optional[str] = None
    start: str
    stop: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    aggregate: Optional[str] = None
    every: Optional[str] = None
    limit: Optional[int] = Field(default=None, ge=1)
    fill: Optional[Literal["none", "prev", "linear"]] = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("aggregate")
    @classmethod
    def _validate_aggregate(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        allowed = {"mean", "max", "min", "sum", "count", "median", "spread", "last", "first"}
        if value not in allowed:
            raise ValueError(f"aggregate must be one of {sorted(allowed)}")
        return value


class WindowStatsRequest(BaseModel):
    target: str
    measurement: str
    field: str
    window: str
    tags: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")


class WindowStatsResponse(BaseModel):
    mean: Optional[float]
    min: Optional[float]
    max: Optional[float]
    last: Optional[Any]
    count: int
    start: str
    stop: str

    model_config = ConfigDict(extra="forbid")


class WritePointRequest(BaseModel):
    target: str
    measurement: str
    fields: Mapping[str, Any]
    tags: Optional[Mapping[str, Any]] = None
    time_iso: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class WritePointResponse(BaseModel):
    ok: bool
    written: int

    model_config = ConfigDict(extra="forbid")


class ResourceDescription(BaseModel):
    uri: str
    title: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class ListResourcesResponse(BaseModel):
    items: List[ResourceDescription]

    model_config = ConfigDict(extra="forbid")


class DryRunResult(BaseModel):
    version: str
    default_target: Optional[str]
    reachable: bool

    model_config = ConfigDict(extra="forbid")
