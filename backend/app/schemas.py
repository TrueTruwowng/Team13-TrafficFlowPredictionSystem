from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DatasetObjectResponse(BaseModel):
    name: str


class DatasetListResponse(BaseModel):
    bucket: str
    prefix: str
    items: list[DatasetObjectResponse] = Field(default_factory=list)


class LayerDatesResponse(BaseModel):
    layer: str
    prefix: str
    dates: list[str] = Field(default_factory=list)


class PreviewResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]


class DayDataResponse(BaseModel):
    target_date: str
    columns: list[str]
    row_count: int
    rows: list[dict[str, Any]]


class LatestLayerDataResponse(BaseModel):
    layer: str
    prefix: str
    target_date: str | None = None
    object_count: int
    columns: list[str]
    row_count: int
    rows: list[dict[str, Any]]


class DayRoadCountResponse(BaseModel):
    target_date: str
    unique_roads: int
    roads: list[str] = Field(default_factory=list)


class RoadComparisonResponse(BaseModel):
    start_date: str
    end_date: str
    start_unique_roads: int
    end_unique_roads: int
    start_roads: list[str] = Field(default_factory=list)
    end_roads: list[str] = Field(default_factory=list)
    roads_only_in_start: list[str] = Field(default_factory=list)
    roads_only_in_end: list[str] = Field(default_factory=list)
    shared_roads: list[str] = Field(default_factory=list)


class DateRangeDataResponse(BaseModel):
    start_date: str
    end_date: str
    columns: list[str]
    row_count: int
    rows: list[dict[str, Any]]


class StatsResponse(BaseModel):
    rows: int | None = None
    rows_with_road_name: int | None = None
    unique_roads: int | None = None
    unique_weather: int | None = None
    min_record_datetime: str | None = None
    max_record_datetime: str | None = None


class MapBoundsResponse(BaseModel):
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float


class MapFeatureResponse(BaseModel):
    osm_way_id: str
    sumo_edge_ids: list[str] = Field(default_factory=list)
    name: str | None = None
    highway: str | None = None
    coordinates: list[list[float]] = Field(default_factory=list)


class MapGeometryResponse(BaseModel):
    bounds: MapBoundsResponse
    features: list[MapFeatureResponse] = Field(default_factory=list)

