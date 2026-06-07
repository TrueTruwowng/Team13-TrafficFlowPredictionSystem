from __future__ import annotations

from datetime import date
import re

from fastapi import APIRouter, HTTPException, Query

from app.core.config import settings
from app.db.duckdb_client import DuckDBClient
from app.schemas import DatasetListResponse, DatasetObjectResponse, DateRangeDataResponse, DayDataResponse, DayRoadCountResponse, LatestLayerDataResponse, LayerDatesResponse, PreviewResponse, RealtimeLayerDataResponse, RoadComparisonResponse, StatsResponse
from app.services.gcs_repository import GCSParquetRepository


router = APIRouter(prefix="/datasets", tags=["datasets"])

repository = GCSParquetRepository(
    bucket_name=settings.gcs_bucket,
    prefix=settings.gcs_prefix,
    cache_dir=settings.cache_dir,
)
duckdb_client = DuckDBClient(settings.duckdb_path)
DATE_PART_PATTERN = re.compile(r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)")
FILE_PART_PATTERN = re.compile(r"part-(\d+)", re.IGNORECASE)


def _filter_frame_by_road(frame, road_name: str | None):
    if not road_name:
        return frame

    normalized_road_name = road_name.strip()
    if not normalized_road_name:
        return frame

    road_column = None
    for candidate in ("road_name", "roadName"):
        if candidate in frame.columns:
            road_column = candidate
            break

    if road_column is None:
        return frame.iloc[0:0]

    normalized_values = frame[road_column].fillna("").astype(str).str.strip()
    return frame.loc[normalized_values == normalized_road_name]


def _layer_prefix_from_gold(layer: str) -> str:
    normalized_prefix = settings.gcs_prefix.strip("/")
    parts = [part for part in normalized_prefix.split("/") if part]
    if "gold" in parts:
        parts[parts.index("gold")] = layer
        return "/".join(parts)
    if parts:
        return "/".join([layer, *parts[1:]]) if parts[0] in {"silver", "bronze"} else layer
    return layer


def _extract_object_date(object_name: str) -> date | None:
    match = DATE_PART_PATTERN.search(object_name)
    if not match:
        return None
    return date.fromisoformat(match.group(1))


def _extract_file_part(object_name: str) -> int:
    match = FILE_PART_PATTERN.search(object_name)
    return int(match.group(1)) if match else -1


def _repository_for_layer(layer: str) -> tuple[str, GCSParquetRepository]:
    layer_prefix = _layer_prefix_from_gold(layer)
    return layer_prefix, GCSParquetRepository(
        bucket_name=settings.gcs_bucket,
        prefix=layer_prefix,
        cache_dir=settings.cache_dir,
    )


def _get_latest_layer_data(layer: str) -> LatestLayerDataResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    layer_prefix, layer_repository = _repository_for_layer(layer)

    object_names = layer_repository.list_parquet_objects()
    latest_date = max((object_date for name in object_names if (object_date := _extract_object_date(name)) is not None), default=None)
    if latest_date is None:
        return LatestLayerDataResponse(
            layer=layer,
            prefix=layer_prefix,
            object_count=0,
            columns=[],
            row_count=0,
            rows=[],
        )

    latest_object_names = layer_repository.list_parquet_objects_for_date(latest_date)
    local_paths = layer_repository.download_many(latest_object_names)
    frame = duckdb_client.read_parquet_files(local_paths)
    return LatestLayerDataResponse(
        layer=layer,
        prefix=layer_prefix,
        target_date=latest_date.isoformat(),
        object_count=len(latest_object_names),
        columns=list(frame.columns),
        row_count=int(len(frame)),
        rows=frame.to_dict(orient="records"),
    )


@router.get("", response_model=DatasetListResponse)
def list_datasets() -> DatasetListResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    items = [DatasetObjectResponse(name=name) for name in repository.list_parquet_objects()]
    return DatasetListResponse(bucket=settings.gcs_bucket, prefix=settings.gcs_prefix, items=items)


@router.get("/by-date", response_model=DatasetListResponse)
def list_datasets_by_date(target_date: date = Query(...)) -> DatasetListResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    items = [DatasetObjectResponse(name=name) for name in repository.list_parquet_objects_for_date(target_date)]
    return DatasetListResponse(bucket=settings.gcs_bucket, prefix=f"{settings.gcs_prefix}date={target_date.isoformat()}/", items=items)


@router.get("/by-date/data", response_model=DayDataResponse)
def get_day_data(target_date: date = Query(...), road_name: str | None = Query(default=None)) -> DayDataResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    object_names = repository.list_parquet_objects_for_date(target_date)
    if not object_names:
        return DayDataResponse(target_date=target_date.isoformat(), columns=[], row_count=0, rows=[])

    local_paths = repository.download_many(object_names)
    frame = duckdb_client.read_parquet_files(local_paths)
    frame = _filter_frame_by_road(frame, road_name)
    return DayDataResponse(
        target_date=target_date.isoformat(),
        columns=list(frame.columns),
        row_count=int(len(frame)),
        rows=frame.to_dict(orient="records"),
    )


@router.get("/silver/latest/data", response_model=LatestLayerDataResponse)
def get_latest_silver_data() -> LatestLayerDataResponse:
    return _get_latest_layer_data("silver")


@router.get("/silver/realtime/data", response_model=RealtimeLayerDataResponse)
def get_realtime_silver_data() -> RealtimeLayerDataResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    layer_prefix, layer_repository = _repository_for_layer("silver")
    object_names = layer_repository.list_parquet_objects()
    dated_objects = [(object_date, name) for name in object_names if (object_date := _extract_object_date(name)) is not None]
    if not dated_objects:
        return RealtimeLayerDataResponse(
            layer="silver",
            prefix=layer_prefix,
            object_count=0,
            columns=[],
            row_count=0,
            rows=[],
        )

    latest_date = max(object_date for object_date, _ in dated_objects)
    latest_date_objects = sorted(
        (name for object_date, name in dated_objects if object_date == latest_date),
        key=lambda name: (_extract_file_part(name), name),
    )
    local_paths = layer_repository.download_many(latest_date_objects)
    frame = duckdb_client.read_parquet_files(local_paths)
    return RealtimeLayerDataResponse(
        layer="silver",
        prefix=layer_prefix,
        target_date=latest_date.isoformat(),
        object_name=None,
        object_count=len(latest_date_objects),
        columns=list(frame.columns),
        row_count=int(len(frame)),
        rows=frame.to_dict(orient="records"),
    )


@router.get("/silver/dates", response_model=LayerDatesResponse)
def list_silver_dates() -> LayerDatesResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    layer_prefix, layer_repository = _repository_for_layer("silver")
    object_names = layer_repository.list_parquet_objects()
    dates = sorted({object_date.isoformat() for name in object_names if (object_date := _extract_object_date(name)) is not None})
    return LayerDatesResponse(layer="silver", prefix=layer_prefix, dates=dates)


@router.get("/silver/by-date/data", response_model=LatestLayerDataResponse)
def get_silver_data_by_date(target_date: date = Query(...)) -> LatestLayerDataResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    layer_prefix, layer_repository = _repository_for_layer("silver")
    object_names = layer_repository.list_parquet_objects_for_date(target_date)
    if not object_names:
        return LatestLayerDataResponse(
            layer="silver",
            prefix=layer_prefix,
            target_date=target_date.isoformat(),
            object_count=0,
            columns=[],
            row_count=0,
            rows=[],
        )

    local_paths = layer_repository.download_many(object_names)
    frame = duckdb_client.read_parquet_files(local_paths)
    return LatestLayerDataResponse(
        layer="silver",
        prefix=layer_prefix,
        target_date=target_date.isoformat(),
        object_count=len(object_names),
        columns=list(frame.columns),
        row_count=int(len(frame)),
        rows=frame.to_dict(orient="records"),
    )


@router.get("/by-date/roads", response_model=DayRoadCountResponse)
def get_day_road_count(target_date: date = Query(...)) -> DayRoadCountResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    object_names = repository.list_parquet_objects_for_date(target_date)
    if not object_names:
        return DayRoadCountResponse(target_date=target_date.isoformat(), unique_roads=0, roads=[])

    local_paths = repository.download_many(object_names)
    roads = duckdb_client.list_unique_roads(local_paths)
    return DayRoadCountResponse(target_date=target_date.isoformat(), unique_roads=len(roads), roads=roads)


@router.get("/by-date/road-comparison", response_model=RoadComparisonResponse)
def compare_day_roads(start_date: date = Query(...), end_date: date = Query(...)) -> RoadComparisonResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")

    start_object_names = repository.list_parquet_objects_for_date(start_date)
    end_object_names = repository.list_parquet_objects_for_date(end_date)

    if not start_object_names and not end_object_names:
        return RoadComparisonResponse(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            start_unique_roads=0,
            end_unique_roads=0,
        )

    start_roads = []
    end_roads = []

    if start_object_names:
        start_paths = repository.download_many(start_object_names)
        start_roads = duckdb_client.list_unique_roads(start_paths)

    if end_object_names:
        end_paths = repository.download_many(end_object_names)
        end_roads = duckdb_client.list_unique_roads(end_paths)

    start_set = set(start_roads)
    end_set = set(end_roads)

    return RoadComparisonResponse(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        start_unique_roads=len(start_roads),
        end_unique_roads=len(end_roads),
        start_roads=start_roads,
        end_roads=end_roads,
        roads_only_in_start=sorted(start_set - end_set),
        roads_only_in_end=sorted(end_set - start_set),
        shared_roads=sorted(start_set & end_set),
    )


@router.get("/by-range", response_model=DatasetListResponse)
def list_datasets_by_range(start_date: date = Query(...), end_date: date = Query(...)) -> DatasetListResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")

    items = [DatasetObjectResponse(name=name) for name in repository.list_parquet_objects_for_date_range(start_date, end_date)]
    return DatasetListResponse(bucket=settings.gcs_bucket, prefix=settings.gcs_prefix, items=items)


@router.get("/by-range/data", response_model=DateRangeDataResponse)
def get_range_data(start_date: date = Query(...), end_date: date = Query(...), road_name: str | None = Query(default=None)) -> DateRangeDataResponse:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")

    object_names = repository.list_parquet_objects_for_date_range(start_date, end_date)
    if not object_names:
        return DateRangeDataResponse(start_date=start_date.isoformat(), end_date=end_date.isoformat(), columns=[], row_count=0, rows=[])

    local_paths = repository.download_many(object_names)
    frame = duckdb_client.read_parquet_files(local_paths)
    frame = _filter_frame_by_road(frame, road_name)
    return DateRangeDataResponse(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        columns=list(frame.columns),
        row_count=int(len(frame)),
        rows=frame.to_dict(orient="records"),
    )


@router.get("/preview", response_model=PreviewResponse)
def preview_dataset(object_name: str = Query(...), limit: int = Query(50, ge=1, le=500)) -> PreviewResponse:
    local_path = repository.download_object(object_name)
    frame = duckdb_client.preview_parquet([local_path], limit=limit)
    return PreviewResponse(columns=list(frame.columns), rows=frame.to_dict(orient="records"))


@router.get("/stats", response_model=StatsResponse)
def dataset_stats(object_name: str = Query(...)) -> StatsResponse:
    local_path = repository.download_object(object_name)
    frame = duckdb_client.stats_parquet([local_path])
    if frame.empty:
        return StatsResponse()
    record = frame.iloc[0].to_dict()
    return StatsResponse(**record)

