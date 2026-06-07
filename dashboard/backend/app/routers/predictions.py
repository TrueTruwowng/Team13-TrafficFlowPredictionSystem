from __future__ import annotations

from datetime import date, datetime, timedelta
import re

from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.core.config import settings
from app.db.duckdb_client import DuckDBClient
from app.ml.predictor import get_modelai_predictor, required_raw_columns
from app.services.gcs_repository import GCSParquetRepository


router = APIRouter(prefix="/predictions", tags=["predictions"])

duckdb_client = DuckDBClient(settings.duckdb_path)
DATE_PART_PATTERN = re.compile(r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)")


def _layer_prefix_from_gold(layer: str) -> str:
    normalized_prefix = settings.gcs_prefix.strip("/")
    parts = [part for part in normalized_prefix.split("/") if part]
    if "gold" in parts:
        parts[parts.index("gold")] = layer
        return "/".join(parts)
    if parts:
        return "/".join([layer, *parts[1:]]) if parts[0] in {"silver", "bronze"} else layer
    return layer


def _repository_for_layer(layer: str) -> tuple[str, GCSParquetRepository]:
    layer_prefix = _layer_prefix_from_gold(layer)
    return layer_prefix, GCSParquetRepository(
        bucket_name=settings.gcs_bucket,
        prefix=layer_prefix,
        cache_dir=settings.cache_dir,
    )


def _extract_object_date(object_name: str) -> date | None:
    match = DATE_PART_PATTERN.search(object_name)
    if not match:
        return None
    return date.fromisoformat(match.group(1))


def _read_latest_silver_frame() -> tuple[str, GCSParquetRepository, date | None, list[str], pd.DataFrame]:
    layer_prefix, repository = _repository_for_layer("silver")
    object_names = repository.list_parquet_objects()
    latest_date = max((object_date for name in object_names if (object_date := _extract_object_date(name)) is not None), default=None)
    if latest_date is None:
        return layer_prefix, repository, None, [], pd.DataFrame()

    latest_object_names = repository.list_parquet_objects_for_date(latest_date)
    local_paths = repository.download_many(latest_object_names)
    frame = duckdb_client.read_parquet_files(local_paths)
    return layer_prefix, repository, latest_date, latest_object_names, frame


def _max_record_datetime(frame: pd.DataFrame) -> pd.Timestamp:
    record_column = "recordDatetime"
    if record_column not in frame.columns and "record_dateTime" in frame.columns:
        record_column = "record_dateTime"
    if record_column not in frame.columns:
        raise ValueError("Latest silver data is missing recordDatetime.")

    values = pd.to_datetime(frame[record_column], errors="coerce")
    if values.isna().all():
        raise ValueError("Latest silver data has no parseable recordDatetime values.")
    return values.max()


def _parse_target_datetime(value: str) -> pd.Timestamp:
    try:
        parsed = pd.to_datetime(value, errors="raise")
    except Exception as exc:
        raise HTTPException(status_code=422, detail="target_datetime must be a valid datetime string.") from exc
    if pd.isna(parsed):
        raise HTTPException(status_code=422, detail="target_datetime must be a valid datetime string.")
    return pd.Timestamp(parsed).tz_localize(None) if pd.Timestamp(parsed).tzinfo else pd.Timestamp(parsed)


@router.get("/modelai-final/health")
def modelai_health() -> dict:
    try:
        predictor = get_modelai_predictor()
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    return {
        "status": "ready",
        "model_dir": str(settings.modelai_model_dir),
        "preprocess_dir": str(settings.modelai_preprocess_dir),
        "required_columns": required_raw_columns(),
        "weights": predictor.weights,
    }


@router.get("/modelai-final/silver/by-date")
def predict_silver_by_date(
    target_date: date = Query(...),
    limit: int = Query(500, ge=1, le=10_000),
) -> dict:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    layer_prefix, repository = _repository_for_layer("silver")
    object_names = repository.list_parquet_objects_for_date(target_date)
    if not object_names:
        return {
            "layer": "silver",
            "prefix": layer_prefix,
            "target_date": target_date.isoformat(),
            "object_count": 0,
            "row_count": 0,
            "returned_rows": 0,
            "rows": [],
        }

    local_paths = repository.download_many(object_names)
    frame = duckdb_client.read_parquet_files(local_paths)

    try:
        output = get_modelai_predictor().predict_same_timestamp(frame)
    except Exception as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    returned = output.head(limit).copy()
    returned["recordDatetime"] = returned["recordDatetime"].astype(str)

    return {
        "layer": "silver",
        "prefix": layer_prefix,
        "target_date": target_date.isoformat(),
        "object_count": len(object_names),
        "row_count": int(len(output)),
        "returned_rows": int(len(returned)),
        "weights": get_modelai_predictor().weights,
        "rows": returned.to_dict(orient="records"),
    }


@router.get("/modelai-final/silver/forecast/status")
def forecast_status() -> dict:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    try:
        layer_prefix, _, latest_date, object_names, frame = _read_latest_silver_frame()
        if latest_date is None or frame.empty:
            return {
                "layer": "silver",
                "prefix": layer_prefix,
                "latest_date": None,
                "anchor_datetime": None,
                "max_forecast_datetime": None,
                "object_count": 0,
                "row_count": 0,
            }
        anchor = _max_record_datetime(frame)
    except Exception as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    return {
        "layer": "silver",
        "prefix": layer_prefix,
        "latest_date": latest_date.isoformat(),
        "anchor_datetime": anchor.isoformat(),
        "max_forecast_datetime": (anchor + timedelta(days=3)).isoformat(),
        "object_count": len(object_names),
        "row_count": int(len(frame)),
        "step_minutes": 5,
        "max_days": 3,
    }


@router.get("/modelai-final/silver/forecast")
def forecast_latest_silver(
    target_datetime: str = Query(...),
    step_minutes: int = Query(5, ge=1, le=1440),
) -> dict:
    if not settings.gcs_bucket:
        raise HTTPException(status_code=400, detail="GCS_BUCKET is not configured")

    try:
        target_ts = _parse_target_datetime(target_datetime)
        layer_prefix, _, latest_date, object_names, frame = _read_latest_silver_frame()
        if latest_date is None or frame.empty:
            return {
                "layer": "silver",
                "prefix": layer_prefix,
                "latest_date": None,
                "anchor_datetime": None,
                "forecast_datetime": target_ts.isoformat(),
                "horizon_minutes": 0,
                "object_count": 0,
                "row_count": 0,
                "rows": [],
            }

        anchor = _max_record_datetime(frame)
        if target_ts <= anchor:
            raise HTTPException(status_code=422, detail="target_datetime must be after the latest available recordDatetime.")
        max_target = anchor + timedelta(days=3)
        if target_ts > max_target:
            raise HTTPException(status_code=422, detail="target_datetime cannot be more than 3 days after latest data.")

        horizon_minutes = int(round((target_ts - anchor).total_seconds() / 60))

        output = get_modelai_predictor().forecast_at_datetime(
            frame,
            target_datetime=target_ts.to_pydatetime(),
            step_minutes=step_minutes,
            anchor_datetime=anchor.to_pydatetime(),
        )
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    returned = output.copy()
    returned["anchor_datetime"] = returned["anchor_datetime"].astype(str)
    returned["forecast_datetime"] = returned["forecast_datetime"].astype(str)
    rows = []
    for row in returned.to_dict(orient="records"):
        density = row.get("pred_avg_density")
        rows.append(
            {
                "road_name": row.get("road_name"),
                "recordDatetime": row.get("forecast_datetime"),
                "avg_density": density,
                "pred_avg_density": density,
                "pred_avg_density_scaled": row.get("pred_avg_density_scaled"),
                "horizon_minutes": row.get("horizon_minutes"),
            }
        )

    return {
        "layer": "silver",
        "prefix": layer_prefix,
        "latest_date": latest_date.isoformat(),
        "anchor_datetime": anchor.isoformat(),
        "forecast_datetime": target_ts.isoformat(),
        "horizon_minutes": horizon_minutes,
        "object_count": len(object_names),
        "row_count": len(rows),
        "weights": get_modelai_predictor().weights,
        "rows": rows,
    }
