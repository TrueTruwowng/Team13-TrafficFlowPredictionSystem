from __future__ import annotations

from datetime import date, timedelta
from dataclasses import dataclass
from pathlib import Path

from google.cloud import storage

from app.core.config import settings


@dataclass
class DatasetObject:
    name: str
    local_path: Path


class GCSParquetRepository:
    def __init__(self, bucket_name: str, prefix: str = "", cache_dir: Path | None = None) -> None:
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")
        self.cache_dir = cache_dir or settings.cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._client: storage.Client | None = None

    @property
    def client(self) -> storage.Client:
        if self._client is None:
            self._client = storage.Client(project=settings.gcp_project or None)
        return self._client

    def _normalized_prefix(self) -> str:
        return self.prefix.strip("/")

    def _prefix_for_date(self, target_date: date) -> str:
        date_prefix = f"date={target_date.isoformat()}/"
        base_prefix = self._normalized_prefix()
        return f"{base_prefix}/{date_prefix}" if base_prefix else date_prefix

    def _list_parquet_objects_with_prefix(self, prefix: str) -> list[str]:
        bucket = self.client.bucket(self.bucket_name)
        blobs = self.client.list_blobs(bucket, prefix=prefix or None)
        return sorted(blob.name for blob in blobs if str(blob.name).lower().endswith(".parquet"))

    def list_parquet_objects(self) -> list[str]:
        return self._list_parquet_objects_with_prefix(self._normalized_prefix())

    def list_parquet_objects_for_date(self, target_date: date) -> list[str]:
        return self._list_parquet_objects_with_prefix(self._prefix_for_date(target_date))

    def list_parquet_objects_for_date_range(self, start_date: date, end_date: date) -> list[str]:
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        objects: list[str] = []
        current_date = start_date
        while current_date <= end_date:
            objects.extend(self.list_parquet_objects_for_date(current_date))
            current_date += timedelta(days=1)

        return sorted(set(objects))

    def download_object(self, object_name: str) -> Path:
        safe_name = object_name.replace("/", "__")
        local_path = self.cache_dir / safe_name
        if local_path.exists():
            return local_path

        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(object_name)
        blob.download_to_filename(str(local_path))
        return local_path

    def download_many(self, object_names: list[str]) -> list[Path]:
        return [self.download_object(name) for name in object_names]
