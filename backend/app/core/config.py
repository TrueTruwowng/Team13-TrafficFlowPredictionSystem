from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
MAPS_DIR = BASE_DIR / "app" / "utils" / "maps"
ML_ARTIFACTS_DIR = BASE_DIR / "app" / "ml" / "artifacts"
load_dotenv(BASE_DIR / ".env")


def _split_csv(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def _path_from_env(name: str, default: Path) -> Path:
    value = os.getenv(name, "").strip()
    return Path(value) if value else default


@dataclass(frozen=True)
class Settings:
    gcs_bucket: str = field(default_factory=lambda: os.getenv("GCS_BUCKET", ""))
    gcs_prefix: str = field(default_factory=lambda: os.getenv("GCS_PREFIX", ""))
    gcp_project: str = field(default_factory=lambda: os.getenv("GCP_PROJECT", ""))
    duckdb_path: str = field(default_factory=lambda: os.getenv("DUCKDB_PATH", ":memory:"))
    cache_dir: Path = field(
        default_factory=lambda: Path(os.getenv("DASHBOARD_CACHE_DIR", Path(__file__).resolve().parents[3] / ".cache" / "parquet"))
    )
    cors_origins: list[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("CORS_ORIGINS"),
            ["http://localhost:3000", "http://127.0.0.1:3000"],
        )
    )
    nghia_do_net_xml_path: Path = field(
        default_factory=lambda: _path_from_env("NGHIA_DO_NET_XML_PATH", MAPS_DIR / "nghia_do.net.xml")
    )
    nghia_do_osm_xml_path: Path = field(
        default_factory=lambda: _path_from_env("NGHIA_DO_OSM_XML_PATH", MAPS_DIR / "nghia_do_cut.osm.xml")
    )
    modelai_model_dir: Path = field(
        default_factory=lambda: _path_from_env("MODELAI_MODEL_DIR", ML_ARTIFACTS_DIR / "modelai_final")
    )
    modelai_preprocess_dir: Path = field(
        default_factory=lambda: _path_from_env("MODELAI_PREPROCESS_DIR", ML_ARTIFACTS_DIR / "preprocessing_state")
    )


settings = Settings()
