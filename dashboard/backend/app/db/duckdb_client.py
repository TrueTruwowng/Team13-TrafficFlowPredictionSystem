from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


class DuckDBClient:
    def __init__(self, database_path: str = ":memory:") -> None:
        self.connection = duckdb.connect(database_path)

    @staticmethod
    def _build_path_sql(parquet_paths: list[Path]) -> str:
        return ", ".join("'{}'".format(path.as_posix().replace("'", "''")) for path in parquet_paths)

    @staticmethod
    def _read_parquet_source(path_sql: str) -> str:
        return f"read_parquet([{path_sql}], union_by_name=true)"

    def preview_parquet(self, parquet_paths: list[Path], limit: int = 50) -> pd.DataFrame:
        if not parquet_paths:
            return pd.DataFrame()
        path_sql = self._build_path_sql(parquet_paths)
        query = f"SELECT * FROM {self._read_parquet_source(path_sql)} LIMIT {int(limit)}"
        return self.connection.execute(query).df()

    def read_parquet_files(self, parquet_paths: list[Path]) -> pd.DataFrame:
        if not parquet_paths:
            return pd.DataFrame()
        path_sql = self._build_path_sql(parquet_paths)
        query = f"SELECT * FROM {self._read_parquet_source(path_sql)}"
        return self.connection.execute(query).df()

    def count_unique_roads(self, parquet_paths: list[Path]) -> int:
        if not parquet_paths:
            return 0
        frame = self.read_parquet_files(parquet_paths)
        if frame.empty:
            return 0

        road_column = None
        for candidate in ("road_name", "roadName"):
            if candidate in frame.columns:
                road_column = candidate
                break

        if road_column is None:
            return 0

        return int(frame[road_column].dropna().astype(str).nunique())

    def list_unique_roads(self, parquet_paths: list[Path]) -> list[str]:
        if not parquet_paths:
            return []

        frame = self.read_parquet_files(parquet_paths)
        if frame.empty:
            return []

        road_column = None
        for candidate in ("road_name", "roadName"):
            if candidate in frame.columns:
                road_column = candidate
                break

        if road_column is None:
            return []

        roads = (
            frame[road_column]
            .dropna()
            .astype(str)
            .map(str.strip)
        )
        return sorted({road for road in roads if road})

    def stats_parquet(self, parquet_paths: list[Path]) -> pd.DataFrame:
        if not parquet_paths:
            return pd.DataFrame()
        path_sql = self._build_path_sql(parquet_paths)
        query = f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(*) FILTER (WHERE road_name IS NOT NULL) AS rows_with_road_name,
            COUNT(DISTINCT road_name) AS unique_roads,
            COUNT(DISTINCT weather) AS unique_weather,
            MIN(recordDatetime) AS min_record_datetime,
            MAX(recordDatetime) AS max_record_datetime
        FROM {self._read_parquet_source(path_sql)}
        """
        return self.connection.execute(query).df()

    def list_numeric_columns(self, parquet_paths: list[Path]) -> list[str]:
        if not parquet_paths:
            return []
        path_sql = self._build_path_sql(parquet_paths)
        query = f"DESCRIBE SELECT * FROM {self._read_parquet_source(path_sql)}"
        schema_df = self.connection.execute(query).df()
        numeric_types = ("INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "SMALLINT", "TINYINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT")
        return [
            row["column_name"]
            for _, row in schema_df.iterrows()
            if str(row.get("column_type", "")).upper().startswith(numeric_types)
        ]
