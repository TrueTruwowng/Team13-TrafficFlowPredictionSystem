from __future__ import annotations

import pickle
from datetime import datetime, timedelta
from functools import lru_cache
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from app.core.config import settings


ST_FEATURES = [
    "neighbor_density_hop1",
    "neighbor_density_hop2",
    "spatial_speed_diff",
    "spatial_density_ratio",
    "traffic_pressure_index",
    "density_acceleration",
    "speed_anomaly_index",
    "spatial_density_share",
    "traffic_resistance",
    "node_centrality",
    "st_momentum",
]


def load_pickle(path: Path):
    with path.open("rb") as handle:
        try:
            return pickle.load(handle)
        except Exception:
            pass

    try:
        import joblib
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            f"Could not read {path.name} with pickle. Install joblib to load this artifact format."
        ) from exc

    return joblib.load(path)


def normalize_input_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    rename_map = {}
    if "roadName" in renamed.columns and "road_name" not in renamed.columns:
        rename_map["roadName"] = "road_name"
    if "record_dateTime" in renamed.columns and "recordDatetime" not in renamed.columns:
        rename_map["record_dateTime"] = "recordDatetime"
    if rename_map:
        renamed = renamed.rename(columns=rename_map)
    return renamed


def required_raw_columns() -> list[str]:
    return [
        "road_name",
        "recordDatetime",
        "weather",
        "avg_speed",
        "avg_density",
        "avg_occupancy",
        "avg_waitingTime",
        "avg_traveltime",
        "avg_flow",
        "total_entered",
        "total_left",
        "avg_timeloss",
        "temperature",
        "windspeed",
        "precipitation",
    ]


def missing_columns(df: pd.DataFrame, columns: Iterable[str]) -> list[str]:
    return [column for column in columns if column not in df.columns]


class OptimizedDualBranchSTNet:
    @staticmethod
    def build(input_dim: int, hidden_channels: int, num_nodes: int):
        import torch
        import torch.nn as nn
        import torch.nn.functional as F

        class _Net(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.num_nodes = num_nodes
                self.spatial_branch = nn.Sequential(
                    nn.Linear(5, hidden_channels),
                    nn.GELU(),
                    nn.Linear(hidden_channels, hidden_channels),
                )
                self.temporal_branch = nn.Sequential(
                    nn.Linear(input_dim + 1 - 5, hidden_channels),
                    nn.GELU(),
                    nn.Linear(hidden_channels, hidden_channels),
                )
                self.attn_gate = nn.Sequential(nn.Linear(hidden_channels, hidden_channels), nn.Sigmoid())
                self.fusion_fc = nn.Linear(hidden_channels * 2, hidden_channels)
                self.regression = nn.Linear(hidden_channels, 1)
                self.dropout = nn.Dropout(0.15)
                self.bn_s = nn.BatchNorm1d(hidden_channels)
                self.bn_t = nn.BatchNorm1d(hidden_channels)
                self.bn_f = nn.BatchNorm1d(hidden_channels)

            def forward(self, x):
                road_ids = torch.clamp(x[:, 15].long(), 0, self.num_nodes - 1)
                dummy_node_degrees = torch.ones(self.num_nodes, device=x.device)
                s_feat = torch.cat([dummy_node_degrees[road_ids].unsqueeze(1), x[:, -4:]], dim=1)
                t_feat = x[:, :-4]
                out_s = self.dropout(F.gelu(self.bn_s(self.spatial_branch(s_feat))))
                out_t = self.dropout(F.gelu(self.bn_t(self.temporal_branch(t_feat))))
                combined = torch.cat([out_s, out_t * self.attn_gate(out_s)], dim=1)
                return self.regression(self.dropout(F.gelu(self.bn_f(self.fusion_fc(combined))))).squeeze(-1)

        return _Net()


class ModelAIPredictor:
    def __init__(self, model_dir: Path, preprocess_dir: Path) -> None:
        self.model_dir = model_dir
        self.preprocess_dir = preprocess_dir
        self.ensemble = self._load_ensemble()

    def _load_ensemble(self) -> dict:
        try:
            import lightgbm as lgb
            import torch
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "ModelAI inference needs lightgbm and torch installed in the backend environment."
            ) from exc

        topo = load_pickle(self.preprocess_dir / "topology_config.pkl")
        scaler = load_pickle(self.preprocess_dir / "minmax_scaler.pkl")
        label_encoder = load_pickle(self.preprocess_dir / "label_encoder.pkl")

        with (self.model_dir / "ensemble_config.txt").open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
        lgb_weight = float(lines[1].split(":")[1].strip())
        st_weight = float(lines[2].split(":")[1].strip())

        gbm = lgb.Booster(model_file=str(self.model_dir / "lgbm_final.txt"))
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        stgcn = OptimizedDualBranchSTNet.build(len(topo["features"]) + 11, 256, topo["num_nodes_safe"]).to(device)
        stgcn.load_state_dict(torch.load(self.model_dir / "stgcn_final.pt", map_location=device))
        stgcn.eval()

        return {
            "topo": topo,
            "scaler": scaler,
            "label_encoder": label_encoder,
            "gbm": gbm,
            "stgcn": stgcn,
            "lgb_weight": lgb_weight,
            "st_weight": st_weight,
            "device": device,
        }

    @property
    def weights(self) -> dict[str, float]:
        return {
            "lightgbm": float(self.ensemble["lgb_weight"]),
            "stgcn": float(self.ensemble["st_weight"]),
        }

    def _build_features(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        topo = self.ensemble["topo"]
        label_encoder = self.ensemble["label_encoder"]
        scaler = self.ensemble["scaler"]
        frame = normalize_input_columns(df)

        missing = missing_columns(frame, required_raw_columns())
        if missing:
            raise ValueError("Input parquet is missing required raw columns: " + ", ".join(missing))

        frame = frame.copy()
        frame["recordDatetime"] = pd.to_datetime(frame["recordDatetime"], errors="coerce")
        if frame["recordDatetime"].isna().any():
            raise ValueError("Some recordDatetime values could not be parsed.")

        known_roads = set(label_encoder.classes_)
        frame = frame[frame["road_name"].isin(known_roads)].copy()
        if frame.empty:
            raise ValueError("No rows with road_name known by the label encoder.")

        frame["hour"] = frame["recordDatetime"].dt.hour
        frame["minute"] = frame["recordDatetime"].dt.minute
        frame["day_of_week"] = frame["recordDatetime"].dt.dayofweek
        frame["is_weekend"] = (frame["day_of_week"] >= 5).astype(int)
        frame["road_id_encoded"] = label_encoder.transform(frame["road_name"])

        frame = pd.get_dummies(frame, columns=["weather"], drop_first=False)

        weather_columns = [feature for feature in topo["features"] if feature.startswith("weather_")]
        for column in weather_columns:
            if column not in frame.columns:
                frame[column] = 0

        frame[topo["existing_numeric"]] = scaler.transform(frame[topo["existing_numeric"]])

        frame = frame.sort_values(by=["road_id_encoded", "recordDatetime"]).copy()
        for lag in range(1, 4):
            frame[f"avg_density_lag_{lag}"] = frame.groupby("road_id_encoded", sort=False)["avg_density"].shift(lag)
            frame[f"avg_speed_lag_{lag}"] = frame.groupby("road_id_encoded", sort=False)["avg_speed"].shift(lag)

        frame["neighbor_density_hop1"] = 0.0
        frame["neighbor_density_hop2"] = 0.0
        for road_id in range(topo["num_nodes_safe"]):
            neighbors_1 = list(topo["adj_dict"].get(road_id, set()))
            neighbors_2 = list(topo["hop2_dict"].get(road_id, set()))
            if neighbors_1:
                frame.loc[frame["road_id_encoded"] == road_id, "neighbor_density_hop1"] = (
                    frame.loc[frame["road_id_encoded"].isin(neighbors_1), topo["density_col"]].mean()
                )
            if neighbors_2:
                frame.loc[frame["road_id_encoded"] == road_id, "neighbor_density_hop2"] = (
                    frame.loc[frame["road_id_encoded"].isin(neighbors_2), topo["density_col"]].mean()
                )

        frame["spatial_speed_diff"] = frame[topo["speed_col"]] - frame["neighbor_density_hop1"]
        frame["spatial_density_ratio"] = frame[topo["density_col"]] / (frame["neighbor_density_hop1"] + 1e-5)
        frame["traffic_pressure_index"] = frame["neighbor_density_hop1"] * frame["neighbor_density_hop2"]
        frame["density_acceleration"] = frame[topo["density_col"]] - frame[topo["lag2_col"]]
        frame["speed_anomaly_index"] = (frame[topo["speed_col"]] - frame["neighbor_density_hop1"]).abs()
        frame["spatial_density_share"] = frame[topo["density_col"]] / (
            frame["neighbor_density_hop1"] + frame[topo["density_col"]] + 1e-5
        )
        frame["traffic_resistance"] = frame[topo["speed_col"]] * (frame["neighbor_density_hop1"] + 1e-5)
        frame["node_centrality"] = frame["neighbor_density_hop1"] + 0.5 * frame["neighbor_density_hop2"]
        frame["st_momentum"] = frame["density_acceleration"] * frame["traffic_pressure_index"]

        numeric_cols = frame.select_dtypes(include=[np.number]).columns
        frame[numeric_cols] = frame[numeric_cols].replace([np.inf, -np.inf], 0).fillna(0)

        final_features = topo["features"] + ST_FEATURES
        missing_features = missing_columns(frame, final_features)
        if missing_features:
            raise ValueError("Feature engineering missed model inputs: " + ", ".join(missing_features))

        return frame, final_features

    def _inverse_density(self, values: np.ndarray) -> np.ndarray:
        topo = self.ensemble["topo"]
        scaler = self.ensemble["scaler"]
        values = np.nan_to_num(values, nan=0.0, posinf=1.0, neginf=0.0)
        values = np.clip(values, -1.0, 2.0)
        dummy = np.zeros((len(values), len(topo["existing_numeric"])))
        density_idx = topo["existing_numeric"].index("avg_density")
        dummy[:, density_idx] = values
        return scaler.inverse_transform(dummy)[:, density_idx]

    def predict_same_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        import torch

        prepared, features = self._build_features(df)
        x = prepared[features]
        pred_lgb = self.ensemble["gbm"].predict(x)

        with torch.no_grad():
            tensor_x = torch.tensor(x.values.astype(np.float32), device=self.ensemble["device"])
            pred_stgcn = self.ensemble["stgcn"](tensor_x).detach().cpu().numpy().reshape(-1)

        pred_scaled = self.ensemble["lgb_weight"] * pred_lgb + self.ensemble["st_weight"] * pred_stgcn
        pred_real = self._inverse_density(pred_scaled)

        output = prepared[["road_name", "recordDatetime"]].copy()
        output["pred_avg_density_scaled"] = pred_scaled
        output["pred_avg_density"] = pred_real
        output["prediction_target"] = "avg_density at the same recordDatetime in the input row"
        if "avg_density" in prepared.columns:
            output["actual_avg_density_scaled"] = prepared["avg_density"].to_numpy()
            output["actual_avg_density"] = self._inverse_density(prepared["avg_density"].to_numpy())
        return output

    def _build_base_scaled_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        topo = self.ensemble["topo"]
        label_encoder = self.ensemble["label_encoder"]
        scaler = self.ensemble["scaler"]
        frame = normalize_input_columns(df)

        missing = missing_columns(frame, required_raw_columns())
        if missing:
            raise ValueError("Input parquet is missing required raw columns: " + ", ".join(missing))

        frame = frame.copy()
        frame["recordDatetime"] = pd.to_datetime(frame["recordDatetime"], errors="coerce")
        if frame["recordDatetime"].isna().any():
            raise ValueError("Some recordDatetime values could not be parsed.")

        known_roads = set(label_encoder.classes_)
        frame = frame[frame["road_name"].isin(known_roads)].copy()
        if frame.empty:
            raise ValueError("No rows with road_name known by the label encoder.")

        frame["hour"] = frame["recordDatetime"].dt.hour
        frame["minute"] = frame["recordDatetime"].dt.minute
        frame["day_of_week"] = frame["recordDatetime"].dt.dayofweek
        frame["is_weekend"] = (frame["day_of_week"] >= 5).astype(int)
        frame["road_id_encoded"] = label_encoder.transform(frame["road_name"])

        frame = pd.get_dummies(frame, columns=["weather"], drop_first=False)
        weather_columns = [feature for feature in topo["features"] if feature.startswith("weather_")]
        for column in weather_columns:
            if column not in frame.columns:
                frame[column] = 0

        frame[topo["existing_numeric"]] = scaler.transform(frame[topo["existing_numeric"]])
        return frame.sort_values(by=["road_id_encoded", "recordDatetime"]).reset_index(drop=True)

    def forecast_at_datetime(
        self,
        raw_df: pd.DataFrame,
        target_datetime: datetime,
        step_minutes: int = 5,
        anchor_datetime: datetime | None = None,
    ) -> pd.DataFrame:
        import torch

        if step_minutes <= 0:
            raise ValueError("step_minutes must be > 0")

        topo = self.ensemble["topo"]
        frame = self._build_base_scaled_frame(raw_df)
        anchor_ts = pd.Timestamp(anchor_datetime) if anchor_datetime is not None else frame["recordDatetime"].max()
        frame = frame[frame["recordDatetime"] <= anchor_ts].copy()
        if frame.empty:
            raise ValueError(f"No usable rows at or before forecast anchor: {anchor_ts}")

        target_ts = pd.Timestamp(target_datetime)
        if target_ts <= anchor_ts:
            raise ValueError("target_datetime must be after the latest available recordDatetime.")

        horizon_minutes = int(round((target_ts - anchor_ts).total_seconds() / 60))
        if horizon_minutes <= 0:
            raise ValueError("target_datetime must be at least 1 minute after the forecast anchor.")

        feature_order = topo["features"] + ST_FEATURES
        road_ids = sorted(frame["road_id_encoded"].unique().tolist())

        states: dict[int, dict] = {}
        for road_id in road_ids:
            road_hist = frame[frame["road_id_encoded"] == road_id].sort_values("recordDatetime")
            if road_hist.empty:
                continue
            last_row = road_hist.iloc[-1]
            density_hist = road_hist["avg_density"].tail(3).tolist()
            speed_hist = road_hist["avg_speed"].tail(3).tolist()

            while len(density_hist) < 3:
                density_hist.insert(0, density_hist[0] if density_hist else 0.0)
            while len(speed_hist) < 3:
                speed_hist.insert(0, speed_hist[0] if speed_hist else 0.0)

            base_values = {
                feature: float(last_row[feature]) if feature in last_row and pd.notna(last_row[feature]) else 0.0
                for feature in topo["features"]
            }
            base_values["road_id_encoded"] = float(road_id)
            states[int(road_id)] = {
                "road_name": str(last_row["road_name"]),
                "base": base_values,
                "density_lags": density_hist,
                "speed_lags": speed_hist,
            }

        if not states:
            raise ValueError("No usable road states found for forecasting.")

        steps = math.ceil(horizon_minutes / step_minutes)
        pred_scaled = np.array([])
        batch_road_ids: list[int] = []
        future_time = anchor_ts

        for step in range(1, steps + 1):
            future_time = target_ts if step == steps else anchor_ts + timedelta(minutes=step * step_minutes)
            lag1_by_road = {rid: float(np.clip(states[rid]["density_lags"][-1], -1.0, 2.0)) for rid in states}
            hop1_by_road = {}
            hop2_by_road = {}
            for rid in states:
                n1 = [n for n in topo["adj_dict"].get(rid, set()) if n in lag1_by_road]
                n2 = [n for n in topo["hop2_dict"].get(rid, set()) if n in lag1_by_road]
                hop1_by_road[rid] = float(np.mean([lag1_by_road[n] for n in n1])) if n1 else 0.0
                hop2_by_road[rid] = float(np.mean([lag1_by_road[n] for n in n2])) if n2 else 0.0

            batch_rows = []
            batch_road_ids = []
            for rid in sorted(states):
                state = states[rid]
                row = dict(state["base"])
                row["hour"] = float(future_time.hour)
                row["minute"] = float(future_time.minute)
                row["day_of_week"] = float(future_time.dayofweek)
                row["is_weekend"] = float(1 if future_time.dayofweek >= 5 else 0)

                row["avg_density_lag_1"] = float(state["density_lags"][-1])
                row["avg_density_lag_2"] = float(state["density_lags"][-2])
                row["avg_density_lag_3"] = float(state["density_lags"][-3])
                row["avg_speed_lag_1"] = float(state["speed_lags"][-1])
                row["avg_speed_lag_2"] = float(state["speed_lags"][-2])
                row["avg_speed_lag_3"] = float(state["speed_lags"][-3])

                row["neighbor_density_hop1"] = float(np.clip(hop1_by_road[rid], -1.0, 2.0))
                row["neighbor_density_hop2"] = float(np.clip(hop2_by_road[rid], -1.0, 2.0))
                row["spatial_speed_diff"] = row[topo["speed_col"]] - row["neighbor_density_hop1"]
                row["spatial_density_ratio"] = row[topo["density_col"]] / (row["neighbor_density_hop1"] + 1e-5)
                row["traffic_pressure_index"] = row["neighbor_density_hop1"] * row["neighbor_density_hop2"]
                row["density_acceleration"] = row[topo["density_col"]] - row[topo["lag2_col"]]
                row["speed_anomaly_index"] = abs(row[topo["speed_col"]] - row["neighbor_density_hop1"])
                row["spatial_density_share"] = row[topo["density_col"]] / (
                    row["neighbor_density_hop1"] + row[topo["density_col"]] + 1e-5
                )
                row["traffic_resistance"] = row[topo["speed_col"]] * (row["neighbor_density_hop1"] + 1e-5)
                row["node_centrality"] = row["neighbor_density_hop1"] + 0.5 * row["neighbor_density_hop2"]
                row["st_momentum"] = row["density_acceleration"] * row["traffic_pressure_index"]

                batch_rows.append(row)
                batch_road_ids.append(rid)

            x = pd.DataFrame(batch_rows)[feature_order]
            x = x.replace([np.inf, -np.inf], 0).fillna(0)
            numeric_cols = x.select_dtypes(include=[np.number]).columns
            x[numeric_cols] = x[numeric_cols].clip(-1000, 1000)

            pred_lgb = self.ensemble["gbm"].predict(x)
            with torch.no_grad():
                tensor_x = torch.tensor(x.values.astype(np.float32), device=self.ensemble["device"])
                pred_stgcn = self.ensemble["stgcn"](tensor_x).detach().cpu().numpy().reshape(-1)

            pred_scaled = self.ensemble["lgb_weight"] * pred_lgb + self.ensemble["st_weight"] * pred_stgcn
            pred_scaled = np.nan_to_num(pred_scaled, nan=0.0, posinf=2.0, neginf=-1.0)
            pred_scaled = np.clip(pred_scaled, -1.0, 2.0)

            for i, rid in enumerate(batch_road_ids):
                states[rid]["density_lags"] = states[rid]["density_lags"][1:] + [float(pred_scaled[i])]
                states[rid]["speed_lags"] = states[rid]["speed_lags"][1:] + [float(states[rid]["base"]["avg_speed"])]

        pred_real = self._inverse_density(pred_scaled)
        return pd.DataFrame(
            [
                {
                    "road_name": states[rid]["road_name"],
                    "anchor_datetime": anchor_ts,
                    "forecast_datetime": future_time,
                    "horizon_minutes": horizon_minutes,
                    "pred_avg_density_scaled": float(pred_scaled[i]),
                    "pred_avg_density": float(pred_real[i]),
                }
                for i, rid in enumerate(batch_road_ids)
            ]
        )


@lru_cache(maxsize=1)
def get_modelai_predictor() -> ModelAIPredictor:
    return ModelAIPredictor(settings.modelai_model_dir, settings.modelai_preprocess_dir)
