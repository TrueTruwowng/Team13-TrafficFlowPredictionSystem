"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { fetchJson, getApiBaseUrl } from "../../lib/api";
import type { ForecastLayerResponse, ForecastStatusResponse, LatestLayerDataResponse, LayerDatesResponse, MapGeometryResponse, RealtimeLayerDataResponse } from "../../lib/types";

type LoadState = "loading" | "ready" | "error";
type VisualizationMode = "historical" | "realtime" | "forecast";
type RealtimeIntervalOption = "3" | "5" | "10" | "15" | "custom";
type ForecastHorizonOption = "30" | "60" | "180" | "360" | "720" | "1440" | "custom";

type DensityRecord = {
  keys: string[];
  roadName: string | null;
  minute: number | null;
  density: number | null;
};

type FeatureDensity = {
  density: number;
  rows: number;
};

type DensityRange = {
  min: number;
  max: number;
};

type LeafletMap = {
  remove: () => void;
  fitBounds: (bounds: unknown, options?: unknown) => void;
};

type LeafletLayer = {
  addTo: (map: LeafletMap) => LeafletLayer;
  getBounds: () => unknown;
  bindPopup?: (content: string) => void;
};

type LeafletApi = {
  map: (element: HTMLElement) => LeafletMap;
  tileLayer: (url: string, options: Record<string, unknown>) => LeafletLayer;
  geoJSON: (data: unknown, options: Record<string, unknown>) => LeafletLayer;
};

declare global {
  interface Window {
    L?: LeafletApi;
  }
}

const LEAFLET_CSS_URL = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
const LEAFLET_JS_URL = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
const ROAD_KEY_FIELDS = ["edge_id", "edgeId", "sumo_edge_id", "sumoEdgeId", "road_id", "roadId", "link_id", "linkId", "way_id", "wayId", "segment_id"];
const ROAD_NAME_FIELDS = ["road_name", "roadName", "name", "street_name", "streetName"];
const DENSITY_FIELDS = ["avg_density", "avgDensity", "average_density", "density"];
const TIME_FIELDS = ["recordDatetime", "record_datetime", "datetime", "timestamp", "time", "interval_start", "window_start"];

function formatNumber(value: number, digits = 0): string {
  return new Intl.NumberFormat("vi-VN", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function valueAsText(row: Record<string, unknown>, fields: string[]): string | null {
  for (const field of fields) {
    const value = row[field];
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return null;
}

function normalizeRoadName(value: string | null): string {
  if (!value) return "";
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase()
    .replace(/^\s*\d+[a-z]?(?:[/-]\d+[a-z]?)?\s+/i, "")
    .replace(/^(duong|pho|ngo|ngach|hem)\s+/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function roadKeysFromText(value: string | null): string[] {
  if (!value) return [];
  const raw = value.trim();
  const withoutDirection = raw.replace(/^-/, "");
  const withoutPart = withoutDirection.split("#", 1)[0];
  return Array.from(new Set([raw, withoutDirection, withoutPart, normalizeRoadName(raw)].filter(Boolean)));
}

function parseMinute(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return Math.max(0, Math.min(1439, Math.round(value)));
  if (typeof value !== "string" || !value.trim()) return null;

  const text = value.trim();
  const timeMatch = /(?:^|T|\s)(\d{1,2}):(\d{2})(?::\d{2})?/.exec(text);
  if (timeMatch) return Number(timeMatch[1]) * 60 + Number(timeMatch[2]);

  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) return parsed.getHours() * 60 + parsed.getMinutes();
  return null;
}

function minuteToTimeInput(minute: number): string {
  if (minute >= 1440) return "24:00";
  const safeMinute = Math.max(0, Math.min(1439, minute));
  const hours = Math.floor(safeMinute / 60);
  const minutes = safeMinute % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

function dateInputValue(value: Date): string {
  return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
}

function addMinutes(value: Date, minutes: number): Date {
  return new Date(value.getTime() + minutes * 60_000);
}

function dateAtMinute(dateText: string, minute: number): Date {
  const [year, month, day] = dateText.split("-").map(Number);
  const safeMinute = Math.max(0, Math.min(1435, Math.round(minute / 5) * 5));
  return new Date(year, (month || 1) - 1, day || 1, Math.floor(safeMinute / 60), safeMinute % 60, 0, 0);
}

function toApiDateTime(value: Date): string {
  return `${dateInputValue(value)}T${String(value.getHours()).padStart(2, "0")}:${String(value.getMinutes()).padStart(2, "0")}:00`;
}

function minuteOfDay(value: Date): number {
  return value.getHours() * 60 + value.getMinutes();
}

function forecastStepMinutes(option: ForecastHorizonOption): number {
  return option === "custom" ? 5 : Number(option);
}

function colorForDensity(value: number | null, minDensity: number, maxDensity: number): string {
  if (value === null || !Number.isFinite(value)) return "#9aa3ad";
  const ratio = maxDensity > minDensity ? Math.max(0, Math.min(1, (value - minDensity) / (maxDensity - minDensity))) : 0;
  if (ratio < 0.25) return "#2ecc71";
  if (ratio < 0.5) return "#f1c40f";
  if (ratio < 0.75) return "#e67e22";
  return "#e74c3c";
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (char) => {
    switch (char) {
      case "&":
        return "&amp;";
      case "<":
        return "&lt;";
      case ">":
        return "&gt;";
      case '"':
        return "&quot;";
      case "'":
        return "&#039;";
      default:
        return char;
    }
  });
}

function toDensityRecord(row: Record<string, unknown>): DensityRecord {
  const keyText = valueAsText(row, ROAD_KEY_FIELDS);
  const roadName = valueAsText(row, ROAD_NAME_FIELDS);
  const density = toNumber(valueAsText(row, DENSITY_FIELDS));
  let minute: number | null = null;

  for (const field of TIME_FIELDS) {
    minute = parseMinute(row[field]);
    if (minute !== null) break;
  }

  return {
    keys: roadKeysFromText(keyText),
    roadName,
    minute,
    density,
  };
}

function latestMinuteFromRows(rows: Record<string, unknown>[]): number | null {
  const minutes = rows
    .map(toDensityRecord)
    .map((record) => record.minute)
    .filter((minute): minute is number => minute !== null);
  return minutes.length ? Math.max(...minutes) : null;
}

function buildDensityByKey(records: DensityRecord[], selectedMinute: number | null): Map<string, FeatureDensity> {
  const buckets = new Map<string, { sum: number; rows: number }>();
  const effectiveMinute = selectedMinute === 1440 ? 1437 : selectedMinute;
  const filteredRecords = effectiveMinute === null ? records : records.filter((record) => record.minute !== null && Math.round(record.minute / 3) * 3 === effectiveMinute);

  for (const record of filteredRecords) {
    if (record.density === null) continue;
    const keys = record.keys.length ? record.keys : record.roadName ? [record.roadName.toLowerCase(), normalizeRoadName(record.roadName)] : [];
    for (const key of keys) {
      const bucket = buckets.get(key) ?? { sum: 0, rows: 0 };
      bucket.sum += record.density;
      bucket.rows += 1;
      buckets.set(key, bucket);
    }
  }

  const densities = new Map<string, FeatureDensity>();
  for (const [key, bucket] of buckets.entries()) {
    densities.set(key, { density: bucket.sum / bucket.rows, rows: bucket.rows });
  }
  return densities;
}

function keysForFeature(feature: MapGeometryResponse["features"][number]): string[] {
  const keys = [feature.osm_way_id, ...feature.sumo_edge_ids.flatMap((edgeId) => roadKeysFromText(edgeId))];
  if (feature.name) keys.push(feature.name, feature.name.toLowerCase(), normalizeRoadName(feature.name));
  return keys;
}

function buildDailyDensityRangeByKey(records: DensityRecord[]): Map<string, DensityRange> {
  const ranges = new Map<string, DensityRange>();

  for (const record of records) {
    if (record.density === null) continue;
    const keys = record.keys.length ? record.keys : record.roadName ? [record.roadName.toLowerCase(), normalizeRoadName(record.roadName)] : [];
    for (const key of keys) {
      const current = ranges.get(key);
      if (!current) {
        ranges.set(key, { min: record.density, max: record.density });
      } else {
        current.min = Math.min(current.min, record.density);
        current.max = Math.max(current.max, record.density);
      }
    }
  }

  return ranges;
}

function findDensityForFeature(feature: MapGeometryResponse["features"][number], densities: Map<string, FeatureDensity>): FeatureDensity | null {
  for (const key of keysForFeature(feature)) {
    const density = densities.get(key);
    if (density) return density;
  }
  return null;
}

function findRangeForFeature(feature: MapGeometryResponse["features"][number], ranges: Map<string, DensityRange>): DensityRange | null {
  for (const key of keysForFeature(feature)) {
    const range = ranges.get(key);
    if (range) return range;
  }
  return null;
}

export default function VisualizationPage() {
  const mapElementRef = useRef<HTMLDivElement | null>(null);
  const mapInstanceRef = useRef<LeafletMap | null>(null);
  const [mapGeometry, setMapGeometry] = useState<MapGeometryResponse | null>(null);
  const [visualizationMode, setVisualizationMode] = useState<VisualizationMode>("historical");
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState("");
  const [loadedDate, setLoadedDate] = useState("");
  const [payload, setPayload] = useState<LatestLayerDataResponse | null>(null);
  const [mapState, setMapState] = useState<LoadState>("loading");
  const [dataState, setDataState] = useState<LoadState>("loading");
  const [error, setError] = useState<string | null>(null);
  const [sliderMinute, setSliderMinute] = useState(0);
  const [submittedMinute, setSubmittedMinute] = useState<number | null>(null);
  const [leafletReady, setLeafletReady] = useState(false);
  const [lastRealtimeUpdate, setLastRealtimeUpdate] = useState<Date | null>(null);
  const [realtimeIntervalOption, setRealtimeIntervalOption] = useState<RealtimeIntervalOption>("15");
  const [customRealtimeIntervalSeconds, setCustomRealtimeIntervalSeconds] = useState(15);
  const [forecastStatus, setForecastStatus] = useState<ForecastStatusResponse | null>(null);
  const [forecastRows, setForecastRows] = useState<Record<string, unknown>[]>([]);
  const [forecastHorizonOption, setForecastHorizonOption] = useState<ForecastHorizonOption>("60");
  const [forecastDate, setForecastDate] = useState("");
  const [forecastMinute, setForecastMinute] = useState(0);
  const [submittedForecast, setSubmittedForecast] = useState<ForecastLayerResponse | null>(null);
  const [osNow, setOsNow] = useState<Date | null>(null);
  const realtimeIntervalSeconds =
    realtimeIntervalOption === "custom" ? Math.max(3, customRealtimeIntervalSeconds || 3) : Number(realtimeIntervalOption);

  useEffect(() => {
    function updateOsNow() {
      setOsNow(new Date());
    }

    updateOsNow();
    const intervalId = window.setInterval(updateOsNow, 60_000);
    return () => window.clearInterval(intervalId);
  }, []);

  useEffect(() => {
    let isCancelled = false;

    async function loadDateData(targetDate: string) {
      const apiBase = getApiBaseUrl();
      setDataState("loading");
      const dayPayload = await fetchJson<LatestLayerDataResponse>(`${apiBase}/datasets/silver/by-date/data?target_date=${encodeURIComponent(targetDate)}`);
      if (isCancelled) return;
      setPayload(dayPayload);
      setLoadedDate(targetDate);
      setSubmittedMinute(null);
      setDataState("ready");
    }

    async function loadInitialData() {
      try {
        setMapState("loading");
        setDataState("loading");
        setError(null);
        const apiBase = getApiBaseUrl();
        const [mapPayload, datePayload] = await Promise.all([
          fetchJson<MapGeometryResponse>(`${apiBase}/maps/nghia-do`),
          fetchJson<LayerDatesResponse>(`${apiBase}/datasets/silver/dates`),
        ]);
        if (isCancelled) return;

        setMapGeometry(mapPayload);
        setMapState("ready");
        setDates(datePayload.dates);

        const latestDate = datePayload.dates[datePayload.dates.length - 1] ?? "";
        setSelectedDate(latestDate);
        if (latestDate) {
          await loadDateData(latestDate);
        } else {
          setDataState("ready");
        }
      } catch (loadError) {
        if (!isCancelled) {
          setMapState("error");
          setDataState("error");
          setError(loadError instanceof Error ? loadError.message : "Không tải được dữ liệu bản đồ");
        }
      }
    }

    void loadInitialData();

    return () => {
      isCancelled = true;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    if (!document.querySelector(`link[href="${LEAFLET_CSS_URL}"]`)) {
      const link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = LEAFLET_CSS_URL;
      document.head.appendChild(link);
    }

    if (window.L) {
      setLeafletReady(true);
      return;
    }

    const existingScript = document.querySelector<HTMLScriptElement>(`script[src="${LEAFLET_JS_URL}"]`);
    const script = existingScript ?? document.createElement("script");
    script.src = LEAFLET_JS_URL;
    script.async = true;
    script.onload = () => setLeafletReady(true);
    script.onerror = () => setError("Không tải được Leaflet từ CDN để hiển thị nền OpenStreetMap");
    if (!existingScript) document.body.appendChild(script);
  }, []);

  useEffect(() => {
    if (visualizationMode !== "realtime") return;

    let isCancelled = false;

    async function loadRealtimeData() {
      try {
        setError(null);
        const apiBase = getApiBaseUrl();
        const realtimePayload = await fetchJson<RealtimeLayerDataResponse>(`${apiBase}/datasets/silver/realtime/data`);
        if (isCancelled) return;

        const latestMinute = latestMinuteFromRows(realtimePayload.rows);
        setPayload(realtimePayload);
        setLoadedDate(realtimePayload.target_date ?? "");
        setLastRealtimeUpdate(new Date());
        if (latestMinute !== null) {
          const roundedMinute = Math.round(latestMinute / 3) * 3;
          setSliderMinute(roundedMinute);
          setSubmittedMinute(roundedMinute);
        }
        setDataState("ready");
      } catch (loadError) {
        if (!isCancelled) {
          setDataState("error");
          setError(loadError instanceof Error ? loadError.message : "Không tải được dữ liệu trực tiếp");
        }
      }
    }

    setDataState("loading");
    void loadRealtimeData();
    const intervalId = window.setInterval(() => void loadRealtimeData(), realtimeIntervalSeconds * 1_000);

    return () => {
      isCancelled = true;
      window.clearInterval(intervalId);
    };
  }, [realtimeIntervalSeconds, visualizationMode]);

  async function loadForecastStatus() {
    const apiBase = getApiBaseUrl();
    setDataState("loading");
    setError(null);
    const statusPayload = await fetchJson<ForecastStatusResponse>(`${apiBase}/predictions/modelai-final/silver/forecast/status`);
    setForecastStatus(statusPayload);

    if (statusPayload.anchor_datetime) {
      const anchor = new Date(statusPayload.anchor_datetime);
      const defaultTarget = addMinutes(anchor, 60);
      setForecastDate(dateInputValue(defaultTarget));
      setForecastMinute(Math.round(minuteOfDay(defaultTarget) / 5) * 5);
    }

    if (statusPayload.latest_date && payload?.target_date !== statusPayload.latest_date) {
      const dayPayload = await fetchJson<LatestLayerDataResponse>(`${apiBase}/datasets/silver/by-date/data?target_date=${encodeURIComponent(statusPayload.latest_date)}`);
      setPayload(dayPayload);
      setLoadedDate(statusPayload.latest_date);
    }

    setDataState("ready");
  }

  function applyForecastHorizon(rawMinutes: string) {
    setForecastHorizonOption(rawMinutes as ForecastHorizonOption);
    if (rawMinutes === "custom" || !forecastStatus?.anchor_datetime) return;
    const target = addMinutes(new Date(forecastStatus.anchor_datetime), Number(rawMinutes));
    setForecastDate(dateInputValue(target));
    setForecastMinute(Math.round(minuteOfDay(target) / 5) * 5);
  }

  async function handleSubmitForecast() {
    if (!forecastStatus?.anchor_datetime || !forecastDate) return;
    try {
      setError(null);
      setDataState("loading");
      const target = dateAtMinute(forecastDate, forecastMinute);
      const stepMinutes = forecastStepMinutes(forecastHorizonOption);
      const apiBase = getApiBaseUrl();
      const forecastPayload = await fetchJson<ForecastLayerResponse>(
        `${apiBase}/predictions/modelai-final/silver/forecast?target_datetime=${encodeURIComponent(toApiDateTime(target))}&step_minutes=${stepMinutes}`
      );
      setForecastRows(forecastPayload.rows);
      setSubmittedForecast(forecastPayload);
      setSubmittedMinute(null);
      setDataState("ready");
    } catch (loadError) {
      setForecastRows([]);
      setSubmittedForecast(null);
      setDataState("error");
      setError(loadError instanceof Error ? loadError.message : "Khong tai duoc du lieu du doan");
    }
  }

  async function handleLoadSelectedDate() {
    if (!selectedDate) return;
    try {
      setError(null);
      setDataState("loading");
      const apiBase = getApiBaseUrl();
      const dayPayload = await fetchJson<LatestLayerDataResponse>(`${apiBase}/datasets/silver/by-date/data?target_date=${encodeURIComponent(selectedDate)}`);
      setPayload(dayPayload);
      setLoadedDate(selectedDate);
      setSubmittedMinute(null);
      setDataState("ready");
    } catch (loadError) {
      setPayload(null);
      setDataState("error");
      setError(loadError instanceof Error ? loadError.message : "Không tải được dữ liệu theo ngày");
    }
  }

  function handleModeChange(nextMode: VisualizationMode) {
    setVisualizationMode(nextMode);
    setError(null);
    if (nextMode === "historical") {
      void handleLoadSelectedDate();
    } else if (nextMode === "forecast") {
      void loadForecastStatus();
    }
  }

  const rangeRecords = useMemo(() => (payload?.rows ?? []).map(toDensityRecord), [payload?.rows]);
  const records = useMemo(() => (visualizationMode === "forecast" ? forecastRows.map(toDensityRecord) : rangeRecords), [forecastRows, rangeRecords, visualizationMode]);
  const availableMinutes = useMemo(() => {
    return Array.from(new Set(records.map((record) => record.minute).filter((minute): minute is number => minute !== null).map((minute) => Math.round(minute / 3) * 3))).sort((a, b) => a - b);
  }, [records]);
  const isLoadedDateToday = osNow !== null && loadedDate === dateInputValue(osNow);
  const historicalSliderMax = isLoadedDateToday
    ? Math.floor(minuteOfDay(osNow) / 3) * 3
    : 1440;

  useEffect(() => {
    if (visualizationMode === "historical" && availableMinutes.length) {
      setSliderMinute(availableMinutes[0]);
      setSubmittedMinute(availableMinutes[0]);
    }
  }, [availableMinutes, visualizationMode]);

  useEffect(() => {
    if (visualizationMode !== "historical") return;

    setSliderMinute((previous) => Math.min(previous, historicalSliderMax));
    setSubmittedMinute((previous) => previous === null ? null : Math.min(previous, historicalSliderMax));
  }, [historicalSliderMax, visualizationMode]);

  function handleSliderChange(rawValue: string) {
    const roundedMinute = Math.max(0, Math.min(historicalSliderMax, Math.round(Number(rawValue) / 3) * 3));
    setSliderMinute(roundedMinute);
    setSubmittedMinute(roundedMinute);
  }

  const densityByKey = useMemo(() => buildDensityByKey(records, submittedMinute), [records, submittedMinute]);
  const dailyDensityRangeByKey = useMemo(() => buildDailyDensityRangeByKey(rangeRecords), [rangeRecords]);
  const selectedMaxDensity = useMemo(() => {
    const values = Array.from(densityByKey.values()).map((item) => item.density);
    return values.length ? Math.max(...values) : 0;
  }, [densityByKey]);

  const geoJsonData = useMemo(() => {
    if (!mapGeometry) return null;
    return {
      type: "FeatureCollection",
      features: mapGeometry.features.map((feature) => {
        const density = findDensityForFeature(feature, densityByKey);
        const dailyRange = findRangeForFeature(feature, dailyDensityRangeByKey);
        const matched = density !== null;
        return {
          type: "Feature",
          properties: {
            name: feature.name,
            highway: feature.highway,
            density: density?.density ?? null,
            rows: density?.rows ?? 0,
            dayMinDensity: dailyRange?.min ?? null,
            dayMaxDensity: dailyRange?.max ?? null,
            color: colorForDensity(density?.density ?? null, dailyRange?.min ?? 0, dailyRange?.max ?? 1),
            weight: matched ? 6 : 2,
            opacity: matched ? 0.92 : 0.22,
            matched,
          },
          geometry: {
            type: "LineString",
            coordinates: feature.coordinates.map(([lat, lon]) => [lon, lat]),
          },
        };
      }),
    };
  }, [dailyDensityRangeByKey, densityByKey, mapGeometry]);

  const matchedFeatures = useMemo(() => {
    if (!geoJsonData) return 0;
    return geoJsonData.features.filter((feature) => feature.properties.matched).length;
  }, [geoJsonData]);

  useEffect(() => {
    if (!leafletReady || !window.L || !mapElementRef.current || !geoJsonData) return;

    if (mapInstanceRef.current) {
      mapInstanceRef.current.remove();
      mapInstanceRef.current = null;
    }

    const leaflet = window.L;
    const map = leaflet.map(mapElementRef.current);
    mapInstanceRef.current = map;

    leaflet
      .tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 20,
        attribution: "&copy; OpenStreetMap contributors",
      })
      .addTo(map);

    const layer = leaflet
      .geoJSON(geoJsonData, {
        style: (feature: { properties: Record<string, unknown> }) => {
          const properties = feature.properties;
          return {
            color: properties.color,
            weight: properties.weight,
            opacity: properties.opacity,
          };
        },
        onEachFeature: (feature: { properties: Record<string, unknown> }, featureLayer: LeafletLayer) => {
          const properties = feature.properties;
          const density = properties.density === null ? "N/A" : formatNumber(Number(properties.density), 3);
          const dayMinDensity = properties.dayMinDensity === null ? "N/A" : formatNumber(Number(properties.dayMinDensity), 3);
          const dayMaxDensity = properties.dayMaxDensity === null ? "N/A" : formatNumber(Number(properties.dayMaxDensity), 3);
          featureLayer.bindPopup?.(
            `<b>${escapeHtml(String(properties.name ?? "Không tên"))}</b><br>` +
              `Loại đường: ${escapeHtml(String(properties.highway ?? ""))}<br>` +
              `Mật độ trung bình: ${density}<br>` +
              `Mật độ bé nhất/cao nhất trong ngày (theo đường): ${dayMinDensity} / ${dayMaxDensity}<br>` +
              `rows: ${formatNumber(Number(properties.rows ?? 0))}`
          );
        },
      })
      .addTo(map);

    map.fitBounds(layer.getBounds(), { padding: [22, 22] });

    return () => {
      map.remove();
      if (mapInstanceRef.current === map) mapInstanceRef.current = null;
    };
  }, [geoJsonData, leafletReady]);

  const detailText =
    visualizationMode === "forecast"
      ? forecastStatus?.anchor_datetime
        ? `Dự đoán từ mốc dữ liệu mới nhất ${forecastStatus.anchor_datetime}.`
        : "Chọn mốc thời gian trong 3 ngày tới để dự đoán mật độ giao thông."
      : visualizationMode === "realtime"
        ? payload?.target_date
          ? `Dữ liệu trực tiếp ngày ${payload.target_date}, tự động cập nhật mỗi 15 giây từ toàn bộ parquet của ngày mới nhất.`
          : "Đang tải dữ liệu trực tiếp mới nhất."
        : payload?.target_date
          ? `Dữ liệu quá khứ của ngày ${payload.target_date}.`
          : "Chưa có dữ liệu silver để hiển thị trên bản đồ.";

  const forecastAnchorDate = forecastStatus?.anchor_datetime ? new Date(forecastStatus.anchor_datetime) : null;
  const forecastMaxDate = forecastStatus?.max_forecast_datetime ? new Date(forecastStatus.max_forecast_datetime) : null;
  const forecastTargetDate = forecastDate ? dateAtMinute(forecastDate, forecastMinute) : null;
  const isForecastTargetValid =
    Boolean(forecastTargetDate && forecastAnchorDate && forecastMaxDate && forecastTargetDate > forecastAnchorDate && forecastTargetDate <= forecastMaxDate);

  return (
    <main className="page-shell visualization-page">
      <section className="visualization-shell">
        <div className="visualization-header">
          <div>
            <div className="hero-kicker light-kicker">Trực quan hóa</div>
            <h1 className="visualization-title">Mật độ giao thông</h1>
            <p className="visualization-copy">{detailText}</p>
          </div>
        </div>

        <section className="visualization-controls light-card">
          <label className="date-field">
            <span>Loại dữ liệu</span>
            <select value={visualizationMode} onChange={(event) => handleModeChange(event.target.value as VisualizationMode)}>
              <option value="historical">Lưu lượng quá khứ</option>
              <option value="realtime">Lưu lượng trực tiếp</option>
              <option value="forecast">Dự đoán lưu lượng</option>
            </select>
          </label>

          {visualizationMode === "historical" ? (
            <>
          <label className="date-field">
            <span>Ngày dữ liệu</span>
            <select value={selectedDate} onChange={(event) => setSelectedDate(event.target.value)} disabled={dataState === "loading" || !dates.length}>
              {dates.map((date) => (
                <option key={date} value={date}>
                  {date}
                </option>
              ))}
            </select>
          </label>
          <button type="button" className="submit-button" onClick={() => void handleLoadSelectedDate()} disabled={!selectedDate || dataState === "loading"}>
            {dataState === "loading" ? "Đang tải..." : "Load ngày"}
          </button>
            </>
          ) : null}

          {visualizationMode === "historical" && dataState === "ready" && loadedDate ? (
            <label className="time-slider-field">
              <div className="time-slider-body">
                <span>Thời điểm trong ngày</span>
                <input type="range" min={0} max={historicalSliderMax} step={3} value={sliderMinute} onChange={(event) => handleSliderChange(event.target.value)} />
                <strong>{minuteToTimeInput(sliderMinute)}</strong>
                <div className="time-slider-scale">
                  <span>0h</span>
                  {isLoadedDateToday ? (
                    <span>{minuteToTimeInput(historicalSliderMax)} (hiện tại)</span>
                  ) : (
                    <>
                      <span>6h</span>
                      <span>12h</span>
                      <span>18h</span>
                      <span>24h</span>
                    </>
                  )}
                </div>
              </div>
            </label>
          ) : null}

          {visualizationMode === "realtime" ? (
            <div className="visualization-live-status">
              <strong>{dataState === "loading" ? "Đang tải dữ liệu trực tiếp..." : `Mốc mới nhất của dữ liệu: ${minuteToTimeInput(sliderMinute)}`}</strong>
              <span>{lastRealtimeUpdate ? `Cập nhật: ${lastRealtimeUpdate.toLocaleTimeString("vi-VN")}` : "Chưa có lần cập nhật nào"}</span>
              <label className="realtime-interval-field">
                <span>Chu kỳ cập nhật</span>
                <select value={realtimeIntervalOption} onChange={(event) => setRealtimeIntervalOption(event.target.value as RealtimeIntervalOption)}>
                  <option value="3">3 giây</option>
                  <option value="5">5 giây</option>
                  <option value="10">10 giây</option>
                  <option value="15">15 giây</option>
                  <option value="custom">Tùy chọn</option>
                </select>
                {realtimeIntervalOption === "custom" ? (
                  <input
                    type="number"
                    min={3}
                    step={1}
                    value={customRealtimeIntervalSeconds}
                    onChange={(event) => setCustomRealtimeIntervalSeconds(Math.max(3, Number(event.target.value) || 3))}
                  />
                ) : null}
                <small>Đang cập nhật mỗi {realtimeIntervalSeconds} giây. Tối thiểu 3 giây.</small>
              </label>
            </div>
          ) : null}

          {visualizationMode === "forecast" ? (
            <div className="visualization-live-status forecast-controls">
              <strong>
                {forecastStatus?.anchor_datetime
                  ? `Dữ liệu mới nhất: ${forecastStatus.latest_date ?? "--"} (${forecastStatus.anchor_datetime})`
                  : dataState === "loading"
                    ? "Đang đọc mốc dữ liệu mới nhất..."
                    : "Chưa có mốc dữ liệu mới nhất"}
              </strong>
              {forecastStatus?.max_forecast_datetime ? <span>Giới hạn dự đoán: {forecastStatus.max_forecast_datetime}</span> : null}
              <label className="forecast-field">
                <span>Mốc nhanh</span>
                <select value={forecastHorizonOption} onChange={(event) => applyForecastHorizon(event.target.value)}>
                  <option value="30">30 phút</option>
                  <option value="60">1 giờ</option>
                  <option value="180">3 giờ</option>
                  <option value="360">6 giờ</option>
                  <option value="720">12 giờ</option>
                  <option value="1440">1 ngày</option>
                  <option value="custom">Tùy chọn</option>
                </select>
              </label>
              {forecastHorizonOption === "custom" ? (
                <>
                  <label className="date-field forecast-field">
                    <span>Ngày dự đoán</span>
                    <input
                      type="date"
                      value={forecastDate}
                      min={forecastAnchorDate ? dateInputValue(forecastAnchorDate) : undefined}
                      max={forecastMaxDate ? dateInputValue(forecastMaxDate) : undefined}
                      onChange={(event) => setForecastDate(event.target.value)}
                      disabled={!forecastStatus?.anchor_datetime || dataState === "loading"}
                    />
                  </label>
                  <label className="time-slider-field">
                    <div className="time-slider-body">
                      <span>Thời điểm trong ngày dự đoán</span>
                      <input
                        type="range"
                        min={0}
                        max={1435}
                        step={5}
                        value={forecastMinute}
                        onChange={(event) => setForecastMinute(Math.max(0, Math.min(1435, Math.round(Number(event.target.value) / 5) * 5)))}
                        disabled={!forecastStatus?.anchor_datetime || dataState === "loading"}
                      />
                      <strong>{minuteToTimeInput(forecastMinute)}</strong>
                      <div className="time-slider-scale">
                        <span>0h</span>
                        <span>6h</span>
                        <span>12h</span>
                        <span>18h</span>
                        <span>24h</span>
                      </div>
                    </div>
                  </label>
                </>
              ) : null}
              <button type="button" className="submit-button" onClick={() => void handleSubmitForecast()} disabled={!isForecastTargetValid || dataState === "loading"}>
                {dataState === "loading" ? "Đang dự đoán..." : "Submit dự đoán"}
              </button>
              {submittedForecast ? (
                <small>
                  Đã vẽ dự đoán {submittedForecast.forecast_datetime}, horizon {formatNumber(submittedForecast.horizon_minutes)} phút.
                </small>
              ) : null}
            </div>
          ) : null}
        </section>

        {error ? <div className="error-box visualization-error">{error}</div> : null}

        <div className="map-card traffic-map-card">
          <div className="traffic-map-toolbar">
            <span>{mapState === "ready" && mapGeometry ? `${formatNumber(mapGeometry.features.length)} tuyến OSM/SUMO` : "Đang tải OSM"}</span>
            <span>{formatNumber(matchedFeatures)} tuyến có avg_density</span>
            <span>Max density tại mốc này: {formatNumber(selectedMaxDensity, 2)}</span>
          </div>
          <div ref={mapElementRef} className="leaflet-traffic-map" />
          <div className="traffic-map-legend">
            <span><i className="density-low" /> Thấp</span>
            <span><i className="density-mid" /> Trung bình</span>
            <span><i className="density-high" /> Cao</span>
            <span><i className="density-none" /> Chưa match</span>
          </div>
        </div>
      </section>
    </main>
  );
}
