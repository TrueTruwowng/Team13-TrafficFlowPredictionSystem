"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { fetchJson, getApiBaseUrl } from "../../lib/api";
import type { LatestLayerDataResponse, LayerDatesResponse, MapGeometryResponse } from "../../lib/types";

type LoadState = "loading" | "ready" | "error";

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

  const records = useMemo(() => (payload?.rows ?? []).map(toDensityRecord), [payload?.rows]);
  const availableMinutes = useMemo(() => {
    return Array.from(new Set(records.map((record) => record.minute).filter((minute): minute is number => minute !== null).map((minute) => Math.round(minute / 3) * 3))).sort((a, b) => a - b);
  }, [records]);

  useEffect(() => {
    if (availableMinutes.length) {
      setSliderMinute(availableMinutes[0]);
      setSubmittedMinute(availableMinutes[0]);
    }
  }, [availableMinutes]);

  function handleSliderChange(rawValue: string) {
    const roundedMinute = Math.max(0, Math.min(1440, Math.round(Number(rawValue) / 3) * 3));
    setSliderMinute(roundedMinute);
    setSubmittedMinute(roundedMinute);
  }

  const densityByKey = useMemo(() => buildDensityByKey(records, submittedMinute), [records, submittedMinute]);
  const dailyDensityRangeByKey = useMemo(() => buildDailyDensityRangeByKey(records), [records]);
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
              `highway: ${escapeHtml(String(properties.highway ?? ""))}<br>` +
              `avg_density: ${density}<br>` +
              `day min/max: ${dayMinDensity} / ${dayMaxDensity}<br>` +
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

  const detailText = payload?.target_date
    ? `Dữ liệu mới nhất của ngày ${payload.target_date}`
    : "Chưa có dữ liệu silver để hiển thị trên bản đồ.";

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

          {dataState === "ready" && loadedDate ? (
            <label className="time-slider-field">
              <div className="time-slider-body">
                <span>Thời điểm trong ngày</span>
                <input type="range" min={0} max={1440} step={3} value={sliderMinute} onChange={(event) => handleSliderChange(event.target.value)} />
                <strong>{minuteToTimeInput(sliderMinute)}</strong>
                <div className="time-slider-scale">
                  <span>0h</span>
                  <span>6h</span>
                  <span>12h</span>
                  <span>18h</span>
                  <span>24h</span>
                </div>
              </div>
            </label>
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
