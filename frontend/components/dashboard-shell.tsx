"use client";

import { useEffect, useMemo, useState, useRef } from "react";

import { fetchJson, getApiBaseUrl } from "../lib/api";
import type { DatasetListResponse, DatasetObject, DayDataResponse, DayRoadCountResponse } from "../lib/types";

type DashboardShellProps = {
  apiBaseUrl: string;
};

type CloudStatus = "loading" | "connected" | "disconnected";

type FilterSummary = {
  records: number;
  roads: number;
  hours: number;
};

type SortDirection = "asc" | "desc";

function extractDateFromName(name: string): string | null {
  const match = /date=(\d{4}-\d{2}-\d{2})/.exec(name);
  return match?.[1] ?? null;
}

function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }

  return new Intl.NumberFormat("vi-VN", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function parseDateTime(value: unknown): Date | null {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

// Return 3-minute bucket index (0..479) for a row, or null when unavailable
function getHour(row: Record<string, unknown>): number | null {
  const fromHourColumn = row.hour;
  if (typeof fromHourColumn === "number" && Number.isFinite(fromHourColumn)) {
    // If upstream provided an hour number, convert to nearest 3-minute bucket start
    const hour = Math.max(0, Math.min(23, Math.floor(fromHourColumn)));
    return Math.floor((hour * 60) / 3);
  }

  const dt = parseDateTime(row.recordDatetime);
  if (!dt) return null;
  const minutesSinceMidnight = dt.getHours() * 60 + dt.getMinutes();
  return Math.floor(minutesSinceMidnight / 3);
}

function buildRoadNames(rows: Record<string, unknown>[]): string[] {
  return Array.from(new Set(rows.map((row) => String(row.road_name ?? "Unknown road")).filter(Boolean))).sort((left, right) => left.localeCompare(right));
}

function buildHourList(rows: Record<string, unknown>[]): number[] {
  return Array.from(
    new Set(
      rows
        .map((row) => getHour(row))
        .filter((value): value is number => value !== null)
    )
  ).sort((left, right) => left - right);
}

function buildMetrics(rows: Record<string, unknown>[]): FilterSummary {
  return {
    records: rows.length,
    roads: buildRoadNames(rows).length,
    hours: buildHourList(rows).length,
  };
}

function getActualFlowValue(row: Record<string, unknown>): number | null {
  const candidates: unknown[] = [row.actualFlow, row.actual_flow, row.ActualFlow, row.flow, row.avg_flow];
  for (const candidate of candidates) {
    const parsed = toNumber(candidate);
    if (parsed !== null) {
      return parsed;
    }
  }
  return null;
}

function getRowIdentifier(row: Record<string, unknown>, index: number): string {
  const candidates: unknown[] = [row.id, row.row_id, row.record_id, row.uuid, row._id];
  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined) {
      continue;
    }
    const text = String(candidate).trim();
    if (text) {
      return text;
    }
  }

  return String(index + 1);
}

function compareNullableNumbers(left: number | null, right: number | null): number {
  if (left === null && right === null) {
    return 0;
  }
  if (left === null) {
    return 1;
  }
  if (right === null) {
    return -1;
  }
  return left - right;
}

function compareUnknownValues(left: unknown, right: unknown): number {
  const leftNumber = toNumber(left);
  const rightNumber = toNumber(right);
  if (leftNumber !== null && rightNumber !== null) {
    return leftNumber - rightNumber;
  }

  const leftDate = parseDateTime(left);
  const rightDate = parseDateTime(right);
  if (leftDate && rightDate) {
    return leftDate.getTime() - rightDate.getTime();
  }

  const leftText = left === null || left === undefined ? "" : String(left);
  const rightText = right === null || right === undefined ? "" : String(right);
  return leftText.localeCompare(rightText, "vi", { numeric: true, sensitivity: "base" });
}

export default function DashboardShell({ apiBaseUrl }: DashboardShellProps) {
  const RAW_PAGE_SIZE = 200;
  // Use the runtime-resolved API base (prefer env or hostname resolution).
  // Ignore the injected `apiBaseUrl` prop to avoid server-side '/api' values.
  const apiBase = getApiBaseUrl();
  const [cloudStatus, setCloudStatus] = useState<CloudStatus>("loading");
  const [cloudMessage, setCloudMessage] = useState("Đang kiểm tra kết nối Cloud...");
  const [datasetList, setDatasetList] = useState<DatasetListResponse | null>(null);
  const [selectedDate, setSelectedDate] = useState("");
  const [selectedRoad, setSelectedRoad] = useState("");
  const [roadOptions, setRoadOptions] = useState<string[]>([]);
  const [loadingRoads, setLoadingRoads] = useState(false);
  const [rawSortKey, setRawSortKey] = useState<string>("");
  const [rawSortDirection, setRawSortDirection] = useState<SortDirection>("asc");
  const [submittedDate, setSubmittedDate] = useState("");
  const [submittedRoad, setSubmittedRoad] = useState("");
  const [dataPayload, setDataPayload] = useState<DayDataResponse | null>(null);
  const [loadingData, setLoadingData] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hoveredHour, setHoveredHour] = useState<number | null>(null);
  const [rawPage, setRawPage] = useState(1);
  const [rawPageInput, setRawPageInput] = useState("1");

  const availableDates = useMemo(() => {
    const items: DatasetObject[] = datasetList?.items ?? [];
    return Array.from(
      new Set(
        items
          .map((item: DatasetObject) => extractDateFromName(item.name))
          .filter((value: string | null): value is string => Boolean(value))
      )
    ).sort();
  }, [datasetList]);

  useEffect(() => {
    async function loadCloudStatus() {
      try {
        setCloudStatus("loading");
        setCloudMessage("Đang kiểm tra kết nối Cloud...");
        const payload = await fetchJson<DatasetListResponse>(`${apiBase}/datasets`);
        setDatasetList(payload);
        setCloudStatus("connected");
        setCloudMessage(`Đã kết nối Cloud, bucket: ${payload.bucket}`);
      } catch (fetchError) {
        setDatasetList(null);
        setCloudStatus("disconnected");
        setCloudMessage(fetchError instanceof Error ? fetchError.message : "Không kết nối được tới Cloud");
      }
    }

    void loadCloudStatus();
  }, [apiBase]);

  useEffect(() => {
    if (!selectedDate && availableDates.length) {
      setSelectedDate(availableDates[availableDates.length - 1]);
    }
  }, [availableDates, selectedDate]);

  useEffect(() => {
    if (!selectedDate) {
      setRoadOptions([]);
      setSelectedRoad("");
      return;
    }

    let isCancelled = false;

    async function loadRoadOptionsForDate() {
      try {
        setLoadingRoads(true);
        const payload = await fetchJson<DayRoadCountResponse>(
          `${apiBase}/datasets/by-date/roads?target_date=${encodeURIComponent(selectedDate)}`
        );
        if (isCancelled) {
          return;
        }

        setRoadOptions(payload.roads);
        setSelectedRoad((previous: string) => (previous && payload.roads.includes(previous) ? previous : ""));
      } catch {
        if (!isCancelled) {
          setRoadOptions([]);
          setSelectedRoad("");
        }
      } finally {
        if (!isCancelled) {
          setLoadingRoads(false);
        }
      }
    }

    void loadRoadOptionsForDate();

    return () => {
      isCancelled = true;
    };
  }, [apiBaseUrl, selectedDate]);

  async function handleSubmit() {
    if (!selectedDate) {
      setError("Hãy chọn một ngày trước khi gửi request.");
      return;
    }

    try {
      setLoadingData(true);
      setError(null);
      setSubmittedDate(selectedDate);
      const params = new URLSearchParams({ target_date: selectedDate });
      const normalizedRoad = selectedRoad.trim();
      if (normalizedRoad) {
        params.set("road_name", normalizedRoad);
      }

      const payload = await fetchJson<DayDataResponse>(`${apiBase}/datasets/by-date/data?${params.toString()}`);
      setDataPayload(payload);
      setSubmittedRoad(normalizedRoad);
    } catch (fetchError) {
      setDataPayload(null);
      setSubmittedRoad("");
      setError(fetchError instanceof Error ? fetchError.message : "Không tải được dữ liệu theo ngày");
    } finally {
      setLoadingData(false);
    }
  }

  const rows = dataPayload?.rows ?? [];
  const metrics = buildMetrics(rows);
  const roadNames = buildRoadNames(rows);
  const hourList = buildHourList(rows);
  const rowHeaders = dataPayload?.columns.length ? dataPayload.columns : Object.keys(rows[0] ?? {});

  // Field extractors for speed, temperature, precipitation, weather
  function getSpeedValue(row: Record<string, unknown>): number | null {
    const candidates: unknown[] = [row.speed, row.avg_speed, row.avgSpeed, row.speed_kmh, row.velocity, row.avg_velocity];
    for (const candidate of candidates) {
      const parsed = toNumber(candidate);
      if (parsed !== null) return parsed;
    }
    return null;
  }

  function getTemperatureValue(row: Record<string, unknown>): number | null {
    const candidates: unknown[] = [row.temperature, row.temp, row.air_temp, row.airTemperature];
    for (const candidate of candidates) {
      const parsed = toNumber(candidate);
      if (parsed !== null) return parsed;
    }
    return null;
  }

  function getPrecipitationValue(row: Record<string, unknown>): number | null {
    const candidates: unknown[] = [row.precipitation, row.precip, row.rain_mm, row.precip_mm, row.rain];
    for (const candidate of candidates) {
      const parsed = toNumber(candidate);
      if (parsed !== null) return parsed;
    }
    return null;
  }

  function getWeatherLabel(row: Record<string, unknown>): string | null {
    const candidates: unknown[] = [row.weather, row.condition, row.weather_label, row.weatherLabel];
    for (const candidate of candidates) {
      if (candidate === null || candidate === undefined) continue;
      const text = String(candidate).trim();
      if (text) return text;
    }
    return null;
  }

  

  const sortedRawRows = useMemo<Record<string, unknown>[]>(() => {
    if (!rawSortKey) {
      return rows;
    }

    const sorted = [...rows];
    sorted.sort((left, right) => {
      const compareResult = compareUnknownValues(left[rawSortKey], right[rawSortKey]);
      return rawSortDirection === "asc" ? compareResult : -compareResult;
    });
    return sorted;
  }, [rawSortDirection, rawSortKey, rows]);

  const totalRawPages = useMemo(() => {
    return Math.max(1, Math.ceil(sortedRawRows.length / RAW_PAGE_SIZE));
  }, [sortedRawRows.length]);

  useEffect(() => {
    setRawPage((previous: number) => Math.min(previous, totalRawPages));
  }, [totalRawPages]);

  const rawPageRows = useMemo(() => {
    const startIndex = (rawPage - 1) * RAW_PAGE_SIZE;
    return sortedRawRows.slice(startIndex, startIndex + RAW_PAGE_SIZE);
  }, [rawPage, sortedRawRows]);

  useEffect(() => {
    setRawPage(1);
    setRawPageInput("1");
  }, [rawSortDirection, rawSortKey, rows]);

  useEffect(() => {
    setRawPageInput(String(rawPage));
  }, [rawPage]);

  function goToRawPage(pageValue: number) {
    const safePage = Math.max(1, Math.min(totalRawPages, pageValue));
    setRawPage(safePage);
    setRawPageInput(String(safePage));
  }

  const isTodaySelected = useMemo(() => {
    const target = dataPayload?.target_date;
    if (!target) return false;
    const now = new Date();
    const pad = (n: number) => String(n).padStart(2, "0");
    const today = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
    return target === today;
  }, [dataPayload?.target_date]);

  const currentBucketIndex = useMemo(() => {
    if (!isTodaySelected) return null;
    const now = new Date();
    const minutesSinceMidnight = now.getHours() * 60 + now.getMinutes();
    return Math.floor(minutesSinceMidnight / 3);
  }, [isTodaySelected]);

  const rowsForAggregation = useMemo(() => {
    if (!isTodaySelected || currentBucketIndex === null) return rows;
    return rows.filter((row) => {
      const bucket = getHour(row);
      return bucket !== null && bucket <= currentBucketIndex;
    });
  }, [rows, isTodaySelected, currentBucketIndex]);

  const hourlyFlowSeries = useMemo<number[]>(() => {
    const BUCKETS_PER_DAY = 24 * 60 / 3; // 480
    const buckets = Array.from({ length: BUCKETS_PER_DAY }, () => 0);
    for (const row of rowsForAggregation) {
      const bucket = getHour(row);
      const actualFlow = getActualFlowValue(row);
      if (bucket === null || bucket < 0 || bucket >= buckets.length || actualFlow === null) {
        continue;
      }
      buckets[bucket] += actualFlow;
    }
    return buckets;
  }, [rowsForAggregation]);

  const maxHourlyFlow = useMemo<number>(() => {
    return hourlyFlowSeries.reduce((maxValue: number, value: number) => (value > maxValue ? value : maxValue), 0);
  }, [hourlyFlowSeries]);

  // Daily aggregate metrics for the sidebar
  const dailyAggregates = useMemo(() => {
    const buckets = hourlyFlowSeries;
    const dailyMaxFlow = buckets.length ? Math.max(...buckets) : 0;
    const dailyMinFlow = buckets.length ? Math.min(...buckets) : 0;

    // Average speed: prefer flow-weighted if flow exists, otherwise simple mean
    let sumSpeed = 0;
    let countSpeed = 0;
    let weightedSpeedNumer = 0;
    let weightedSpeedDenom = 0;
    for (const row of rowsForAggregation) {
      const speed = getSpeedValue(row);
      const flow = getActualFlowValue(row) ?? 0;
      if (speed !== null) {
        sumSpeed += speed;
        countSpeed += 1;
        if (flow > 0) {
          weightedSpeedNumer += speed * flow;
          weightedSpeedDenom += flow;
        }
      }
    }

    const avgSpeed = weightedSpeedDenom > 0 ? weightedSpeedNumer / weightedSpeedDenom : countSpeed > 0 ? sumSpeed / countSpeed : null;

    // Dominant weather (mode)
    const weatherCounts = new Map<string, number>();
    for (const row of rowsForAggregation) {
      const w = getWeatherLabel(row);
      if (!w) continue;
      const lower = w.toLowerCase();
      weatherCounts.set(lower, (weatherCounts.get(lower) ?? 0) + 1);
    }
    let dominantWeather: string | null = null;
    let dominantCount = 0;
    for (const [w, c] of weatherCounts.entries()) {
      if (c > dominantCount) {
        dominantCount = c;
        dominantWeather = w;
      }
    }

    // Conditional avg temperature and precipitation only when dominant weather indicates rain
    let avgTemperature: number | null = null;
    let avgPrecipitation: number | null = null;
    if (dominantWeather && dominantWeather.includes("rain")) {
      let sumTemp = 0;
      let countTemp = 0;
      let sumPre = 0;
      let countPre = 0;
      for (const row of rowsForAggregation) {
        const w = getWeatherLabel(row);
        if (!w) continue;
        if (!w.toLowerCase().includes("rain")) continue;
        const t = getTemperatureValue(row);
        const p = getPrecipitationValue(row);
        if (t !== null) {
          sumTemp += t;
          countTemp += 1;
        }
        if (p !== null) {
          sumPre += p;
          countPre += 1;
        }
      }
      avgTemperature = countTemp > 0 ? sumTemp / countTemp : null;
      avgPrecipitation = countPre > 0 ? sumPre / countPre : null;
    }

    return {
      dailyMaxFlow,
      dailyMinFlow,
      avgSpeed,
      dominantWeather,
      avgTemperature,
      avgPrecipitation,
    };
  }, [hourlyFlowSeries, rowsForAggregation]);

  const chartGeometry = useMemo(() => {
    const width = 960;
    const height = 320;
    const marginLeft = 70;
    const marginRight = 24;
    const marginTop = 20;
    const marginBottom = 44;
    return {
      width,
      height,
      marginLeft,
      marginRight,
      marginTop,
      marginBottom,
      plotWidth: width - marginLeft - marginRight,
      plotHeight: height - marginTop - marginBottom,
    };
  }, []);

  const hourlyFlowPlot = useMemo(() => {
    const BUCKETS_PER_DAY = hourlyFlowSeries.length;
    const safeMax = maxHourlyFlow > 0 ? maxHourlyFlow : 1;
    return hourlyFlowSeries.map((value: number, bucket: number) => {
      const x = chartGeometry.marginLeft + (bucket / (BUCKETS_PER_DAY - 1)) * chartGeometry.plotWidth;
      const y = chartGeometry.marginTop + chartGeometry.plotHeight - (value / safeMax) * chartGeometry.plotHeight;
      return { bucket, value, x, y };
    });
  }, [chartGeometry.marginLeft, chartGeometry.marginTop, chartGeometry.plotHeight, chartGeometry.plotWidth, hourlyFlowSeries, maxHourlyFlow]);

  // avg speed per bucket (flow-weighted when possible, otherwise simple mean per bucket)
  const hourlyAvgSpeedSeries = useMemo<number[]>(() => {
    const BUCKETS_PER_DAY = 24 * 60 / 3;
    const sumNumer: number[] = Array.from({ length: BUCKETS_PER_DAY }, () => 0);
    const sumDenom: number[] = Array.from({ length: BUCKETS_PER_DAY }, () => 0);

    for (const row of rowsForAggregation) {
      const bucket = getHour(row);
      if (bucket === null || bucket < 0 || bucket >= BUCKETS_PER_DAY) continue;
      const speed = getSpeedValue(row);
      if (speed === null) continue;
      const flow = getActualFlowValue(row);
      const weight = flow !== null && flow > 0 ? flow : 1;
      sumNumer[bucket] += speed * weight;
      sumDenom[bucket] += weight;
    }

    return sumNumer.map((n, i) => (sumDenom[i] > 0 ? n / sumDenom[i] : 0));
  }, [rowsForAggregation]);

  const maxAvgSpeed = useMemo(() => {
    return hourlyAvgSpeedSeries.reduce((m, v) => (v > m ? v : m), 0);
  }, [hourlyAvgSpeedSeries]);

  const hourlySpeedPlot = useMemo(() => {
    const BUCKETS_PER_DAY = hourlyAvgSpeedSeries.length;
    const safeMaxSpeed = maxAvgSpeed > 0 ? maxAvgSpeed : 1;
    return hourlyAvgSpeedSeries.map((value: number, bucket: number) => {
      const x = chartGeometry.marginLeft + (bucket / (BUCKETS_PER_DAY - 1)) * chartGeometry.plotWidth;
      const ySpeed = chartGeometry.marginTop + chartGeometry.plotHeight - (value / safeMaxSpeed) * chartGeometry.plotHeight;
      return { bucket, value, x, ySpeed };
    });
  }, [chartGeometry.marginLeft, chartGeometry.marginTop, chartGeometry.plotHeight, chartGeometry.plotWidth, hourlyAvgSpeedSeries, maxAvgSpeed]);

  const hourlyFlowPoints = useMemo<string>(() => {
    return hourlyFlowPlot.map((point: { bucket: number; value: number; x: number; y: number }) => `${point.x},${point.y}`).join(" ");
  }, [hourlyFlowPlot]);

  const yAxisTicks = useMemo<number[]>(() => {
    const safeMax = maxHourlyFlow > 0 ? maxHourlyFlow : 1;
    return [1, 0.75, 0.5, 0.25, 0].map((ratio) => safeMax * ratio);
  }, [maxHourlyFlow]);

  function formatBucketTime(bucketIndex: number) {
    const minutes = bucketIndex * 3;
    const hh = Math.floor(minutes / 60);
    const mm = minutes % 60;
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${pad(hh)}:${pad(mm)}`;
  }

  const hoveredPoint = useMemo(() => {
    if (hoveredHour === null) {
      return null;
    }
    return hourlyFlowPlot.find((point: { bucket: number; value: number; x: number; y: number }) => point.bucket === hoveredHour) ?? null;
  }, [hourlyFlowPlot, hoveredHour]);

  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoveredMouse, setHoveredMouse] = useState<{ x: number; y: number } | null>(null);

  const hasFlowData = maxHourlyFlow > 0;

  function toggleRoadSort(_column: string) {
    // road summary removed
  }

  function toggleRawSort(column: string) {
    if (rawSortKey === column) {
      setRawSortDirection((previous: SortDirection) => (previous === "asc" ? "desc" : "asc"));
      return;
    }

    setRawSortKey(column);
    setRawSortDirection("asc");
  }

  function sortLabel(isActive: boolean, direction: SortDirection): string {
    if (!isActive) {
      return "↕";
    }
    return direction === "asc" ? "↑" : "↓";
  }

  return (
    <main className="page-shell light-theme">
      <div className="dashboard-frame">
        <aside className="left-sidebar light-card">
          <div className="section-label">Chi tiết ngày</div>
          <h2 className="section-title light-section-title">Thông tin chi tiết ngày</h2>
          {dataPayload ? (
            <div className="summary-list">
              <div className="summary-row">
                <span>Ngày dữ liệu</span>
                <strong>{dataPayload.target_date}</strong>
              </div>
              <div className="summary-row">
                <span>Max lưu lượng (xe/3ph)</span>
                <strong>{formatNumber(dailyAggregates.dailyMaxFlow)}</strong>
              </div>
              <div className="summary-row">
                <span>Min lưu lượng (xe/3ph)</span>
                <strong>{formatNumber(dailyAggregates.dailyMinFlow)}</strong>
              </div>
              <div className="summary-row">
                <span>Tốc độ TB trong ngày</span>
                <strong>{dailyAggregates.avgSpeed !== null ? `${formatNumber(dailyAggregates.avgSpeed, 1)} km/h` : "N/A"}</strong>
              </div>
              <div className="summary-row">
                <span>Thời tiết chiếm ưu thế</span>
                <strong>{dailyAggregates.dominantWeather ? dailyAggregates.dominantWeather : "N/A"}</strong>
              </div>
              {dailyAggregates.dominantWeather && dailyAggregates.dominantWeather.includes("rain") ? (
                <>
                  <div className="summary-row">
                    <span>Nhiệt độ TB (khi mưa)</span>
                    <strong>{dailyAggregates.avgTemperature !== null ? `${formatNumber(dailyAggregates.avgTemperature, 1)} °C` : "N/A"}</strong>
                  </div>
                  <div className="summary-row">
                    <span>Lượng mưa TB (khi mưa)</span>
                    <strong>{dailyAggregates.avgPrecipitation !== null ? `${formatNumber(dailyAggregates.avgPrecipitation, 1)} mm` : "N/A"}</strong>
                  </div>
                </>
              ) : null}
            </div>
          ) : (
            <div className="empty-state light-empty">Chọn ngày và bấm Submit để xem chi tiết ngày.</div>
          )}
        </aside>

        <div className="container light-layout">
        <section className="hero light-hero">
          <div className="hero-card light-card">
            <div className="hero-kicker light-kicker">TrafficFlow Dashboard</div>
            <h1 className="hero-title light-title">Traffic flow platform</h1>

            <div className="hero-stats light-stats">
              <div className="metric-card light-metric">
                <div className="metric-label">Bản ghi</div>
                <div className="metric-value">{formatNumber(metrics.records)}</div>
              </div>
              <div className="metric-card light-metric">
                <div className="metric-label">Đường</div>
                <div className="metric-value">{formatNumber(metrics.roads)}</div>
              </div>
              <div className="metric-card light-metric">
                <div className="metric-label">Giờ</div>
                <div className="metric-value">{formatNumber(metrics.hours)}</div>
              </div>
              <div className="metric-card light-metric">
                <div className="metric-label">Ngày đã gửi</div>
                <div className="metric-value">{submittedDate || "--"}</div>
              </div>
            </div>
          </div>

          <aside className="status-card light-status">
            <div className={`status-badge ${cloudStatus === "connected" ? "success" : cloudStatus === "disconnected" ? "error" : ""}`}>
              {cloudStatus === "connected" ? "Cloud connected" : cloudStatus === "disconnected" ? "Cloud disconnected" : "Checking Cloud"}
            </div>
            <div className="status-grid">
              <div className="status-item light-status-item">
                <div className="status-label">Kết nối Cloud</div>
                <div className="status-value">{cloudMessage}</div>
              </div>
              <div className="status-item light-status-item">
                <div className="status-label">Backend API</div>
                <div className="status-value">{apiBase}</div>
              </div>
              <div className="status-item light-status-item">
                <div className="status-label">Ngày khả dụng</div>
                <div className="status-value">{formatNumber(availableDates.length)}</div>
              </div>
            </div>
            <div className="actions-row light-actions">
              <span className="pill light-pill">{cloudStatus === "connected" ? `${availableDates.length} dates found` : "Waiting for Cloud"}</span>
              <span className="pill light-pill-soft">{submittedRoad ? `Road: ${submittedRoad}` : "Road: tất cả"}</span>
              <span className="pill muted light-pill-muted">{loadingData ? "Loading data" : `${metrics.records} rows loaded`}</span>
            </div>
            {error ? <div className="error-box light-error">{error}</div> : null}
          </aside>
        </section>

        <section className="submit-card light-card">
          <div className="submit-head">
            <div>
              <div className="section-label">Chọn ngày</div>
              <h2 className="section-title light-section-title">Lấy dữ liệu của một ngày cụ thể</h2>
            </div>
            <div className="submit-actions">
              <button
                type="button"
                className="ghost-button light-button"
                onClick={() => {
                  if (availableDates.length) {
                    setSelectedDate(availableDates[availableDates.length - 1]);
                  }
                }}
              >
                Chọn ngày mới nhất
              </button>
              <button type="button" className="submit-button" onClick={() => void handleSubmit()} disabled={loadingData}>
                {loadingData ? "Đang tải..." : "Submit"}
              </button>
            </div>
          </div>

          <div className="date-row">
            <label className="date-field">
              <span>Ngày</span>
              <input
                type="date"
                value={selectedDate}
                onChange={(event) => setSelectedDate(event.target.value)}
                max={availableDates[availableDates.length - 1] ?? undefined}
                min={availableDates[0] ?? undefined}
              />
            </label>

            <label className="date-field">
              <span>Đường (tuỳ chọn)</span>
              <select
                value={selectedRoad}
                onChange={(event) => setSelectedRoad(event.target.value)}
                disabled={!selectedDate || loadingRoads}
              >
                <option value="">Tất cả đường</option>
                {roadOptions.map((roadName) => (
                  <option key={roadName} value={roadName}>
                    {roadName}
                  </option>
                ))}
              </select>
            </label>

            <div className="date-hint">
              {availableDates.length ? `Có ${availableDates.length} ngày trong Cloud` : "Chưa đọc được danh sách ngày từ Cloud"}
              <br />
              {selectedDate
                ? loadingRoads
                  ? "Đang tải danh sách đường theo ngày đã chọn"
                  : roadOptions.length
                    ? `Có ${roadOptions.length} đường khả dụng cho ngày này`
                    : "Không có đường khả dụng cho ngày này"
                : "Chọn ngày để tải danh sách đường"}
            </div>
          </div>
        </section>

        

        <section className="table-card light-card">
          <h2 className="section-title light-section-title">lưu lượng trong ngày</h2>
          {hasFlowData ? (
            <div className="flow-chart-wrap">
              <div className="flow-chart-meta">
                <span>{isTodaySelected ? "Từ 00:00 đến hiện tại" : "Từ 00:00 đến 24:00"}</span>
                <strong>Max khung 3-phút: {formatNumber(maxHourlyFlow)}</strong>
              </div>
              <div className="flow-chart-grid">
                <svg
                  ref={svgRef}
                  viewBox={`0 0 ${chartGeometry.width} ${chartGeometry.height}`}
                  role="img"
                  aria-label="Biểu đồ lưu lượng ActualFlow theo khung 3 phút"
                  onMouseMove={(e) => {
                    const svg = e.currentTarget as SVGSVGElement;
                    const rect = svg.getBoundingClientRect();
                    // convert client coords to SVG (viewBox) coordinates
                    const pt = svg.createSVGPoint();
                    pt.x = e.clientX;
                    pt.y = e.clientY;
                    const ctm = svg.getScreenCTM();
                    let svgX = null as number | null;
                    let svgY = null as number | null;
                    if (ctm) {
                      const svgP = pt.matrixTransform(ctm.inverse());
                      svgX = svgP.x;
                      svgY = svgP.y;
                    }

                    const BUCKETS_PER_DAY = hourlyFlowSeries.length;
                    if (svgX === null) {
                      return;
                    }

                    const ratio = (svgX - chartGeometry.marginLeft) / chartGeometry.plotWidth;
                    const clamped = Math.max(0, Math.min(1, ratio));
                    const bucket = Math.round(clamped * (BUCKETS_PER_DAY - 1));
                    setHoveredHour(bucket);

                    // hoveredMouse uses pixel coordinates relative to the chart container
                    const px = e.clientX - rect.left;
                    const py = e.clientY - rect.top;
                    setHoveredMouse({ x: px, y: py });
                  }}
                  onMouseLeave={() => {
                    setHoveredHour(null);
                    setHoveredMouse(null);
                  }}
                >
                  {yAxisTicks.map((tickValue: number, index: number) => {
                    const y = chartGeometry.marginTop + (index / (yAxisTicks.length - 1)) * chartGeometry.plotHeight;
                    return (
                      <g key={`y-tick-${index}`}>
                        <line
                          className="flow-grid-line"
                          x1={chartGeometry.marginLeft}
                          y1={y}
                          x2={chartGeometry.width - chartGeometry.marginRight}
                          y2={y}
                        />
                        <text className="flow-axis-label" x={chartGeometry.marginLeft - 10} y={y + 4} textAnchor="end">
                          {formatNumber(Math.round(tickValue))}
                        </text>
                      </g>
                    );
                  })}
                  <line
                    className="flow-axis-line"
                    x1={chartGeometry.marginLeft}
                    y1={chartGeometry.marginTop}
                    x2={chartGeometry.marginLeft}
                    y2={chartGeometry.height - chartGeometry.marginBottom}
                  />
                  <line
                    className="flow-axis-line"
                    x1={chartGeometry.marginLeft}
                    y1={chartGeometry.height - chartGeometry.marginBottom}
                    x2={chartGeometry.width - chartGeometry.marginRight}
                    y2={chartGeometry.height - chartGeometry.marginBottom}
                  />
                  {([0, 6, 12, 18, 24] as number[]).map((hourMark) => {
                    const BUCKETS_PER_DAY = hourlyFlowSeries.length;
                    const rawIndex = Math.round((hourMark * 60) / 3);
                    const bucketIndex = Math.min(BUCKETS_PER_DAY - 1, rawIndex);
                    const x = chartGeometry.marginLeft + (bucketIndex / (BUCKETS_PER_DAY - 1)) * chartGeometry.plotWidth;
                    const label = hourMark === 24 ? "24h" : `${hourMark}h`;
                    return (
                      <text
                        key={`x-tick-${hourMark}`}
                        className="flow-axis-label"
                        x={x}
                        y={chartGeometry.height - 12}
                        textAnchor={hourMark === 0 ? "start" : hourMark === 24 ? "end" : "middle"}
                      >
                        {label}
                      </text>
                    );
                  })}
                  <text className="flow-axis-title" x={18} y={chartGeometry.marginTop + chartGeometry.plotHeight / 2} transform={`rotate(-90 18 ${chartGeometry.marginTop + chartGeometry.plotHeight / 2})`}>
                    Flow (xe/3 phút)
                  </text>
                  <polyline className="flow-chart-line" points={hourlyFlowPoints} />
                  <polyline className="speed-chart-line" points={hourlySpeedPlot.map((p) => `${p.x},${p.ySpeed}`).join(" ")} />
                  {/* Right-side speed axis */}
                  {[1, 0.75, 0.5, 0.25, 0].map((ratio, index) => {
                    const safeMaxSpeed = maxAvgSpeed > 0 ? maxAvgSpeed : 1;
                    const y = chartGeometry.marginTop + (index / 4) * chartGeometry.plotHeight;
                    const label = Math.round(safeMaxSpeed * ratio);
                    return (
                      <g key={`y2-tick-${index}`}>
                        <text className="flow-axis-label" x={chartGeometry.width - chartGeometry.marginRight + 10} y={y + 4} textAnchor="start">
                          {label}
                        </text>
                      </g>
                    );
                  })}
                  <text className="flow-axis-label" x={chartGeometry.width - chartGeometry.marginRight + 10} y={chartGeometry.marginTop - 6} textAnchor="start">
                    km/h
                  </text>
                </svg>
                <div className="chart-legend">
                  <div className="legend-item"><span className="legend-swatch flow"/> Lưu lượng (xe/3ph)</div>
                  <div className="legend-item"><span className="legend-swatch speed"/> Tốc độ TB (km/h)</div>
                </div>
                {hoveredPoint && hoveredMouse ? (
                  <div
                    className={`flow-tooltip ${hoveredMouse.y < 120 ? "below" : "above"}`}
                    style={{
                      left: `${hoveredMouse.x}px`,
                      top: `${hoveredMouse.y}px`,
                    }}
                  >
                    <strong>{formatBucketTime(hoveredPoint.bucket)}</strong>
                    <span>Flow: {formatNumber(hoveredPoint.value)} xe/3 phút</span>
                    <span style={{ color: "#ef4444" }}>
                      Tốc độ TB: {hourlyAvgSpeedSeries?.[hoveredPoint.bucket] && hourlyAvgSpeedSeries[hoveredPoint.bucket] > 0 ? `${formatNumber(hourlyAvgSpeedSeries[hoveredPoint.bucket], 1)} km/h` : "N/A"}
                    </span>
                  </div>
                ) : null}
              </div>
            </div>
          ) : (
            <div className="empty-state light-empty">Không có dữ liệu ActualFlow để vẽ biểu đồ cho ngày/đường đã chọn.</div>
          )}
        </section>

        <section className="table-card light-card">
          <h2 className="section-title light-section-title">Dữ liệu thô</h2>
          <div className="date-hint" style={{ marginBottom: "12px" }}>
            Bảng này đang hiển thị {formatNumber(rawPageRows.length)} dòng trên trang {formatNumber(rawPage)} / {formatNumber(totalRawPages)}.
          </div>
          {sortedRawRows.length ? (
            <>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>ID</th>
                      {rowHeaders.map((column) => (
                        <th key={column}>
                          <button type="button" className="sort-button" onClick={() => toggleRawSort(column)}>
                            {column} {sortLabel(rawSortKey === column, rawSortDirection)}
                          </button>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rawPageRows.map((row, index) => (
                      <tr key={`${getRowIdentifier(row, (rawPage - 1) * RAW_PAGE_SIZE + index)}-${String(row.recordDatetime ?? "row")}`}>
                        <td>{getRowIdentifier(row, (rawPage - 1) * RAW_PAGE_SIZE + index)}</td>
                        {rowHeaders.map((column) => {
                          const value = row[column];
                          const text = value === null || value === undefined ? "" : String(value);
                          return <td key={column}>{text}</td>;
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="raw-pagination-bar">
                <div className="raw-pagination-summary">
                  Hiển thị {formatNumber((rawPage - 1) * RAW_PAGE_SIZE + 1)} - {formatNumber((rawPage - 1) * RAW_PAGE_SIZE + rawPageRows.length)} / {formatNumber(sortedRawRows.length)} dòng
                </div>
                <div className="raw-pagination-controls">
                  <button type="button" className="ghost-button light-button" onClick={() => goToRawPage(1)} disabled={rawPage <= 1}>
                    Trang đầu
                  </button>
                  <button type="button" className="ghost-button light-button" onClick={() => goToRawPage(rawPage - 1)} disabled={rawPage <= 1}>
                    Trang trước
                  </button>
                  <label className="raw-page-jump">
                    <span>Trang</span>
                    <input
                      type="number"
                      min={1}
                      max={totalRawPages}
                      value={rawPageInput}
                      onChange={(event) => setRawPageInput(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          const parsed = Number(rawPageInput);
                          if (Number.isFinite(parsed)) {
                            goToRawPage(parsed);
                          }
                        }
                      }}
                    />
                    <span>/ {formatNumber(totalRawPages)}</span>
                  </label>
                  <button
                    type="button"
                    className="submit-button"
                    onClick={() => {
                      const parsed = Number(rawPageInput);
                      if (Number.isFinite(parsed)) {
                        goToRawPage(parsed);
                      }
                    }}
                  >
                    Đi tới
                  </button>
                  <button type="button" className="ghost-button light-button" onClick={() => goToRawPage(rawPage + 1)} disabled={rawPage >= totalRawPages}>
                    Trang sau
                  </button>
                  <button type="button" className="ghost-button light-button" onClick={() => goToRawPage(totalRawPages)} disabled={rawPage >= totalRawPages}>
                    Trang cuối
                  </button>
                </div>
              </div>
            </>
          ) : (
            <div className="empty-state light-empty">Chọn ngày và bấm Submit để tải dữ liệu.</div>
          )}
        </section>
        </div>
      </div>
    </main>
  );
}
