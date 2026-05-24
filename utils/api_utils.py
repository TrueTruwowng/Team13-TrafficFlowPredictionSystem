import requests
import logging
from datetime import datetime, date
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import now_vn

logger = logging.getLogger(__name__)

# WMO Weather Interpretation Code → mô tả ngắn
WMO_CODE_MAP = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Heavy showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}

OPENMETEO_URL         = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARIABLES = "temperature_2m,precipitation,weathercode,windspeed_10m,relativehumidity_2m"


class WeatherAPI:
    """Fetches current weather data from Open-Meteo (no API key required)."""

    def get_weather(self, lat, lon):
        try:
            params = {
                "latitude":        lat,
                "longitude":       lon,
                "current_weather": True,
                "hourly":          "relativehumidity_2m,precipitation",
                "timezone":        "Asia/Bangkok",
                "forecast_days":   1,
            }
            res = requests.get(OPENMETEO_URL, params=params, timeout=10)

            if res.status_code != 200:
                logger.error(f"Open-Meteo error {res.status_code}: {res.text}")
                return None

            body = res.json()
            cw   = body.get("current_weather", {})
            if not cw:
                logger.warning(f"Open-Meteo: current_weather trống cho ({lat}, {lon})")
                return None

            # Lấy humidity tại giờ hiện tại từ hourly data
            current_hour_str = cw.get("time", "")            # "2024-01-01T12:00"
            hourly           = body.get("hourly", {})
            humidity         = None
            precipitation    = None
            if "time" in hourly and current_hour_str in hourly["time"]:
                idx           = hourly["time"].index(current_hour_str)
                humidity      = hourly.get("relativehumidity_2m", [None])[idx]
                precipitation = hourly.get("precipitation", [None])[idx]

            weather_code = cw.get("weathercode", 0)
            return {
                "temperature":   cw.get("temperature"),
                "windspeed":     cw.get("windspeed"),
                "weathercode":   weather_code,
                "weather":       WMO_CODE_MAP.get(weather_code, "Unknown"),
                "humidity":      humidity,
                "precipitation": precipitation if precipitation is not None else 0.0,
                "ingestion_time": now_vn().isoformat(),
            }

        except Exception as e:
            logger.error(f"Lỗi kết nối Open-Meteo: {e}")
            return None

    def get_historical_weather(self, lat, lon, start_date, end_date):
        """
        Lấy dữ liệu thời tiết lịch sử theo giờ từ Open-Meteo Archive API.

        Args:
            lat, lon   : tọa độ
            start_date : str hoặc date, vd "2026-01-01"
            end_date   : str hoặc date, vd "2026-05-01"

        Returns:
            list[dict] — mỗi phần tử là 1 bản ghi theo giờ, hoặc [] nếu lỗi.
        """
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y-%m-%d")

        try:
            params = {
                "latitude":   lat,
                "longitude":  lon,
                "start_date": start_date,
                "end_date":   end_date,
                "hourly":     HOURLY_VARIABLES,
                "timezone":   "Asia/Bangkok",
            }
            res = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=30)

            if res.status_code != 200:
                logger.error(f"Open-Meteo Archive error {res.status_code}: {res.text}")
                return []

            hourly = res.json().get("hourly", {})
            times  = hourly.get("time", [])
            if not times:
                logger.warning(f"Open-Meteo Archive: không có dữ liệu cho ({lat}, {lon})")
                return []

            records = []
            for i, t in enumerate(times):
                code = hourly.get("weathercode", [])[i]
                records.append({
                    "datetime":      t,                          # "2026-01-01T00:00"
                    "temperature":   hourly.get("temperature_2m",       [])[i],
                    "precipitation": hourly.get("precipitation",         [])[i],
                    "weathercode":   code,
                    "weather":       WMO_CODE_MAP.get(code, "Unknown"),
                    "windspeed":     hourly.get("windspeed_10m",         [])[i],
                    "humidity":      hourly.get("relativehumidity_2m",   [])[i],
                    "ingestion_time": now_vn().isoformat(),
                })

            logger.info(f"Lấy được {len(records)} bản ghi lịch sử ({start_date} → {end_date})")
            return records

        except Exception as e:
            logger.error(f"Lỗi kết nối Open-Meteo Archive: {e}")
            return []
