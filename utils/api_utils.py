import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class TrafficWeatherAPI:
    def __init__(self, tomtom_key, weather_key):
        self.tomtom_key = tomtom_key
        self.weather_key = weather_key

    def get_traffic_flow(self, coor):
        # Lưu ý: Zoom level 15 là rất chi tiết, đôi khi tọa độ không khớp chính xác đoạn đường sẽ rỗng
        url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/15/json"
        try:
            params = {"key": self.tomtom_key, "point": coor}
            res = requests.get(url, params=params, timeout=10)
            
            if res.status_code == 200:
                data = res.json().get("flowSegmentData")
                if data:
                    data["ingestion_time"] = datetime.now().isoformat()
                    return data
                else:
                    logger.warning(f"⚠️ TomTom trả về 200 nhưng 'flowSegmentData' bị trống cho tọa độ: {coor}")
            else:
                # Đây là nơi "bắt bệnh" quan trọng nhất
                logger.error(f"❌ TomTom API Error {res.status_code}: {res.text}")
                
        except Exception as e:
            logger.error(f"📡 Lỗi kết nối Traffic API: {str(e)}")
        return None

    def get_weather(self, lat, lon):
        url = "https://api.openweathermap.org/data/2.5/weather"
        try:
            params = {"lat": lat, "lon": lon, "appid": self.weather_key, "units": "metric"}
            res = requests.get(url, params=params, timeout=10)
            
            if res.status_code == 200:
                data = res.json()
                data["ingestion_time"] = datetime.now().isoformat()
                return data
            else:
                logger.error(f"❌ Weather API Error {res.status_code}: {res.text}")
                
        except Exception as e:
            logger.error(f"☁️ Lỗi kết nối Weather API: {str(e)}")
        return None