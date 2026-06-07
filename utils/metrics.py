"""
Prometheus metrics registry cho pipeline real-time (main.py).
Gọi start_metrics_server() một lần khi khởi động orchestrator.
"""

from prometheus_client import Counter, Gauge, start_http_server

METRICS_PORT = 8000

# ── Process health ─────────────────────────────────────────────────────────────

process_up = Gauge(
    "pipeline_process_up",
    "1 nếu process đang chạy, 0 nếu đã thoát",
    ["process"],
)

restart_total = Counter(
    "pipeline_restart_total",
    "Tổng số lần restart của mỗi process",
    ["process"],
)

fast_crash_total = Counter(
    "pipeline_fast_crash_total",
    "Số lần crash dưới MIN_UPTIME_SEC",
    ["process"],
)

process_uptime_seconds = Gauge(
    "pipeline_process_uptime_seconds",
    "Thời gian (giây) process hiện tại đã chạy",
    ["process"],
)

def init_process_metrics(process_name: str) -> None:
    """
    Hàm bùa chú: Gọi hàm này ngay khi hệ thống bắt đầu giám sát một process nào đó.
    Nó sẽ ép Prometheus sinh ra số 0 cho mọi metric của process đó ngay lập tức!
    """
    process_up.labels(process=process_name).set(0)
    restart_total.labels(process=process_name).inc(0)
    fast_crash_total.labels(process=process_name).inc(0)  # <-- Gọi kèm label như này mới đúng bài
    process_uptime_seconds.labels(process=process_name).set(0)

def start_metrics_server(port: int = METRICS_PORT) -> None:
    start_http_server(port)
