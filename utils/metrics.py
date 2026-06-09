from prometheus_client import Counter, Gauge, start_http_server

process_up = Gauge(
    "pipeline_process_up",
    "1 if process is running, 0 if exited",
    ["process"],
)

restart_total = Counter(
    "pipeline_restart_total",
    "Total restart count per process",
    ["process"],
)

fast_crash_total = Counter(
    "pipeline_fast_crash_total",
    "Crashes within MIN_UPTIME_SEC",
    ["process"],
)

process_uptime_seconds = Gauge(
    "pipeline_process_uptime_seconds",
    "Seconds the current process instance has been running",
    ["process"],
)


def init_process_metrics(process_name: str) -> None:
    """Initialise all metrics for a process to zero so Prometheus sees them immediately."""
    process_up.labels(process=process_name).set(0)
    restart_total.labels(process=process_name).inc(0)
    fast_crash_total.labels(process=process_name).inc(0)
    process_uptime_seconds.labels(process=process_name).set(0)


def start_metrics_server(port: int = 8000) -> None:
    start_http_server(port)
