"""
Pipeline entrypoint.

Usage:
  python main.py [YYYY-MM-DD]

Internally, main.py spark-submits itself with --pipeline to run all Spark
layers (bronze → silver → gold) in one SparkSession.  Landing scripts run
as supervised subprocesses alongside it.

Error handling:
  Landing subprocesses are supervised: on unexpected exit the supervisor
  waits restart_delay seconds then restarts with exponential backoff.
  After MAX_FAST_CRASHES consecutive fast crashes the supervisor gives up.
"""

import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Force VN timezone (GMT+7) before any datetime/loguru calls
os.environ["TZ"] = "Asia/Ho_Chi_Minh"
time.tzset()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

PROJECT_DIR  = os.path.dirname(os.path.abspath(__file__))
PYTHON       = sys.executable
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")

MIN_UPTIME_SEC   = 60
BASE_RESTART_SEC = 30
MAX_BACKOFF_SEC  = 300
MAX_FAST_CRASHES = 5

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master14:9092")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE MODE  (spark-submit main.py --pipeline [date])
# Runs inside the Spark executor: single SparkSession, all layers.
# ══════════════════════════════════════════════════════════════════════════════

def _run_pipeline(target_date: str | None) -> None:
    import threading, time
    from spark_session import get_spark
    import Bronze.bronze_processing as bronze
    import Silver.silver_processing as silver
    import Gold.gold_processing as gold

    spark = get_spark("Pipeline")
    # Race khi gold doc silver luc silver MERGE rewrite/xoa file -> SparkFileNotFoundException
    # lam crash ca pipeline. Bo qua file da biet mat thay vi crash. Chi dat trong pipeline mode
    # (streaming song song) — KHONG dat global de backfill van fail to neu gap file mat that.
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    def _wait(path, label, delay=30):
        while True:
            try:
                spark.read.format("delta").load(path).limit(0).count()
                print(f"[{label}] Delta table ready: {path}", flush=True)
                return
            except Exception:
                print(f"[{label}] Waiting for {path}…", flush=True)
                time.sleep(delay)

    def _start_silver():
        _wait(silver.TRAFFIC_BRONZE, "silver")
        _wait(silver.WEATHER_BRONZE, "silver")
        silver.start_streams(spark)
        print("[silver] Stream started.", flush=True)

    def _start_gold():
        _wait(gold.SILVER_PATH, "gold")
        gold.start_streams(spark)
        print("[gold] Stream started.", flush=True)

    bronze.start_streams(spark)
    print("[bronze] Streams started.", flush=True)

    threading.Thread(target=_start_silver, daemon=True).start()
    threading.Thread(target=_start_gold,   daemon=True).start()

    spark.streams.awaitAnyTermination()


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR MODE  (python main.py [date])
# Manages landing subprocesses + launches the Spark pipeline subprocess.
# ══════════════════════════════════════════════════════════════════════════════

from loguru import logger
from config import KAFKA_TOPICS

# Chỉ import metrics khi chạy ở orchestrator mode — không import khi spark-submit --pipeline
# vì prometheus_client có thể chưa có trong Python env của Spark driver
if "--pipeline" not in sys.argv:
    from utils.metrics import (
        start_metrics_server,
        process_up,
        restart_total,
        fast_crash_total,
        process_uptime_seconds,
        init_process_metrics
	)

_shutting_down = False
_log_fh        = None
_log_lock      = threading.Lock()

_SURFACE  = ("error", "exception", "traceback", "critical",
             "killed", "oom", "outofmemory", "warn")
_SURFACE_EXCLUDE = ("dagscheduler: failed: set()",)  # Spark INFO mislabelled
_PROGRESS = ("stream started", "delta table ready", "streams started",
             "batch", "trigger execution",
             "-> landing", "→ landing",   # landing write success
             "→ delta")


def _wait_for_kafka(retries: int = 20, delay: int = 15) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable

    logger.info(f"Checking Kafka at {KAFKA_BOOTSTRAP}…")
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5_000)
            existing = set(admin.list_topics())
            needed   = set(KAFKA_TOPICS.values())
            missing  = needed - existing
            if missing:
                admin.create_topics([
                    NewTopic(name=t, num_partitions=3, replication_factor=3)
                    for t in missing
                ])
                logger.info(f"Kafka: created missing topics {missing}")
            else:
                logger.info(f"Kafka: topics OK {needed}")
            admin.close()
            return
        except NoBrokersAvailable:
            logger.warning(f"Kafka not reachable (attempt {attempt}/{retries}) — retrying in {delay}s")
        except Exception as e:
            logger.warning(f"Kafka check failed (attempt {attempt}/{retries}): {e}")
        time.sleep(delay)

    logger.critical(f"Kafka unreachable after {retries} attempts.")
    sys.exit(1)


@dataclass
class ProcSpec:
    label:     str
    cmd:       list[str]
    date_args: list[str] = field(default_factory=list)
    proc:             subprocess.Popen | None = field(default=None, repr=False)
    fast_crash_count: int = 0
    _lock:            threading.Lock = field(default_factory=threading.Lock, repr=False)


def _setup_logging(date_label: str) -> Path:
    global _log_fh
    log_dir  = Path(PROJECT_DIR) / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"pipeline_{date_label}_{datetime.now().strftime('%H%M%S')}.log"
    logger.remove()
    logger.add(sys.stderr,       format="{time:HH:mm:ss} | {level:<7} | {message}", level="INFO",  colorize=False)
    logger.add(str(log_path),    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",     level="DEBUG")
    _log_fh = open(log_path, "a", buffering=1)
    logger.info(f"Full logs → {log_path}")
    return log_path


def _stream_log(proc: subprocess.Popen, label: str) -> None:
    def _reader():
        for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            ts   = datetime.now().strftime("%H:%M:%S")
            with _log_lock:
                if _log_fh:
                    _log_fh.write(f"{ts} [{label}] {line}\n")
                    _log_fh.flush()
            low = line.lower()
            if any(k in low for k in _SURFACE) and not any(x in low for x in _SURFACE_EXCLUDE):
                logger.warning(f"[{label}] {line}")
            elif any(k in low for k in _PROGRESS):
                logger.info(f"[{label}] {line}")
    threading.Thread(target=_reader, daemon=True).start()


_all_specs: list[ProcSpec] = []


def _uptime_tracker(spec: ProcSpec, started_at: float, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        process_uptime_seconds.labels(process=spec.label).set(time.monotonic() - started_at)
        time.sleep(5)


def _supervise(spec: ProcSpec) -> None:
    process_up.labels(process=spec.label).set(0)
    while not _shutting_down:
        started_at  = time.monotonic()
        stop_uptime = threading.Event()
        try:
            proc = subprocess.Popen(
                spec.cmd + spec.date_args,
                cwd=PROJECT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:
            logger.error(f"[{spec.label}] Failed to start: {e}")
            time.sleep(BASE_RESTART_SEC)
            continue

        with spec._lock:
            spec.proc = proc
        _stream_log(proc, spec.label)
        logger.info(f"[{spec.label}] Started pid={proc.pid}")

        process_up.labels(process=spec.label).set(1)
        process_uptime_seconds.labels(process=spec.label).set(0)
        threading.Thread(
            target=_uptime_tracker, args=(spec, started_at, stop_uptime), daemon=True
        ).start()

        ret    = proc.wait()
        uptime = time.monotonic() - started_at
        stop_uptime.set()

        process_up.labels(process=spec.label).set(0)

        if _shutting_down:
            return

        logger.warning(f"[{spec.label}] Exited (code {ret}) after {uptime:.0f}s")

        if uptime < MIN_UPTIME_SEC:
            spec.fast_crash_count += 1
            fast_crash_total.labels(process=spec.label).inc()
            if spec.fast_crash_count >= MAX_FAST_CRASHES:
                logger.critical(f"[{spec.label}] {spec.fast_crash_count} fast crashes — giving up.")
                return
            delay = min(BASE_RESTART_SEC * (2 ** (spec.fast_crash_count - 1)), MAX_BACKOFF_SEC)
            logger.warning(f"[{spec.label}] Fast crash #{spec.fast_crash_count} — retry in {delay}s")
        else:
            spec.fast_crash_count = 0
            delay = BASE_RESTART_SEC
            logger.info(f"[{spec.label}] Restarting in {delay}s…")

        restart_total.labels(process=spec.label).inc()
        time.sleep(delay)


def _start_supervised(spec: ProcSpec) -> None:
    threading.Thread(target=_supervise, args=(spec,), daemon=True).start()


def _shutdown(signum=None, frame=None) -> None:
    global _shutting_down
    _shutting_down = True
    logger.info("Shutting down — terminating all child processes…")
    for spec in _all_specs:
        with spec._lock:
            p = spec.proc
        if p:
            try: p.terminate()
            except OSError: pass
    time.sleep(10)
    for spec in _all_specs:
        with spec._lock:
            p = spec.proc
        if p:
            try: p.kill()
            except OSError: pass
    logger.info("All processes stopped.")
    sys.exit(0)

def _kafka_health_checker() -> None:
    """Luồng chạy ngầm liên tục kiểm tra Kafka, cập nhật trạng thái và Uptime"""
    from kafka.admin import KafkaAdminClient
    global _shutting_down
    logger.info("[kafka_checker] Started health check thread for Kafka Cluster.")
    
    kafka_started_at = None  # Biến ghi nhận thời điểm Kafka bắt đầu sống
    
    while not _shutting_down:
        try:
            # Thử kết nối nhanh tới Kafka
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=2000)
            admin.list_topics()
            admin.close()
            
            # 1. Cập nhật trạng thái UP (1)
            process_up.labels(process="kafka").set(1)
            
            # 2. Tính toán và cập nhật Uptime
            if kafka_started_at is None:
                kafka_started_at = time.monotonic()  # Đánh dấu mốc thời gian bắt đầu UP
            
            uptime_sec = time.monotonic() - kafka_started_at
            process_uptime_seconds.labels(process="kafka").set(uptime_sec)
            
        except Exception:
            # Nếu lỗi (Kafka sập hoặc chưa bật)
            process_up.labels(process="kafka").set(0)
            process_uptime_seconds.labels(process="kafka").set(0)
            kafka_started_at = None  # Reset lại mốc thời gian khi sập
            
        time.sleep(15)  # Kiểm tra định kỳ mỗi 15 giây

def main() -> None:
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    date_args   = [target_date] if target_date else []
    date_label  = target_date or datetime.now().strftime("%Y-%m-%d")

    _setup_logging(date_label)
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    start_metrics_server()
    logger.info(f"Prometheus metrics → http://localhost:8000/metrics")

    PROCESSES_TO_MONITOR = ['spark', 'kafka', 'retrieve_weather', 'replay_traffic', 'ui', 'backend']
    for p in PROCESSES_TO_MONITOR:
    	init_process_metrics(p)	

    _wait_for_kafka()
	# check kafka health
    threading.Thread(target=_kafka_health_checker, daemon=True).start()
    # ── 1. Traffic replay ─────────────────────────────────────────────────────
    logger.info("=== 1/5: replay_sumo_traffic ===")
    s1 = ProcSpec("replay_traffic",
                  [PYTHON, os.path.join(PROJECT_DIR, "Landing/replay_sumo_traffic.py")],
                  date_args)
    _all_specs.append(s1)
    _start_supervised(s1)

    # ── 2. Weather fetch ──────────────────────────────────────────────────────
    logger.info("=== 2/5: retrieve_data (weather) ===")
    s2 = ProcSpec("retrieve_weather",
                  [PYTHON, os.path.join(PROJECT_DIR, "Landing/retrieve_data.py")],
                  date_args)
    _all_specs.append(s2)
    _start_supervised(s2)

    # ── 3. Dashboard backend (FastAPI, port 8001) ────────────────────────────
    logger.info("=== 3/5: dashboard backend (port 8001) ===")
    BACKEND_DIR = os.path.join(PROJECT_DIR, "dashboard", "dashboard_Trafficflow", "backend")
    MYENV_UVICORN = "/home/dis/myenv/bin/uvicorn"
    s_backend = ProcSpec("backend",
                         ["bash", "-c",
                          f"fuser -k 8001/tcp 2>/dev/null; sleep 0.5 && "
                          f"cd {BACKEND_DIR!r} && {MYENV_UVICORN} app.main:app --host 0.0.0.0 --port 8001"])
    _all_specs.append(s_backend)
    _start_supervised(s_backend)

    # ── 4. Dashboard UI (Next.js, port 3000) ─────────────────────────────────
    logger.info("=== 4/5: dashboard UI (port 3000) ===")
    UI_DIR = os.path.join(PROJECT_DIR, "dashboard", "dashboard_Trafficflow", "frontend")
    s_ui = ProcSpec("ui",
                    ["bash", "-c", f"fuser -k 3000/tcp 2>/dev/null; sleep 0.5 && cd {UI_DIR!r} && npm start"])
    _all_specs.append(s_ui)
    _start_supervised(s_ui)

    # ── 5. Single Spark application (bronze → silver → gold) ─────────────────
    logger.info("=== 5/5: Spark pipeline (bronze → silver → gold) ===")
    pipeline_cmd = [
        SPARK_SUBMIT,
        "--packages", "io.delta:delta-spark_2.12:3.2.0",
	os.path.join(PROJECT_DIR, "main.py"),
        "--pipeline",
    ] + date_args
    s3 = ProcSpec("spark", pipeline_cmd)
    _all_specs.append(s3)
    _start_supervised(s3)

    logger.info("All components running — Ctrl+C to stop.")
    try:
        while not _shutting_down:
            time.sleep(5)
    except KeyboardInterrupt:
        _shutdown()


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--pipeline" in sys.argv:
        # Remove the flag; pass remaining positional args as date
        args = [a for a in sys.argv[1:] if a != "--pipeline"]
        _run_pipeline(args[0] if args else None)
    else:
        main()
