### Separate script to retrieve SUMO traffic data, parse it, and store in GCS as JSON for Kafka to pick up. Run after SUMO finishes.
import json
import os
import sys
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import GCS_BUCKET

load_dotenv()

BUCKET_NAME        = os.getenv("GCS_BUCKET_NAME")
GCS_PREFIX         = "simulation/traffic"
CHECKPOINT_BLOB    = f"{GCS_PREFIX}/_checkpoint.json"
OSM_FILE           = "/home/dis/sumo_project/nghia_do_cut.osm.xml"
SOURCE_BUCKET_NAME = "big-data-storage13"
SOURCE_PREFIX      = "output_sumo"

if not BUCKET_NAME:
    logger.critical("Thiếu GCS_BUCKET_NAME!")
    sys.exit(1)


def _get_buckets():
    from google.cloud import storage
    client = storage.Client()
    return client.bucket(GCS_BUCKET), client.bucket(SOURCE_BUCKET_NAME)


def _parse_date_from_filename(filename: str) -> str:
    """'test_01-05-2026_output.xml' → '2026-05-01'"""
    m = re.search(r"(\d{2}-\d{2}-\d{4})", filename)
    if not m:
        raise ValueError(f"Không tìm thấy ngày trong tên file: {filename}")
    return datetime.strptime(m.group(1), "%d-%m-%Y").strftime("%Y-%m-%d")


def _read_checkpoint(bucket) -> set:
    blob = bucket.blob(CHECKPOINT_BLOB)
    if blob.exists():
        return set(json.loads(blob.download_as_text()).get("processed", []))
    return set()


def _write_checkpoint(bucket, processed: set):
    bucket.blob(CHECKPOINT_BLOB).upload_from_string(
        json.dumps({"processed": sorted(processed)}),
        content_type="application/json",
    )


def _build_edge_name_map(osm_path: str):
    tree = ET.parse(osm_path)
    root = tree.getroot()
    way_names = {}
    for way in root.findall("way"):
        name_tag = way.find("tag[@k='name']")
        if name_tag is not None:
            way_names[way.get("id")] = name_tag.get("v")

    def edge_to_way(edge_id: str) -> str:
        return edge_id.lstrip("-").split("#")[0]

    return way_names, edge_to_way


def _to_hhmm(seconds: str) -> str:
    total_min = int(float(seconds)) // 60
    return f"{total_min // 60:02d}:{total_min % 60:02d}"


def _hhmm_to_compact(hhmm: str) -> str:
    """'06:30' → '0630' dùng cho tên file"""
    return hhmm.replace(":", "")


def _process_file(xml_path: str, date_str: str,
                  way_names: dict, edge_to_way, bucket) -> bool:
    """Iterparse toàn bộ XML, ghi thẳng mỗi interval lên GCS simulation/traffic/."""
    total_intervals = 0

    try:
        for event, elem in ET.iterparse(xml_path, events=["end"]):
            if elem.tag != "interval":
                continue

            begin_s = elem.get("begin")
            end_s   = elem.get("end")
            begin   = _to_hhmm(begin_s)
            end     = _to_hhmm(end_s)

            records = []
            for edge in elem.findall("edge"):
                edge_id = edge.get("id", "")
                if edge_id.startswith(":"):
                    continue
                records.append({
                    "date":      date_str,
                    "begin":     begin,
                    "end":       end,
                    "road_name": way_names.get(edge_to_way(edge_id)),
                    **edge.attrib,
                })

            elem.clear()

            if not records:
                continue

            gcs_path = f"{GCS_PREFIX}/{date_str}/{date_str}_{_hhmm_to_compact(end)}.json"
            bucket.blob(gcs_path).upload_from_string(
                json.dumps(records, ensure_ascii=False),
                content_type="application/json",
            )
            total_intervals += 1
            logger.info(f"  [{date_str} {begin}→{end}] {len(records)} edges → {gcs_path}")

    except ET.ParseError as e:
        logger.error(f"XML lỗi/chưa ghi xong: {os.path.basename(xml_path)} ({e})")
        return False

    logger.info(f"Done {date_str}: {total_intervals} intervals")
    return total_intervals > 0


def _select_date_range(blobs: list, start_date: str | None, end_date: str | None) -> list:
    """Lọc blobs theo khoảng ngày [start_date, end_date] (YYYY-MM-DD, bao gồm 2 đầu).
    None ở đầu nào thì không giới hạn đầu đó."""
    def _parse_bound(d: str, name: str):
        try:
            return datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"{name} phải theo định dạng YYYY-MM-DD, nhận được: {d!r}")

    lo = _parse_bound(start_date, "start_date") if start_date else None
    hi = _parse_bound(end_date,   "end_date")   if end_date   else None
    if lo is None and hi is None:
        return blobs

    out = []
    for b in blobs:
        d = datetime.strptime(
            _parse_date_from_filename(os.path.basename(b.name)), "%Y-%m-%d"
        ).date()
        if lo is not None and d < lo:
            continue
        if hi is not None and d > hi:
            continue
        out.append(b)
    return out


def retrieve_sumo_traffic(start_date: str | None = None, end_date: str | None = None,
                          overwrite: bool = False):
    """Chạy 1 lần rồi thoát — Airflow lo schedule.

    Args:
        start_date: Ngày bắt đầu chia khoảng (YYYY-MM-DD). Bỏ qua các file trước ngày này.
                    None = không giới hạn đầu dưới.
        end_date:   Ngày kết thúc (YYYY-MM-DD, bao gồm). Bỏ qua các file sau ngày này.
                    None = không giới hạn đầu trên.
        overwrite:  True = xử lý lại và ghi đè ngay cả khi file đã có trong checkpoint.
    """
    import tempfile
    dest_bucket, src_bucket = _get_buckets()
    processed = _read_checkpoint(dest_bucket)

    blobs = [
        b for b in src_bucket.list_blobs(prefix=SOURCE_PREFIX)
        if b.name.endswith("_output.xml")
    ]
    blobs = _select_date_range(blobs, start_date, end_date)
    pending = sorted(
        blobs if overwrite else
        [b for b in blobs if _parse_date_from_filename(os.path.basename(b.name)) not in processed],
        key=lambda b: _parse_date_from_filename(os.path.basename(b.name)),
    )

    if not pending:
        logger.info("Không có file SUMO mới.")
        return

    logger.info(f"Tìm thấy {len(pending)} file cần xử lý.")
    way_names, edge_to_way = _build_edge_name_map(OSM_FILE)

    for blob in pending:
        filename = os.path.basename(blob.name)
        date_str = _parse_date_from_filename(filename)
        logger.info(f"Tải + xử lý: {blob.name}")

        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            blob.download_to_filename(tmp_path)
            ok = _process_file(tmp_path, date_str, way_names, edge_to_way, dest_bucket)
        finally:
            os.unlink(tmp_path)

        if ok:
            processed.add(date_str)
            _write_checkpoint(dest_bucket, processed)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Retrieve SUMO traffic data và đẩy lên GCS.")
    parser.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Chỉ xử lý các file từ ngày này trở đi (bỏ qua các file trước đó).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Chỉ xử lý các file đến ngày này (bao gồm). Bỏ qua các file sau đó.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Ghi đè file đã xử lý trước đó (bỏ qua checkpoint).",
    )
    args = parser.parse_args()
    retrieve_sumo_traffic(start_date=args.start_date, end_date=args.end_date, overwrite=args.overwrite)
