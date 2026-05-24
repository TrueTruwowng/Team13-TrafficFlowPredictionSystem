import json
import math
import os

_LOOKUP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "osm_way_lookup.json")
_lookup: list | None = None


def _load():
    global _lookup
    if _lookup is None:
        with open(_LOOKUP_PATH, encoding="utf-8") as f:
            _lookup = json.load(f)


def _dist(lat1, lon1, lat2, lon2) -> float:
    dlat = (lat2 - lat1) * 111_000
    dlon = (lon2 - lon1) * 111_000 * math.cos(math.radians(lat1))
    return math.sqrt(dlat ** 2 + dlon ** 2)


def find_nearest_way(lat: float, lon: float) -> dict:
    """
    Given a coordinate, return the nearest OSM way entry from osm_way_lookup.json.

    Returns:
        {"osm_id": str, "name": str, "highway": str, "lat": float, "lon": float, "dist_m": float}
    """
    _load()
    best = min(_lookup, key=lambda w: _dist(lat, lon, w["lat"], w["lon"]))
    return {**best, "dist_m": round(_dist(lat, lon, best["lat"], best["lon"]), 1)}
