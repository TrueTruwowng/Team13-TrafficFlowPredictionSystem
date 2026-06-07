from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
from pathlib import Path
import xml.etree.ElementTree as ET

from app.schemas import MapBoundsResponse, MapFeatureResponse, MapGeometryResponse


def _sumo_edge_base(edge_id: str) -> str | None:
    if not edge_id or edge_id.startswith(":"):
        return None
    base = edge_id.lstrip("-").split("#", 1)[0]
    return base or None


class NghiaDoMapRepository:
    def __init__(self, osm_xml_path: Path, net_xml_path: Path) -> None:
        self.osm_xml_path = osm_xml_path
        self.net_xml_path = net_xml_path

    def load_geometry(self) -> MapGeometryResponse:
        return _load_geometry(str(self.osm_xml_path), str(self.net_xml_path))


@lru_cache(maxsize=4)
def _load_geometry(osm_xml_path: str, net_xml_path: str) -> MapGeometryResponse:
    osm_path = Path(osm_xml_path)
    net_path = Path(net_xml_path)
    if not osm_path.exists():
        raise FileNotFoundError(f"OSM XML not found: {osm_path}")
    if not net_path.exists():
        raise FileNotFoundError(f"SUMO net XML not found: {net_path}")

    edge_ids_by_way = _load_sumo_edge_ids_by_osm_way(net_path)
    nodes: dict[str, tuple[float, float]] = {}
    features: list[MapFeatureResponse] = []
    bounds: MapBoundsResponse | None = None

    for _, element in ET.iterparse(osm_path, events=("end",)):
        if element.tag == "bounds":
            bounds = MapBoundsResponse(
                min_lat=float(element.attrib["minlat"]),
                min_lon=float(element.attrib["minlon"]),
                max_lat=float(element.attrib["maxlat"]),
                max_lon=float(element.attrib["maxlon"]),
            )
        elif element.tag == "node":
            node_id = element.attrib.get("id")
            lat = element.attrib.get("lat")
            lon = element.attrib.get("lon")
            if node_id and lat and lon:
                nodes[node_id] = (float(lat), float(lon))
        elif element.tag == "way":
            way_id = element.attrib.get("id")
            if way_id:
                feature = _build_way_feature(element, way_id, nodes, edge_ids_by_way.get(way_id, []))
                if feature is not None:
                    features.append(feature)
            element.clear()

    if bounds is None:
        bounds = _bounds_from_features(features)

    return MapGeometryResponse(bounds=bounds, features=features)


def _load_sumo_edge_ids_by_osm_way(net_path: Path) -> dict[str, list[str]]:
    edge_ids_by_way: dict[str, list[str]] = defaultdict(list)
    for _, element in ET.iterparse(net_path, events=("end",)):
        if element.tag != "edge":
            continue

        edge_id = element.attrib.get("id", "")
        base = _sumo_edge_base(edge_id)
        if base is not None:
            edge_ids_by_way[base].append(edge_id)
        element.clear()

    return {way_id: sorted(set(edge_ids)) for way_id, edge_ids in edge_ids_by_way.items()}


def _build_way_feature(
    element: ET.Element,
    way_id: str,
    nodes: dict[str, tuple[float, float]],
    sumo_edge_ids: list[str],
) -> MapFeatureResponse | None:
    tags = {child.attrib.get("k"): child.attrib.get("v") for child in element if child.tag == "tag"}
    highway = tags.get("highway")
    name = tags.get("name")
    if not highway or not name or not sumo_edge_ids:
        return None

    coordinates: list[list[float]] = []
    for child in element:
        if child.tag != "nd":
            continue
        node_ref = child.attrib.get("ref")
        point = nodes.get(node_ref or "")
        if point is not None:
            lat, lon = point
            coordinates.append([lat, lon])

    if len(coordinates) < 2:
        return None

    return MapFeatureResponse(
        osm_way_id=way_id,
        sumo_edge_ids=sumo_edge_ids,
        name=name,
        highway=highway,
        coordinates=coordinates,
    )


def _bounds_from_features(features: list[MapFeatureResponse]) -> MapBoundsResponse:
    latitudes = [point[0] for feature in features for point in feature.coordinates]
    longitudes = [point[1] for feature in features for point in feature.coordinates]
    return MapBoundsResponse(
        min_lat=min(latitudes, default=0),
        min_lon=min(longitudes, default=0),
        max_lat=max(latitudes, default=0),
        max_lon=max(longitudes, default=0),
    )
