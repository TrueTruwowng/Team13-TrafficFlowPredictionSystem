export type DatasetObject = {
  name: string;
};

export type DatasetListResponse = {
  bucket: string;
  prefix: string;
  items: DatasetObject[];
};

export type DayDataResponse = {
  target_date: string;
  columns: string[];
  row_count: number;
  rows: Record<string, unknown>[];
};

export type LatestLayerDataResponse = {
  layer: string;
  prefix: string;
  target_date: string | null;
  object_count: number;
  columns: string[];
  row_count: number;
  rows: Record<string, unknown>[];
};

export type LayerDatesResponse = {
  layer: string;
  prefix: string;
  dates: string[];
};

export type MapBoundsResponse = {
  min_lat: number;
  min_lon: number;
  max_lat: number;
  max_lon: number;
};

export type MapFeatureResponse = {
  osm_way_id: string;
  sumo_edge_ids: string[];
  name: string | null;
  highway: string | null;
  coordinates: number[][];
};

export type MapGeometryResponse = {
  bounds: MapBoundsResponse;
  features: MapFeatureResponse[];
};

export type DayRoadCountResponse = {
  target_date: string;
  unique_roads: number;
  roads: string[];
};

export type DateRangeDataResponse = {
  start_date: string;
  end_date: string;
  columns: string[];
  row_count: number;
  rows: Record<string, unknown>[];
};
