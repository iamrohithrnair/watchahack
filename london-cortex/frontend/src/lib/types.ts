export interface Observation {
  id: string;
  source: string;
  obs_type: "image" | "numeric" | "text" | "geo" | "categorical" | "event";
  value: string | number | null;
  location_id: string | null;
  lat: number | null;
  lon: number | null;
  metadata: Record<string, unknown>;
  timestamp: string;
}

export interface Anomaly {
  id: string;
  source: string;
  observation_id: string | null;
  description: string;
  z_score: number;
  location_id: string | null;
  lat: number | null;
  lon: number | null;
  severity: number;
  metadata: Record<string, unknown>;
  timestamp: string;
  ttl_hours: number;
}

export interface AgentMessage {
  id: string;
  from_agent: string;
  to_agent: string | null;
  channel: string;
  priority: number;
  content: string;
  data: Record<string, unknown>;
  references_json: string[];
  location_id: string | null;
  ttl_hours: number;
  timestamp: string;
}

export interface TaskHealth {
  [taskName: string]: {
    consecutive_errors: number;
    last_run: string | null;
    last_success: string | null;
    current_interval: number;
    min_interval: number;
    max_interval: number;
  };
}

export interface MapData {
  anomalies: Anomaly[];
  observations: MapObservation[];
  discoveries: MapDiscovery[];
  obs_total: number;
  obs_focused: number;
}

export interface MapObservation {
  id: string;
  lat: number;
  lon: number;
  source: string;
  obs_type: string;
  timestamp: string;
  location_id: string;
  value: string | number | null;
  zone: number;
  station?: string;
  species?: string;
  site_name?: string;
  camera_id?: string;
  description?: string;
  road?: string;
  borough?: string;
}

export interface MapDiscovery {
  lat: number;
  lon: number;
  from_agent: string;
  content: string;
  timestamp: string;
  location_id: string;
}

export interface RetinaState {
  cells: RetinaCell[];
  stats: Record<string, number>;
  recent_saccades: Saccade[];
}

export interface RetinaCell {
  cell_id: string;
  zone: number;
  score: number;
  center_lat: number;
  center_lon: number;
}

export interface Saccade {
  from_cell: string;
  to_cell: string;
  reason: string;
  timestamp: string;
}

export interface CameraSummary {
  counts: { active: number; suspect: number; dead: number };
  total_tracked: number;
  dead_cameras: string[];
  top_active: { id: string; hours: number }[];
}

export interface LogEntry {
  time: string;
  level: string;
  name: string;
  msg: string;
}

export interface ChannelData {
  [channel: string]: AgentMessage[];
}

export interface Stats {
  observations: number;
  anomalies: number;
  connections: number;
  messages: number;
}

export interface ImageInfo {
  name: string;
  mtime: number;
}
