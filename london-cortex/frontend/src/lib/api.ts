const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export async function fetchAPI<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export const api = {
  health: () => fetchAPI<{ status: string }>("/api/health"),
  stats: () => fetchAPI<import("./types").Stats>("/api/stats"),
  anomalies: () => fetchAPI<import("./types").Anomaly[]>("/api/anomalies"),
  observations: () => fetchAPI<import("./types").Observation[]>("/api/observations"),
  discoveries: () => fetchAPI<import("./types").AgentMessage[]>("/api/discoveries"),
  tasks: () => fetchAPI<import("./types").TaskHealth>("/api/tasks"),
  mapData: () => fetchAPI<import("./types").MapData>("/api/map_data"),
  channels: () => fetchAPI<import("./types").ChannelData>("/api/channels"),
  retina: () => fetchAPI<import("./types").RetinaState>("/api/retina"),
  cameras: () => fetchAPI<import("./types").CameraSummary>("/api/cameras"),
  images: () => fetchAPI<import("./types").ImageInfo[]>("/api/images"),
};
