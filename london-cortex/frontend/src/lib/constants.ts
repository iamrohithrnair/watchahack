export const LONDON_CENTER: [number, number] = [51.5074, -0.1278];
export const LONDON_ZOOM = 11;

export const POLL_INTERVALS = {
  map: 5000,
  anomalies: 3000,
  observations: 3000,
  discoveries: 5000,
  channels: 5000,
  stats: 10000,
  tasks: 10000,
  cameras: 30000,
  retina: 5000,
  images: 30000,
} as const;

export const LEVEL_COLORS: Record<string, string> = {
  DEBUG: "#A0A0B0",
  INFO: "#6366F1",
  WARNING: "#F59E0B",
  ERROR: "#EF4444",
  CRITICAL: "#DC2626",
};

export const SEVERITY_COLORS: Record<number, string> = {
  1: "#10B981",
  2: "#3B82F6",
  3: "#F59E0B",
  4: "#F97316",
  5: "#EF4444",
};

export const CHANNEL_LIST = [
  "#discoveries",
  "#hypotheses",
  "#anomalies",
  "#requests",
  "#meta",
] as const;
