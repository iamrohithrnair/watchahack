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
  DEBUG: "#888",
  INFO: "#3b82f6",
  WARNING: "#f59e0b",
  ERROR: "#ef4444",
  CRITICAL: "#dc2626",
};

export const SEVERITY_COLORS: Record<number, string> = {
  1: "#22c55e",
  2: "#3b82f6",
  3: "#f59e0b",
  4: "#f97316",
  5: "#ef4444",
};

export const CHANNEL_LIST = [
  "#discoveries",
  "#hypotheses",
  "#anomalies",
  "#requests",
  "#meta",
] as const;
