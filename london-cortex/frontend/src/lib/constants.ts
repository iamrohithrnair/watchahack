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
  DEBUG:    "#9A8E86",
  INFO:     "#5B7A90",
  WARNING:  "#C09440",
  ERROR:    "#B85048",
  CRITICAL: "#8C3830",
};

export const SEVERITY_COLORS: Record<number, string> = {
  1: "#4D7C58",
  2: "#5B7A90",
  3: "#C09440",
  4: "#B8603A",
  5: "#B85048",
};

export const CHANNEL_LIST = [
  "#discoveries",
  "#hypotheses",
  "#anomalies",
  "#requests",
  "#meta",
] as const;
