# London Cortex Next.js Dashboard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-file HTML dashboard with a production-grade Next.js 15 application while keeping the Python backend unchanged.

**Architecture:** Python aiohttp backend (port 8000) serves API endpoints. Next.js 15 App Router (port 3000) consumes the API using SWR for polling and SSE for real-time log streaming. Docker Compose orchestrates both services.

**Tech Stack:** Next.js 15, React 19, TypeScript 5, Tailwind CSS 4, SWR 2, react-leaflet 4, @tanstack/react-virtual 3

---

### Task 1: Scaffold Next.js App

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/next.config.ts`
- Create: `frontend/tsconfig.json`
- Create: `frontend/tailwind.config.ts`
- Create: `frontend/postcss.config.js`
- Create: `frontend/src/app/layout.tsx`
- Create: `frontend/src/app/page.tsx`
- Create: `frontend/src/styles/globals.css`
- Create: `frontend/.gitignore`

- [ ] **Step 1: Create the frontend directory and initialize Next.js**

```bash
cd /Users/Rohithn/AI/experiments/watchahack
mkdir -p london-cortex/frontend/src
```

- [ ] **Step 2: Create package.json**

Create `london-cortex/frontend/package.json`:

```json
{
  "name": "london-cortex-dashboard",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "next lint"
  },
  "dependencies": {
    "next": "^15.3.0",
    "react": "^19.1.0",
    "react-dom": "^19.1.0",
    "swr": "^2.3.0",
    "react-leaflet": "^5.0.0",
    "leaflet": "^1.9.4",
    "@tanstack/react-virtual": "^3.13.0"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "@types/react": "^19.0.0",
    "@types/react-dom": "^19.0.0",
    "@types/leaflet": "^1.9.0",
    "typescript": "^5.7.0",
    "tailwindcss": "^4.1.0",
    "@tailwindcss/postcss": "^4.1.0",
    "postcss": "^8.5.0"
  }
}
```

- [ ] **Step 3: Create next.config.ts**

Create `london-cortex/frontend/next.config.ts`:

```typescript
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/:path*`,
      },
      {
        source: "/ws",
        destination: `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/ws`,
      },
      {
        source: "/stream",
        destination: `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/stream`,
      },
      {
        source: "/images/:path*",
        destination: `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/images/:path*`,
      },
    ];
  },
};

export default nextConfig;
```

- [ ] **Step 4: Create tsconfig.json**

Create `london-cortex/frontend/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2017",
    "lib": ["dom", "dom.iterable", "esnext"],
    "allowJs": true,
    "skipLibCheck": true,
    "strict": true,
    "noEmit": true,
    "esModuleInterop": true,
    "module": "esnext",
    "moduleResolution": "bundler",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "jsx": "preserve",
    "incremental": true,
    "plugins": [{ "name": "next" }],
    "paths": { "@/*": ["./src/*"] }
  },
  "include": ["next-env.d.ts", "**/*.ts", "**/*.tsx", ".next/types/**/*.ts"],
  "exclude": ["node_modules"]
}
```

- [ ] **Step 5: Create postcss.config.js**

Create `london-cortex/frontend/postcss.config.js`:

```javascript
module.exports = {
  plugins: {
    "@tailwindcss/postcss": {},
  },
};
```

- [ ] **Step 6: Create globals.css**

Create `london-cortex/frontend/src/styles/globals.css`:

```css
@import "tailwindcss";

:root {
  --bg-primary: #0a0a0f;
  --bg-secondary: #12121a;
  --bg-card: #1a1a2e;
  --border: #2a2a3e;
  --text-primary: #e4e4f0;
  --text-secondary: #8888a0;
  --accent: #6366f1;
  --accent-hover: #818cf8;
  --success: #22c55e;
  --warning: #f59e0b;
  --danger: #ef4444;
  --info: #3b82f6;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  padding: 0;
  background: var(--bg-primary);
  color: var(--text-primary);
  font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}

::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

::-webkit-scrollbar-track {
  background: var(--bg-secondary);
}

::-webkit-scrollbar-thumb {
  background: var(--border);
  border-radius: 3px;
}

::-webkit-scrollbar-thumb:hover {
  background: var(--text-secondary);
}
```

- [ ] **Step 7: Create root layout**

Create `london-cortex/frontend/src/app/layout.tsx`:

```tsx
import "@/styles/globals.css";

export const metadata = {
  title: "London Cortex",
  description: "Autonomous city intelligence dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link
          rel="stylesheet"
          href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
          integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
          crossOrigin=""
        />
      </head>
      <body className="bg-[var(--bg-primary)] text-[var(--text-primary)]">
        {children}
      </body>
    </html>
  );
}
```

- [ ] **Step 8: Create placeholder home page**

Create `london-cortex/frontend/src/app/page.tsx`:

```tsx
export default function Home() {
  return (
    <main className="flex h-screen items-center justify-center">
      <div className="text-center">
        <h1 className="text-4xl font-bold text-[var(--accent)]">London Cortex</h1>
        <p className="mt-2 text-[var(--text-secondary)]">Dashboard loading...</p>
      </div>
    </main>
  );
}
```

- [ ] **Step 9: Create frontend .gitignore**

Create `london-cortex/frontend/.gitignore`:

```
node_modules/
.next/
out/
.env
.env.local
.env.production
```

- [ ] **Step 10: Install dependencies and verify build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm install
npm run build
```

Expected: Build succeeds with no errors.

- [ ] **Step 11: Commit**

```bash
git add london-cortex/frontend/
git commit -m "feat: scaffold Next.js 15 frontend with Tailwind CSS and TypeScript"
```

---

### Task 2: TypeScript Types and API Client

**Files:**
- Create: `frontend/src/lib/types.ts`
- Create: `frontend/src/lib/api.ts`
- Create: `frontend/src/lib/constants.ts`

- [ ] **Step 1: Create TypeScript types matching the Python backend models**

Create `london-cortex/frontend/src/lib/types.ts`:

```typescript
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

export interface Connection {
  id: string;
  source_a: string;
  source_b: string;
  description: string;
  confidence: number;
  evidence: string[];
  prediction: string | null;
  prediction_deadline: string | null;
  prediction_outcome: boolean | null;
  location_id: string | null;
  metadata: Record<string, unknown>;
  relationship_type: string;
  chain_id: string | null;
  is_permanent: boolean;
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

export interface InvestigationEvent {
  event: string;
  data: Record<string, unknown>;
}
```

- [ ] **Step 2: Create API client**

Create `london-cortex/frontend/src/lib/api.ts`:

```typescript
const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export async function fetchAPI<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export function sseURL(path: string): string {
  return `${API_BASE}${path}`;
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
```

- [ ] **Step 3: Create constants**

Create `london-cortex/frontend/src/lib/constants.ts`:

```typescript
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
```

- [ ] **Step 4: Verify TypeScript compilation**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npx tsc --noEmit
```

Expected: No type errors.

- [ ] **Step 5: Commit**

```bash
git add london-cortex/frontend/src/lib/
git commit -m "feat: add TypeScript types and API client for backend endpoints"
```

---

### Task 3: Custom Hooks (SSE + SWR)

**Files:**
- Create: `frontend/src/hooks/useAPI.ts`
- Create: `frontend/src/hooks/useSSE.ts`
- Create: `frontend/src/hooks/useInvestigate.ts`

- [ ] **Step 1: Create SWR-based useAPI hook**

Create `london-cortex/frontend/src/hooks/useAPI.ts`:

```typescript
"use client";

import useSWR from "swr";

async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export function useAPI<T>(path: string | null, interval: number = 5000) {
  return useSWR<T>(path, fetcher, {
    refreshInterval: interval,
    revalidateOnFocus: false,
    dedupingInterval: 1000,
  });
}
```

- [ ] **Step 2: Create SSE hook for log streaming**

Create `london-cortex/frontend/src/hooks/useSSE.ts`:

```typescript
"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { LogEntry } from "@/lib/types";

export function useSSE(url: string) {
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const bufferRef = useRef<LogEntry[]>([]);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const es = new EventSource(url);
    esRef.current = es;

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const entry: LogEntry = data.data || data;
        bufferRef.current = [...bufferRef.current.slice(-499), entry];
        setEntries([...bufferRef.current]);
      } catch {
        // ignore malformed messages
      }
    };

    es.onerror = () => {
      es.close();
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, [url]);

  const clear = useCallback(() => {
    bufferRef.current = [];
    setEntries([]);
  }, []);

  return { entries, clear };
}
```

- [ ] **Step 3: Create investigation SSE hook**

Create `london-cortex/frontend/src/hooks/useInvestigate.ts`:

```typescript
"use client";

import { useCallback, useRef, useState } from "react";

export interface InvestigationResult {
  events: { event: string; data: Record<string, unknown> }[];
  threadId: string | null;
  loading: boolean;
  error: string | null;
}

export function useInvestigate() {
  const [result, setResult] = useState<InvestigationResult>({
    events: [],
    threadId: null,
    loading: false,
    error: null,
  });
  const abortRef = useRef<AbortController | null>(null);

  const investigate = useCallback(async (question: string, threadId?: string) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setResult({ events: [], threadId: null, loading: true, error: null });

    try {
      const params = new URLSearchParams({ q: question });
      if (threadId) params.set("thread_id", threadId);
      const url = `/api/investigate?${params}`;

      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok || !res.body) throw new Error(`Investigation failed: ${res.status}`);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      const newEvents: InvestigationResult["events"] = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        let currentEvent = "";
        for (const line of lines) {
          if (line.startsWith("event: ")) {
            currentEvent = line.slice(7).trim();
          } else if (line.startsWith("data: ") && currentEvent) {
            try {
              const data = JSON.parse(line.slice(6));
              newEvents.push({ event: currentEvent, data });
              setResult((prev) => ({
                ...prev,
                events: [...newEvents],
              }));
            } catch {
              // skip malformed
            }
            currentEvent = "";
          }
        }
      }

      // Extract thread_id from the last done event
      const doneEvent = newEvents.find((e) => e.event === "done");
      setResult({
        events: newEvents,
        threadId: (doneEvent?.data?.thread_id as string) || null,
        loading: false,
        error: null,
      });
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        setResult((prev) => ({
          ...prev,
          loading: false,
          error: (err as Error).message,
        }));
      }
    }
  }, []);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    setResult((prev) => ({ ...prev, loading: false }));
  }, []);

  return { result, investigate, cancel };
}
```

- [ ] **Step 4: Verify TypeScript compilation**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npx tsc --noEmit
```

Expected: No type errors.

- [ ] **Step 5: Commit**

```bash
git add london-cortex/frontend/src/hooks/
git commit -m "feat: add custom hooks for SSE streaming, SWR data fetching, and investigations"
```

---

### Task 4: Layout Components (Sidebar + Header)

**Files:**
- Create: `frontend/src/components/layout/Sidebar.tsx`
- Create: `frontend/src/components/layout/Header.tsx`
- Update: `frontend/src/app/layout.tsx`
- Update: `frontend/src/app/page.tsx`

- [ ] **Step 1: Create Sidebar component**

Create `london-cortex/frontend/src/components/layout/Sidebar.tsx`:

```tsx
"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  { href: "/", label: "Dashboard", icon: "◈" },
  { href: "/investigate", label: "Investigate", icon: "◎" },
  { href: "/channels", label: "Channels", icon: "◫" },
  { href: "/cameras", label: "Cameras", icon: "◧" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-16 bg-[var(--bg-secondary)] border-r border-[var(--border)] flex flex-col items-center py-4 gap-2">
      <div className="mb-6 text-xl font-bold text-[var(--accent)]">LC</div>
      {NAV_ITEMS.map((item) => (
        <Link
          key={item.href}
          href={item.href}
          className={`flex flex-col items-center justify-center w-12 h-12 rounded-lg text-xs transition-colors ${
            pathname === item.href
              ? "bg-[var(--accent)] text-white"
              : "text-[var(--text-secondary)] hover:bg-[var(--bg-card)] hover:text-[var(--text-primary)]"
          }`}
          title={item.label}
        >
          <span className="text-lg">{item.icon}</span>
          <span className="mt-0.5 text-[10px]">{item.label}</span>
        </Link>
      ))}
    </aside>
  );
}
```

- [ ] **Step 2: Create Header component**

Create `london-cortex/frontend/src/components/layout/Header.tsx`:

```tsx
"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Stats } from "@/lib/types";

export function Header() {
  const { data: stats } = useAPI<Stats>("/api/stats", 10000);

  return (
    <header className="h-12 bg-[var(--bg-secondary)] border-b border-[var(--border)] flex items-center px-6 justify-between">
      <h1 className="text-lg font-semibold">London Cortex</h1>
      <div className="flex gap-6 text-sm text-[var(--text-secondary)]">
        <span title="Observations">
          ◈ {stats?.observations?.toLocaleString() ?? "—"}
        </span>
        <span title="Anomalies" className="text-[var(--warning)]">
          △ {stats?.anomalies?.toLocaleString() ?? "—"}
        </span>
        <span title="Connections">
          ⟷ {stats?.connections?.toLocaleString() ?? "—"}
        </span>
        <span title="Messages">
          ◫ {stats?.messages?.toLocaleString() ?? "—"}
        </span>
      </div>
    </header>
  );
}
```

- [ ] **Step 3: Update root layout to include sidebar and header**

Update `london-cortex/frontend/src/app/layout.tsx`:

```tsx
import "@/styles/globals.css";
import { Sidebar } from "@/components/layout/Sidebar";
import { Header } from "@/components/layout/Header";

export const metadata = {
  title: "London Cortex",
  description: "Autonomous city intelligence dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link
          rel="stylesheet"
          href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
          integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
          crossOrigin=""
        />
      </head>
      <body className="bg-[var(--bg-primary)] text-[var(--text-primary)]">
        <Sidebar />
        <div className="ml-16 flex flex-col h-screen">
          <Header />
          <main className="flex-1 overflow-hidden">{children}</main>
        </div>
      </body>
    </html>
  );
}
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm run build
```

Expected: Build succeeds.

- [ ] **Step 5: Commit**

```bash
git add london-cortex/frontend/src/
git commit -m "feat: add sidebar navigation and header with live stats"
```

---

### Task 5: Dashboard Home Page (Map + Logs + Stats)

**Files:**
- Create: `frontend/src/components/map/LiveMap.tsx`
- Create: `frontend/src/components/map/MapLegend.tsx`
- Create: `frontend/src/components/log-stream/LogStream.tsx`
- Create: `frontend/src/components/log-stream/LogEntry.tsx`
- Create: `frontend/src/components/stats/StatsBar.tsx`
- Update: `frontend/src/app/page.tsx`

- [ ] **Step 1: Create LiveMap component**

Create `london-cortex/frontend/src/components/map/LiveMap.tsx`:

```tsx
"use client";

import { useEffect, useRef } from "react";
import L from "leaflet";
import { useAPI } from "@/hooks/useAPI";
import { LONDON_CENTER, LONDON_ZOOM, SEVERITY_COLORS } from "@/lib/constants";
import type { MapData } from "@/lib/types";

export function LiveMap() {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<L.Map | null>(null);
  const markersRef = useRef<L.LayerGroup>(L.layerGroup());
  const { data } = useAPI<MapData>("/api/map_data", 5000);

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;

    const map = L.map(mapRef.current, {
      center: LONDON_CENTER,
      zoom: LONDON_ZOOM,
      zoomControl: false,
      attributionControl: false,
    });

    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
    }).addTo(map);

    L.control.zoom({ position: "topright" }).addTo(map);
    markersRef.current.addTo(map);
    mapInstanceRef.current = map;

    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!data || !mapInstanceRef.current) return;

    markersRef.current.clearLayers();

    // Anomaly markers
    for (const a of data.anomalies) {
      if (!a.lat || !a.lon) continue;
      const color = SEVERITY_COLORS[a.severity] || "#3b82f6";
      L.circleMarker([a.lat, a.lon], {
        radius: 4 + a.severity * 2,
        fillColor: color,
        color: color,
        weight: 1,
        opacity: 0.8,
        fillOpacity: 0.6,
      })
        .bindPopup(`<strong>${a.source}</strong><br/>${a.description}<br/>z=${a.z_score.toFixed(1)}`)
        .addTo(markersRef.current);
    }

    // Observation markers (clustered by color per zone)
    for (const o of data.observations) {
      const zoneColor = o.zone === 2 ? "#f59e0b" : o.zone === 1 ? "#6366f1" : "#334155";
      L.circleMarker([o.lat, o.lon], {
        radius: 2,
        fillColor: zoneColor,
        color: zoneColor,
        weight: 0,
        fillOpacity: 0.4,
      }).addTo(markersRef.current);
    }

    // Discovery markers
    for (const d of data.discoveries) {
      L.circleMarker([d.lat, d.lon], {
        radius: 8,
        fillColor: "#22c55e",
        color: "#fff",
        weight: 2,
        fillOpacity: 0.7,
      })
        .bindPopup(`<strong>${d.from_agent}</strong><br/>${d.content.slice(0, 200)}`)
        .addTo(markersRef.current);
    }
  }, [data]);

  return (
    <div ref={mapRef} className="h-full w-full" />
  );
}
```

- [ ] **Step 2: Create MapLegend component**

Create `london-cortex/frontend/src/components/map/MapLegend.tsx`:

```tsx
export function MapLegend() {
  return (
    <div className="absolute bottom-4 left-4 z-[1000] bg-[var(--bg-card)] border border-[var(--border)] rounded-lg p-3 text-xs">
      <div className="font-semibold mb-1">Legend</div>
      <div className="space-y-1">
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#22c55e] inline-block" /> Discovery</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#ef4444] inline-block" /> High Severity</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#f59e0b] inline-block" /> Medium Severity</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#3b82f6] inline-block" /> Low Severity</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#6366f1] inline-block" /> Perifovea</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#334155] inline-block" /> Peripheral</div>
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Create LogEntry component**

Create `london-cortex/frontend/src/components/log-stream/LogEntry.tsx`:

```tsx
import { LEVEL_COLORS } from "@/lib/constants";
import type { LogEntry as LogEntryType } from "@/lib/types";

export function LogEntry({ entry }: { entry: LogEntryType }) {
  const color = LEVEL_COLORS[entry.level] || "#888";

  return (
    <div className="flex gap-2 px-2 py-0.5 hover:bg-[var(--bg-card)] text-xs font-mono">
      <span className="text-[var(--text-secondary)] shrink-0 w-16">{entry.time}</span>
      <span className="shrink-0 w-14 font-semibold" style={{ color }}>
        {entry.level}
      </span>
      <span className="text-[var(--accent)] shrink-0 w-32 truncate">{entry.name}</span>
      <span className="truncate">{entry.msg}</span>
    </div>
  );
}
```

- [ ] **Step 4: Create LogStream component with virtualization**

Create `london-cortex/frontend/src/components/log-stream/LogStream.tsx`:

```tsx
"use client";

import { useRef, useCallback, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useSSE } from "@/hooks/useSSE";
import { LogEntry } from "./LogEntry";

export function LogStream() {
  const { entries, clear } = useSSE("/stream");
  const parentRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [levelFilter, setLevelFilter] = useState<string>("ALL");

  const filtered = levelFilter === "ALL"
    ? entries
    : entries.filter((e) => e.level === levelFilter);

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 24,
    overscan: 50,
  });

  const handleScroll = useCallback(() => {
    if (!parentRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = parentRef.current;
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 50);
  }, []);

  // Auto-scroll to bottom
  if (autoScroll && filtered.length > 0) {
    requestAnimationFrame(() => {
      virtualizer.scrollToIndex(filtered.length - 1, { align: "end" });
    });
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-2 py-1 border-b border-[var(--border)]">
        <div className="flex gap-1">
          {["ALL", "INFO", "WARNING", "ERROR"].map((level) => (
            <button
              key={level}
              onClick={() => setLevelFilter(level)}
              className={`px-2 py-0.5 text-xs rounded ${
                levelFilter === level
                  ? "bg-[var(--accent)] text-white"
                  : "bg-[var(--bg-card)] text-[var(--text-secondary)]"
              }`}
            >
              {level}
            </button>
          ))}
        </div>
        <div className="flex gap-2 text-xs text-[var(--text-secondary)]">
          <span>{filtered.length} lines</span>
          <button onClick={clear} className="hover:text-[var(--text-primary)]">Clear</button>
        </div>
      </div>
      <div ref={parentRef} onScroll={handleScroll} className="flex-1 overflow-auto">
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {virtualizer.getVirtualItems().map((item) => (
            <LogEntry key={item.key} entry={filtered[item.index]} />
          ))}
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 5: Create StatsBar component**

Create `london-cortex/frontend/src/components/stats/StatsBar.tsx`:

```tsx
"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Anomaly } from "@/lib/types";

export function StatsBar() {
  const { data: anomalies } = useAPI<Anomaly[]>("/api/anomalies", 3000);

  const bySeverity = anomalies?.reduce(
    (acc, a) => {
      acc[a.severity] = (acc[a.severity] || 0) + 1;
      return acc;
    },
    {} as Record<number, number>
  );

  return (
    <div className="flex gap-3 p-2 border-b border-[var(--border)]">
      {anomalies && (
        <div className="flex items-center gap-2 text-xs">
          <span className="text-[var(--text-secondary)]">Active Anomalies:</span>
          <span className="text-[var(--danger)] font-semibold">{anomalies.length}</span>
          {[5, 4, 3, 2, 1].map((s) => (
            <span key={s} className="text-[var(--text-secondary)]">
              S{s}: <span className="font-medium text-[var(--text-primary)]">{bySeverity?.[s] || 0}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 6: Wire up the dashboard home page**

Update `london-cortex/frontend/src/app/page.tsx`:

```tsx
import dynamic from "next/dynamic";
import { StatsBar } from "@/components/stats/StatsBar";
import { LogStream } from "@/components/log-stream/LogStream";
import { MapLegend } from "@/components/map/MapLegend";

const LiveMap = dynamic(
  () => import("@/components/map/LiveMap").then((mod) => ({ default: mod.LiveMap })),
  { ssr: false }
);

export default function DashboardPage() {
  return (
    <div className="flex flex-col h-full">
      <StatsBar />
      <div className="flex flex-1 overflow-hidden">
        <div className="flex-1 relative">
          <LiveMap />
          <MapLegend />
        </div>
        <div className="w-96 border-l border-[var(--border)] flex flex-col">
          <LogStream />
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 7: Verify build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm run build
```

Expected: Build succeeds.

- [ ] **Step 8: Commit**

```bash
git add london-cortex/frontend/src/
git commit -m "feat: add dashboard home with live map, log stream, and stats bar"
```

---

### Task 6: Investigation Page

**Files:**
- Create: `frontend/src/components/investigate/InvestigatePanel.tsx`
- Create: `frontend/src/components/investigate/EvidenceCard.tsx`
- Create: `frontend/src/app/investigate/page.tsx`

- [ ] **Step 1: Create EvidenceCard component**

Create `london-cortex/frontend/src/components/investigate/EvidenceCard.tsx`:

```tsx
export function EvidenceCard({ data }: { data: Record<string, unknown> }) {
  const source = (data.source as string) || "unknown";
  const content = (data.content as string) || (data.description as string) || "";
  const confidence = data.confidence as number | undefined;

  return (
    <div className="bg-[var(--bg-card)] border border-[var(--border)] rounded-lg p-3 text-sm">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[var(--accent)] font-medium">{source}</span>
        {confidence !== undefined && (
          <span className="text-xs text-[var(--text-secondary)]">
            {Math.round(confidence * 100)}% confidence
          </span>
        )}
      </div>
      <p className="text-[var(--text-secondary)] text-xs line-clamp-3">{content}</p>
    </div>
  );
}
```

- [ ] **Step 2: Create InvestigatePanel component**

Create `london-cortex/frontend/src/components/investigate/InvestigatePanel.tsx`:

```tsx
"use client";

import { useState, useRef, useEffect } from "react";
import { useInvestigate } from "@/hooks/useInvestigate";
import { EvidenceCard } from "./EvidenceCard";

export function InvestigatePanel() {
  const [question, setQuestion] = useState("");
  const { result, investigate, cancel } = useInvestigate();
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [result.events]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!question.trim()) return;
    investigate(question.trim(), result.threadId || undefined);
    setQuestion("");
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-auto p-4 space-y-4">
        {result.events.length === 0 && !result.loading && (
          <div className="text-center text-[var(--text-secondary)] mt-20">
            <p className="text-lg">Ask London Cortex a question</p>
            <p className="text-sm mt-1">
              e.g. &quot;What is happening near Canary Wharf?&quot;
            </p>
          </div>
        )}

        {result.events.map((evt, i) => {
          if (evt.event === "evidence") {
            return <EvidenceCard key={i} data={evt.data} />;
          }
          if (evt.event === "reasoning" || evt.event === "answer") {
            return (
              <div key={i} className="bg-[var(--bg-card)] rounded-lg p-3 text-sm">
                <div className="text-[var(--accent)] text-xs font-semibold mb-1 uppercase">
                  {evt.event}
                </div>
                <div className="whitespace-pre-wrap">
                  {(evt.data.text as string) || (evt.data.content as string) || JSON.stringify(evt.data)}
                </div>
              </div>
            );
          }
          return null;
        })}

        {result.loading && (
          <div className="text-center text-[var(--text-secondary)] animate-pulse">
            Investigating...
          </div>
        )}

        {result.error && (
          <div className="text-[var(--danger)] text-sm">{result.error}</div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <form onSubmit={handleSubmit} className="border-t border-[var(--border)] p-4 flex gap-2">
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder={result.threadId ? "Follow up..." : "Ask a question..."}
          className="flex-1 bg-[var(--bg-card)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-[var(--accent)]"
          disabled={result.loading}
        />
        {result.loading ? (
          <button
            type="button"
            onClick={cancel}
            className="px-4 py-2 bg-[var(--danger)] text-white rounded-lg text-sm"
          >
            Stop
          </button>
        ) : (
          <button
            type="submit"
            className="px-4 py-2 bg-[var(--accent)] text-white rounded-lg text-sm hover:bg-[var(--accent-hover)]"
            disabled={!question.trim()}
          >
            Ask
          </button>
        )}
      </form>
    </div>
  );
}
```

- [ ] **Step 3: Create investigate page**

Create `london-cortex/frontend/src/app/investigate/page.tsx`:

```tsx
import { InvestigatePanel } from "@/components/investigate/InvestigatePanel";

export default function InvestigatePage() {
  return (
    <div className="h-full max-w-3xl mx-auto">
      <InvestigatePanel />
    </div>
  );
}
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm run build
```

Expected: Build succeeds.

- [ ] **Step 5: Commit**

```bash
git add london-cortex/frontend/src/
git commit -m "feat: add investigation page with SSE-powered chat interface"
```

---

### Task 7: Channels Page

**Files:**
- Create: `frontend/src/components/channels/ChannelView.tsx`
- Create: `frontend/src/components/channels/ChannelMessage.tsx`
- Create: `frontend/src/app/channels/page.tsx`

- [ ] **Step 1: Create ChannelMessage component**

Create `london-cortex/frontend/src/components/channels/ChannelMessage.tsx`:

```tsx
import type { AgentMessage } from "@/lib/types";

const AGENT_COLORS: Record<string, string> = {
  brain: "#6366f1",
  chronicler: "#8b5cf6",
  daemon: "#ef4444",
  validator: "#22c55e",
  curiosity: "#f59e0b",
  discovery: "#3b82f6",
};

export function ChannelMessage({ msg }: { msg: AgentMessage }) {
  const color = AGENT_COLORS[msg.from_agent] || "#888";

  return (
    <div className="flex gap-2 px-3 py-2 hover:bg-[var(--bg-card)] border-b border-[var(--border)]">
      <div
        className="shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold"
        style={{ backgroundColor: color + "30", color }}
      >
        {msg.from_agent.slice(0, 2).toUpperCase()}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-semibold text-sm" style={{ color }}>
            {msg.from_agent}
          </span>
          <span className="text-xs text-[var(--text-secondary)]">
            {new Date(msg.timestamp).toLocaleTimeString()}
          </span>
        </div>
        <p className="text-sm text-[var(--text-primary)] mt-0.5 break-words">
          {msg.content}
        </p>
        {msg.data?.type && (
          <span className="inline-block mt-1 px-1.5 py-0.5 text-[10px] bg-[var(--bg-card)] rounded text-[var(--text-secondary)]">
            {msg.data.type as string}
          </span>
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Create ChannelView component**

Create `london-cortex/frontend/src/components/channels/ChannelView.tsx`:

```tsx
"use client";

import { useState } from "react";
import { useAPI } from "@/hooks/useAPI";
import { CHANNEL_LIST } from "@/lib/constants";
import { ChannelMessage } from "./ChannelMessage";
import type { ChannelData, AgentMessage } from "@/lib/types";

export function ChannelView() {
  const [active, setActive] = useState<string>(CHANNEL_LIST[0]);
  const { data } = useAPI<ChannelData>("/api/channels", 5000);

  const messages = (data?.[active] || []) as AgentMessage[];

  return (
    <div className="flex h-full">
      <div className="w-48 border-r border-[var(--border)] flex flex-col">
        {CHANNEL_LIST.map((ch) => {
          const count = (data?.[ch] || []).length;
          return (
            <button
              key={ch}
              onClick={() => setActive(ch)}
              className={`px-3 py-2 text-sm text-left flex justify-between items-center ${
                active === ch
                  ? "bg-[var(--accent)] text-white"
                  : "hover:bg-[var(--bg-card)] text-[var(--text-secondary)]"
              }`}
            >
              <span>{ch}</span>
              {count > 0 && (
                <span className="text-xs opacity-70">{count}</span>
              )}
            </button>
          );
        })}
      </div>
      <div className="flex-1 overflow-auto">
        {messages.length === 0 ? (
          <div className="text-center text-[var(--text-secondary)] mt-20">
            No messages in {active}
          </div>
        ) : (
          messages.map((msg) => <ChannelMessage key={msg.id} msg={msg} />)
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Create channels page**

Create `london-cortex/frontend/src/app/channels/page.tsx`:

```tsx
import { ChannelView } from "@/components/channels/ChannelView";

export default function ChannelsPage() {
  return (
    <div className="h-full">
      <ChannelView />
    </div>
  );
}
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm run build
```

Expected: Build succeeds.

- [ ] **Step 5: Commit**

```bash
git add london-cortex/frontend/src/
git commit -m "feat: add channels page with Slack-style agent conversation view"
```

---

### Task 8: Cameras Page

**Files:**
- Create: `frontend/src/components/cameras/CameraGrid.tsx`
- Create: `frontend/src/components/cameras/CameraLightbox.tsx`
- Create: `frontend/src/app/cameras/page.tsx`

- [ ] **Step 1: Create CameraGrid component**

Create `london-cortex/frontend/src/components/cameras/CameraGrid.tsx`:

```tsx
"use client";

import { useState } from "react";
import { useAPI } from "@/hooks/useAPI";
import { CameraLightbox } from "./CameraLightbox";
import type { ImageInfo } from "@/lib/types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export function CameraGrid() {
  const { data: images } = useAPI<ImageInfo[]>("/api/images", 30000);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div className="h-full overflow-auto p-4">
      <div className="grid grid-cols-4 lg:grid-cols-6 gap-3">
        {images?.map((img) => (
          <button
            key={img.name}
            onClick={() => setSelected(img.name)}
            className="aspect-video bg-[var(--bg-card)] border border-[var(--border)] rounded-lg overflow-hidden hover:border-[var(--accent)] transition-colors"
          >
            <img
              src={`${API_BASE}/images/thumbs/${img.name}`}
              alt={img.name}
              className="w-full h-full object-cover"
              loading="lazy"
            />
          </button>
        ))}
        {(!images || images.length === 0) && (
          <div className="col-span-full text-center text-[var(--text-secondary)] py-20">
            No camera images available
          </div>
        )}
      </div>

      {selected && (
        <CameraLightbox
          name={selected}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
```

- [ ] **Step 2: Create CameraLightbox component**

Create `london-cortex/frontend/src/components/cameras/CameraLightbox.tsx`:

```tsx
const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export function CameraLightbox({ name, onClose }: { name: string; onClose: () => void }) {
  return (
    <div
      className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="max-w-4xl max-h-[80vh] relative" onClick={(e) => e.stopPropagation()}>
        <button
          onClick={onClose}
          className="absolute -top-3 -right-3 w-8 h-8 bg-[var(--bg-card)] border border-[var(--border)] rounded-full flex items-center justify-center text-[var(--text-secondary)] hover:text-white z-10"
        >
          x
        </button>
        <img
          src={`${API_BASE}/images/full/${name}`}
          alt={name}
          className="max-w-full max-h-[80vh] rounded-lg"
        />
        <div className="text-center text-xs text-[var(--text-secondary)] mt-2">{name}</div>
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Create cameras page**

Create `london-cortex/frontend/src/app/cameras/page.tsx`:

```tsx
import { CameraGrid } from "@/components/cameras/CameraGrid";

export default function CamerasPage() {
  return (
    <div className="h-full">
      <CameraGrid />
    </div>
  );
}
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm run build
```

Expected: Build succeeds.

- [ ] **Step 5: Commit**

```bash
git add london-cortex/frontend/src/
git commit -m "feat: add cameras page with thumbnail grid and lightbox viewer"
```

---

### Task 9: Add SSE Endpoint to Python Backend

**Files:**
- Modify: `london-cortex/core/dashboard.py`

This task adds an SSE `/stream` endpoint to the Python backend so the Next.js frontend can use EventSource for real-time logs instead of WebSocket (which requires a client library).

- [ ] **Step 1: Add SSE stream endpoint to DashboardServer**

Add the following method to the `DashboardServer` class in `london-cortex/core/dashboard.py`, after the `_api_retina` method (after line 514):

```python
    async def _api_stream(self, request: web.Request) -> web.Response:
        """SSE endpoint for real-time log streaming."""
        response = web.StreamResponse()
        response.content_type = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        response.headers["Access-Control-Allow-Origin"] = "*"
        await response.prepare(request)

        # Create a queue for this client
        import asyncio
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=200)

        # Custom handler that pushes to this client's queue
        class SSEClient:
            def __init__(self, q: asyncio.Queue):
                self._queue = q

        client = SSEClient(queue)

        # Register with the log handler
        original_clients = self.log_handler._clients.copy()
        # We'll use a simple approach: poll the buffer
        last_idx = len(self.log_handler.buffer)

        try:
            while True:
                # Check for new entries
                current = list(self.log_handler.buffer)
                if len(current) > last_idx:
                    for entry in current[last_idx:]:
                        try:
                            data = json.dumps(entry, default=str)
                            await response.write(f"data: {data}\n\n".encode())
                        except Exception:
                            break
                    last_idx = len(current)
                elif len(current) < last_idx:
                    # Buffer was trimmed
                    last_idx = len(current)
                await asyncio.sleep(0.1)
        except (ConnectionError, asyncio.CancelledError):
            pass
        return response
```

- [ ] **Step 2: Register the SSE route in the start method**

In the `start` method of `DashboardServer`, add the route registration. Find the line that registers `/api/retina` (around line 141):

```python
        app.router.add_get("/api/retina", self._api_retina)
```

Add after it:

```python
        app.router.add_get("/stream", self._api_stream)
```

- [ ] **Step 3: Update CORS to allow all origins for development**

In the `cors_middleware` function (line 33), change the origin from `"http://localhost:3000"` to `"*"`:

```python
    response.headers["Access-Control-Allow-Origin"] = "*"
```

Also update the CORS preflight handler (line 186) similarly.

- [ ] **Step 4: Verify the Python backend still starts**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex
python -c "from cortex.core.dashboard import DashboardServer; print('OK')"
```

Expected: Prints "OK".

- [ ] **Step 5: Commit**

```bash
git add london-cortex/core/dashboard.py
git commit -m "feat: add SSE /stream endpoint for real-time log streaming"
```

---

### Task 10: Docker Configuration

**Files:**
- Create: `london-cortex/Dockerfile.backend`
- Create: `london-cortex/frontend/Dockerfile`
- Create: `london-cortex/docker-compose.yml`
- Create: `london-cortex/.dockerignore`

- [ ] **Step 1: Create backend Dockerfile**

Create `london-cortex/Dockerfile.backend`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system deps for rasterio, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libgdal-dev gdal-bin && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "-m", "cortex"]
```

- [ ] **Step 2: Create frontend Dockerfile**

Create `london-cortex/frontend/Dockerfile`:

```dockerfile
FROM node:20-slim AS builder

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build

FROM node:20-slim
WORKDIR /app
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
COPY --from=builder /app/public ./public 2>/dev/null || true

EXPOSE 3000

CMD ["npm", "start"]
```

- [ ] **Step 3: Create docker-compose.yml**

Create `london-cortex/docker-compose.yml`:

```yaml
services:
  backend:
    build:
      context: .
      dockerfile: Dockerfile.backend
    ports:
      - "8000:8000"
    env_file:
      - .env
    volumes:
      - ./data:/app/data
    restart: unless-stopped

  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      - NEXT_PUBLIC_API_URL=http://backend:8000
    depends_on:
      - backend
    restart: unless-stopped
```

- [ ] **Step 4: Create .dockerignore**

Create `london-cortex/.dockerignore`:

```
.venv/
__pycache__/
*.pyc
data/
.env
.git
node_modules/
.next/
frontend/
```

- [ ] **Step 5: Commit**

```bash
git add london-cortex/Dockerfile.backend london-cortex/docker-compose.yml london-cortex/.dockerignore london-cortex/frontend/Dockerfile
git commit -m "feat: add Docker Compose configuration for backend and frontend"
```

---

### Task 11: Final Verification and Integration Test

- [ ] **Step 1: Full frontend build**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npm run build
```

Expected: Build succeeds with all pages generated.

- [ ] **Step 2: TypeScript check**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex/frontend
npx tsc --noEmit
```

Expected: No type errors.

- [ ] **Step 3: Verify Python backend imports cleanly**

```bash
cd /Users/Rohithn/AI/experiments/watchahack/london-cortex
python -c "from cortex.core.dashboard import DashboardServer; from cortex.core.board import MessageBoard; print('Backend imports OK')"
```

Expected: "Backend imports OK".

- [ ] **Step 4: Final commit with any remaining fixes**

```bash
git add -A
git commit -m "chore: final cleanup and verification"
```
