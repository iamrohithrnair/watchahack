# London Cortex Next.js Dashboard — Design Spec

## Goal

Convert the london-cortex frontend from a single HTML file served by aiohttp into a production-grade Next.js 15 application. The Python backend (ingestors, agents, board, graph, retina) remains unchanged. The Next.js app consumes the existing API and adds SSE for real-time log streaming.

## Architecture

- **Backend**: Python aiohttp on port 8000 (existing, minimal changes)
- **Frontend**: Next.js 15 App Router on port 3000
- **Orchestration**: Docker Compose with shared network
- **Real-time**: SSE for logs, SWR polling for structured data

## Python Backend Changes (Minimal)

1. Add `/api/stream` SSE endpoint for log streaming (alongside existing `/ws` WebSocket)
2. Update CORS headers to allow Docker network origins
3. Add `/api/system` endpoint returning startup metadata (active ingestors, skipped ingestors, graph stats)
4. Existing API routes stay untouched: `/api/health`, `/api/tasks`, `/api/anomalies`, `/api/observations`, `/api/discoveries`, `/api/stats`, `/api/images`, `/api/cameras`, `/api/map_data`, `/api/channels`, `/api/retina`, `/api/investigate`

## Next.js Frontend

### Project Structure

```
frontend/
├── src/
│   ├── app/
│   │   ├── layout.tsx          # Root layout: providers, sidebar
│   │   ├── page.tsx            # Dashboard: map + log stream + stats
│   │   ├── investigate/
│   │   │   └── page.tsx        # Investigation chat panel
│   │   ├── channels/
│   │   │   └── page.tsx        # Agent conversation channels
│   │   └── cameras/
│   │       └── page.tsx        # Camera grid with lightbox
│   ├── components/
│   │   ├── map/
│   │   │   ├── LiveMap.tsx     # Leaflet map with markers
│   │   │   ├── RetinaOverlay.tsx  # Attention zone visualization
│   │   │   └── MapLegend.tsx
│   │   ├── log-stream/
│   │   │   ├── LogStream.tsx   # Virtualized log viewer
│   │   │   └── LogEntry.tsx
│   │   ├── stats/
│   │   │   ├── StatsBar.tsx    # Top-level counters
│   │   │   └── SourceHealth.tsx
│   │   ├── channels/
│   │   │   ├── ChannelView.tsx # Slack-style thread view
│   │   │   └── ChannelMessage.tsx
│   │   ├── investigate/
│   │   │   ├── InvestigatePanel.tsx  # Chat with SSE
│   │   │   └── EvidenceCard.tsx
│   │   ├── cameras/
│   │   │   ├── CameraGrid.tsx  # Thumbnail grid
│   │   │   └── CameraLightbox.tsx
│   │   └── layout/
│   │       ├── Sidebar.tsx
│   │       └── Header.tsx
│   ├── hooks/
│   │   ├── useSSE.ts           # SSE connection hook
│   │   ├── useAPI.ts           # SWR-based data fetching
│   │   └── useInvestigate.ts   # Investigation SSE stream
│   ├── lib/
│   │   ├── api.ts              # API client with base URL config
│   │   ├── types.ts            # TypeScript interfaces
│   │   └── constants.ts
│   └── styles/
│       └── globals.css         # Tailwind base + custom styles
├── Dockerfile
├── next.config.ts
├── tailwind.config.ts
├── tsconfig.json
├── postcss.config.js
└── package.json
```

### Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Next.js | 15 | Framework (App Router) |
| React | 19 | UI library |
| TypeScript | 5 | Type safety |
| Tailwind CSS | 4 | Styling |
| SWR | 2 | Data fetching + caching |
| react-leaflet | 4 | Map component |
| @tanstack/react-virtual | 3 | Virtualized log list |

### Data Fetching Strategy

| Data | Method | Interval | Endpoint |
|---|---|---|---|
| Logs | SSE | Real-time | `/api/stream` |
| Map data | SWR poll | 5s | `/api/map_data` |
| Anomalies | SWR poll | 3s | `/api/anomalies` |
| Observations | SWR poll | 3s | `/api/observations` |
| Discoveries | SWR poll | 5s | `/api/discoveries` |
| Channels | SWR poll | 5s | `/api/channels` |
| Stats | SWR poll | 10s | `/api/stats` |
| Tasks/Health | SWR poll | 10s | `/api/tasks` |
| Cameras | SWR poll | 30s | `/api/cameras` |
| Retina | SWR poll | 5s | `/api/retina` |
| Images | SWR poll | 30s | `/api/images` |
| Investigate | SSE | On-demand | `/api/investigate` |
| System | SWR poll | 30s | `/api/system` (new) |

### Pages

1. **Dashboard** (`/`): Main view with live map, log stream sidebar, stats bar, and anomaly feed
2. **Investigate** (`/investigate`): Chat-style investigation panel with SSE streaming
3. **Channels** (`/channels`): Agent conversation viewer organized by channel
4. **Cameras** (`/cameras`): Camera thumbnail grid with modal lightbox viewer

### Key Components

**LiveMap**: Leaflet map centered on London showing:
- Anomaly markers (color-coded by severity)
- Observation clusters (density heatmap)
- Retina attention overlay (fovea/perifovea/periphery zones)
- Discovery markers

**LogStream**: Virtualized list showing:
- Timestamp, level, source, message
- Level-based filtering (INFO, WARNING, ERROR)
- Auto-scroll with manual scroll lock
- Search/filter capability

**InvestigatePanel**: Chat interface with:
- Text input for questions
- SSE-streamed responses with event types (evidence, reasoning, answer)
- Thread continuity via thread_id
- Evidence cards showing supporting data

**ChannelView**: Slack-style message list:
- Channel tabs (#discoveries, #hypotheses, #anomalies, #meta, #requests)
- Agent avatars/names
- Timestamps and message content
- Collapsible long messages

**CameraGrid**: Grid of camera thumbnails:
- 24 most recent thumbnails
- Click to open lightbox with full-size image
- Camera health status overlay
- Last updated timestamp

## Docker Compose

```yaml
services:
  backend:
    build:
      context: .
      dockerfile: Dockerfile.backend
    ports:
      - "8000:8000"
    env_file: .env
    volumes:
      - ./data:/app/data

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
```

## Environment Variables

**Python Backend** (`.env`):
- `GEMINI_API_KEY` — Required for LLM agents
- `TFL_APP_KEY`, `TFL_APP_ID` — Optional TfL data
- All other optional API keys remain unchanged

**Next.js Frontend**:
- `NEXT_PUBLIC_API_URL` — Backend URL (default: `http://localhost:8000`)

## Migration Path

1. Create Next.js app in `frontend/` directory
2. Add SSE endpoint to Python backend
3. Build all React components
4. Add Docker configuration
5. Verify all features work against live backend
6. Remove `static/` directory after confirmation

## Out of Scope

- Rewriting the Python backend in TypeScript
- Adding authentication/authorization
- Adding database migrations
- Changing the agent or ingestor architecture
- Mobile-specific layouts (responsive but not mobile-first)
