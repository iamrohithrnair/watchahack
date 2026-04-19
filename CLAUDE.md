# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Commits
- Never add `Co-Authored-By` trailers to any commits in this repo.

## Commands

### Backend (Python)
```bash
# Install dependencies (from repo root)
uv pip install -r london-cortex/requirements.txt

# Run the system (from repo root)
.venv/bin/python -m cortex

# Run with auto-restart on crash
cd london-cortex && bash run_loop.sh
```

Backend API: http://localhost:8000  
Frontend (served separately): http://localhost:3000

### Frontend (Next.js)
```bash
cd london-cortex/frontend

npm install        # install deps
npm run dev        # dev server (port 3000)
npm run build      # production build
npm run lint       # ESLint
```

## Architecture

### Overview
London Cortex is an autonomous city intelligence system. The Python backend continuously ingests 63+ data sources, analyzes them through multiple agent layers, and serves a REST/WebSocket API on port 8000. A Next.js frontend on port 3000 consumes that API.

`cortex/` at the repo root is a symlink to `london-cortex/`. Run `python -m cortex` from the repo root; it resolves to `london-cortex/__main__.py`.

### Backend Data Flow
```
Ingestors (63+) → Retina (foveal attention) → Intelligence Layer → SQLite + NetworkX
```

1. **Ingestors** (`london-cortex/ingestors/`) — one file per data source, registered declaratively in `registry.py`. Missing API keys cause silent skip at startup.
2. **Retina** (`core/retina.py`) — divides London's ~4,000 grid cells into fovea/perifovea/periphery zones. Peripheral anomalies trigger saccades that snap attention to affected areas.
3. **Intelligence Layer** (`agents/`):
   - `brain.py` — LLM synthesis engine (main orchestrator)
   - `interpreters.py` — domain-specific signal interpreters (vision, numeric, text, financial)
   - `connectors.py` — cross-domain connectors (spatial, narrative, statistical, causal)
   - `validator.py` — backtests predictions against real outcomes
   - `curiosity.py` / `discovery.py` — self-directed investigation and cross-domain exploration
   - `chronicler.py` — multi-day narrative tracking
   - `investigator.py` — serves interactive investigation queries from the frontend
4. **MessageBoard** (`core/board.py`) — SQLite hub (WAL mode, busy timeout 10s) used for all agent communication. 7 tables covering observations, anomalies, connections, conversations, messages, predictions, and discoveries.
5. **NetworkX spatial graph** (`core/graph.py`) — 500m grid over London's bounding box (51.28–51.70°N, -0.51–0.33°E).
6. **EMA baselines** (`core/memory.py`) — exponential moving averages per metric, persisted as JSON in `data/memory/`.

### LLM Backend
Configured via env vars or the dashboard Settings panel:
- `gemini` (default): `GEMINI_API_KEY` — uses Gemini 2.0 Flash and Gemini 2.5 Pro
- `glm`: `GLM_API_KEY` + `LLM_PROVIDER=glm` — uses GLM 4 Flash / GLM 4 Plus

Rate limits are enforced in `core/scheduler.py` (60 rpm flash, 30 rpm pro).

### Frontend (Next.js App Router)
Located in `london-cortex/frontend/`. Uses:
- **SWR** for data fetching from the backend API
- **React Leaflet** for the interactive map
- **TanStack Virtual** for virtualized log lists
- **Tailwind CSS v4**

Pages: `/` (main dashboard with map + log stream), `/cameras` (TfL JamCam grid), `/channels` (agent conversation channels), `/investigate` (interactive Q&A panel).

The frontend talks exclusively to the backend at `localhost:8000`; CORS is open (`*`) in development.

### Key Config
All thresholds and intervals live in `london-cortex/core/config.py`:
- `ANOMALY_Z_THRESHOLD = 2.0` — sigma threshold for anomaly detection
- `ANOMALY_TTL_HOURS = 6` — anomaly expiry
- `INGESTOR_INTERVALS` dict — per-source polling frequency in seconds
- Data written to `london-cortex/data/` (auto-created): SQLite DB, image cache, memory JSON, logs

Images are auto-cleaned: thumbnails after 48h, full images after 7d.
