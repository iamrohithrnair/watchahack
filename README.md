# London Cortex

**An autonomous AI that watches London through 63+ real-time data sources — detecting anomalies, finding hidden connections, and building an evolving understanding of the city.**

## What It Does

London Cortex is an autonomous AI system that monitors London as a living city in real time. It continuously ingests data from 63+ sources — TfL cameras, air quality sensors, energy grid status, satellite imagery, financial markets, news feeds, and more — and uses a pipeline of specialized AI agents to detect anomalies, find cross-source patterns, and synthesize grounded insights about what's actually happening across the city.

Every insight carries a confidence score that decays through each layer of inference, requiring multi-source corroboration before anything is surfaced as a genuine discovery. This epistemic integrity model prevents the system from hallucinating patterns — if only one source reports something, the confidence stays low. If three independent sources corroborate it, the system escalates.

To handle the volume of city-wide data, it uses a neuroscience-inspired **Retina** system — a foveal attention model that divides London's spatial grid into attention zones (fovea, perifovea, periphery), allocating full processing resolution only to areas showing anomalous activity while compressing quiet zones. Anomalies in the periphery trigger "saccades" that instantly promote those cells to full attention, mirroring how the human eye rapidly shifts focus. This lets the system monitor all of London while concentrating compute where it matters most.

## Architecture

```
                         ┌─────────────────┐
                         │    63+ APIs &    │
                         │   Live Feeds     │
                         └────────┬────────┘
                                  │
                                  ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                        INGEST LAYER                           │
  │  TfL · LAQN · Sentinel · GDELT · yfinance · Met Office · ... │
  └───────────────────────────┬───────────────────────────────────┘
                              │ raw observations
                              ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                     RETINA (Attention)                        │
  │                                                               │
  │  ◉ Fovea ── full resolution, every observation analyzed       │
  │  ◎ Perifovea ── medium threshold                              │
  │  · Periphery ── compressed, but anomalies trigger saccades    │
  │                 that snap foveal attention to the area         │
  └───────────────────────────┬───────────────────────────────────┘
                              │ filtered + prioritized
                              ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                    INTELLIGENCE LAYER                         │
  │                                                               │
  │  Interpreters ──▶ Connectors ──▶ Brain (LLM)                  │
  │  vision, numeric,   spatial,       synthesizes signals,       │
  │  text, financial    narrative,     epistemic grounding,       │
  │                     statistical,   generates discoveries      │
  │                     causal                                    │
  └──────────┬──────────────────────────────────┬─────────────────┘
             │                                  │
             ▼                                  ▼
  ┌─────────────────────┐           ┌─────────────────────┐
  │  Validator           │           │  Curiosity &         │
  │  backtests           │           │  Discovery           │
  │  predictions,        │           │  self-directed       │
  │  tracks reliability  │           │  investigation       │
  └─────────────────────┘           └─────────────────────┘
             │                                  │
             └──────────────┬───────────────────┘
                            ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                      PERSISTENCE                              │
  │  SQLite + NetworkX graph (500m grid) + EMA baselines          │
  └───────────────────────────────────────────────────────────────┘

  ┌─────────────────────┐           ┌─────────────────────┐
  │  Chronicler          │           │  Daemon              │
  │  weaves multi-day    │           │  self-healing,       │
  │  narratives from     │           │  spawns Claude Code  │
  │  accumulated insight │           │  to fix failures     │
  └─────────────────────┘           └─────────────────────┘

  ┌───────────────────────────────────────────────────────────────┐
  │                       DASHBOARD                               │
  │  live log stream · interactive map · source health · images   │
  │           http://localhost:3000   ·   API: localhost:8000     │
  └───────────────────────────────────────────────────────────────┘
```

## Data Sources

| Category | Sources |
|----------|---------|
| **Traffic & Mobility** | TfL JamCam images, traffic speeds, station crowding, bus AVL, cycle hire, road disruptions, National Rail, Waze, TomTom |
| **Air Quality** | LAQN, PurpleAir, Google AQ, OpenAQ, Sensor.Community, openSenseMap |
| **Energy & Grid** | Carbon Intensity, grid generation/demand/frequency, UKPN substations, electricity prices |
| **Weather** | Open-Meteo forecasts, Met Office observations & warnings |
| **Financial** | yfinance stocks, CoinGecko crypto, Polymarket prediction markets |
| **News & Social** | GDELT, Guardian, social sentiment analysis |
| **Satellite** | Sentinel-2 land use change, Sentinel-5P atmospheric composition |
| **Public Services** | Police crime data, NHS syndromic surveillance, Land Registry |
| **Events & Culture** | Eventbrite, V&A Museum, UK Parliament, Companies House |
| **Nature** | iNaturalist wildlife sightings |
| **System Health** | Internal metrics, sensor uptime, provider status, data lineage, APM |

## Dashboard

The web dashboard provides:

- **Live log stream** — WebSocket-powered real-time feed of all agent activity
- **Interactive map** — Leaflet.js with anomaly markers on London's 500m grid
- **Source status cards** — health and freshness of each data source
- **Image viewer** — JamCam snapshots with lightbox and AI annotations
- **Stats bar** — message count, active anomalies, agent status
- **Settings panel** — configure LLM provider (Gemini or GLM 5.1) and API keys

## Getting Started

```bash
# Clone the repo
git clone https://github.com/iamrohithrnair/watchahack.git
cd watchahack

# Create virtual environment and install dependencies
uv venv
uv pip install -r london-cortex/requirements.txt

# Create .env with your API key (only one is required)
echo "GEMINI_API_KEY=your_key_here" > london-cortex/.env

# Run from the watchahack directory
.venv/bin/python -m cortex
```

Or with auto-restart:

```bash
cd london-cortex && bash run_loop.sh
```

The SQLite database is created automatically on first run. The dashboard is served at `http://localhost:3000` and the API at `http://localhost:8000`.

### LLM Backends

London Cortex supports two LLM backends, configured via `LLM_PROVIDER` in `.env`:

| Provider | Key | Models |
|----------|-----|--------|
| `gemini` (default) | `GEMINI_API_KEY` | Gemini 2.0 Flash, Gemini 2.5 Pro |
| `glm` | `GLM_API_KEY` | GLM 4 Flash, GLM 4 Plus |

All other API keys are optional — ingestors with missing keys are silently skipped.

### API Keys

Ingestors requiring specific keys are skipped at startup if the key is not present. See `core/config.py` for the full list of supported environment variables.

## Important Notes

**Stopping the system:**
- `Ctrl+C` once → graceful shutdown (waits for in-flight tasks)
- `Ctrl+C` twice → force exit immediately

**The system is autonomous:**
- The daemon monitors system health and can spawn Claude Code instances to diagnose and fix failing components
- Restarts are rate-limited; rapid crash loops trigger a full stop

**API usage:**
- 60+ ingestors poll APIs at intervals from 2 minutes to daily
- Most free-tier keys are fine, but some paid tiers may incur costs at scale
- Ingestors with missing keys are silently skipped

**Data growth:**
- The SQLite database and image cache grow continuously
- Images are auto-cleaned (thumbnails after 48h, full images after 7d)

## Project Structure

```
london-cortex/
├── run.py                  # Entry point
├── __main__.py             # python -m cortex support
├── requirements.txt
├── pyproject.toml
│
├── core/                   # Infrastructure
│   ├── board.py            # SQLite message hub (7 tables)
│   ├── config.py           # Intervals, endpoints, LLM config
│   ├── coordinator.py      # Adaptive scheduler
│   ├── daemon.py           # Self-healing watcher
│   ├── dashboard.py        # aiohttp API server (port 8000)
│   ├── epistemics.py       # Confidence decay & grounding
│   ├── graph.py            # NetworkX spatial grid (500m, ~4000 cells)
│   ├── image_store.py      # Two-tier image storage
│   ├── llm.py              # Pluggable LLM backend (Gemini + GLM 5.1)
│   ├── memory.py           # EMA baselines + JSON persistence
│   ├── models.py           # All dataclasses
│   ├── retina.py           # Foveal attention zones
│   └── scheduler.py        # Rate limiter
│
├── agents/                 # Intelligence layer
│   ├── base.py             # LLM integration + board helpers
│   ├── brain.py            # Main synthesis engine
│   ├── interpreters.py     # Vision, Numeric, Text, Financial
│   ├── connectors.py       # Spatial, Narrative, Statistical, Causal
│   ├── validator.py        # Prediction backtesting
│   ├── curiosity.py        # Self-directed investigation
│   ├── chronicler.py       # Multi-day narrative tracking
│   ├── discovery.py        # Cross-domain exploration
│   ├── explorers.py        # On-demand hypothesis testing
│   ├── web_searcher.py     # Web search verification
│   └── investigator.py     # Interactive investigation
│
├── ingestors/              # 63+ data source adapters
│   ├── base.py             # HTTP client, circuit breaker
│   ├── registry.py         # Declarative registration table
│   └── *.py                # One per data source
│
└── static/                 # Web dashboard
    ├── dashboard.html      # Light mode Flat 2.0 UI
    └── london_grid.json    # 500m grid cell definitions
```

## Key Design Decisions

- **Foveal attention (Retina)** — The city's ~4,000 grid cells are divided into three zones. When an anomaly fires in a peripheral cell, it triggers a "saccade" that promotes that cell to fovea. Cells decay back to periphery after 30–60 minutes of quiet.
- **Epistemic integrity** — Confidence decays at each processing stage. Multi-source corroboration is required. Single-source anomalies stay low confidence.
- **Pluggable LLM backend** — Supports Gemini and GLM 5.1 out of the box. Rate-limited with token-bucket limiter to prevent 429 errors.
- **Self-healing daemon** — Watches for failing components and spawns Claude Code instances to diagnose and fix issues autonomously.
- **Adaptive scheduling** — Ingestors run on flexible intervals that speed up near anomalies and slow down during quiet periods. Circuit breakers prevent cascade failures.
- **Declarative registry** — All 63 ingestors and 15 agents are registered via compact tables, eliminating boilerplate.

## Built With

Python 3.11+ · asyncio · SQLite (aiosqlite) · NetworkX · Google Gemini / Z.ai GLM 5.1 · Leaflet.js · aiohttp · uv
