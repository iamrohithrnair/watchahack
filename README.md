# London Cortex

An autonomous AI that watches London through 63+ real-time data sources — detecting anomalies, finding hidden connections, and building an evolving understanding of the city.

## Quick Start

### Prerequisites

- **Python 3.11+** (required for modern type syntax)
- **[uv](https://docs.astral.sh/uv/)** (fast Python package manager)
- **One LLM API key** — either Google Gemini or Z.ai GLM 5.1

### Step 1: Clone and Set Up

```bash
git clone https://github.com/iamrohithrnair/watchahack.git
cd watchahack
```

### Step 2: Create Virtual Environment

```bash
uv venv
```

This creates a `.venv/` directory in the project root.

### Step 3: Install Dependencies

```bash
uv pip install -r london-cortex/requirements.txt
```

Key dependencies: aiohttp, aiosqlite, networkx, google-genai, httpx, scipy, numpy, and more.

### Step 4: Configure API Key

Create a `.env` file inside `london-cortex/`:

```bash
# Option A: Google Gemini (recommended)
echo 'GEMINI_API_KEY=your_gemini_key_here' > london-cortex/.env

# Option B: Z.ai GLM 5.1
echo 'GLM_API_KEY=your_glm_key_here' > london-cortex/.env
echo 'LLM_PROVIDER=glm' >> london-cortex/.env
```

You can also set both keys and switch providers via the dashboard Settings panel.

### Step 5: Run

From the `watchahack/` root directory:

```bash
.venv/bin/python -m cortex
```

Or with auto-restart (survives crashes):

```bash
cd london-cortex && bash run_loop.sh
```

### Step 6: Open the Dashboard

- **Dashboard**: http://localhost:3000
- **API**: http://localhost:8000

The dashboard shows live logs, an interactive map, source health, and an investigation panel. Both ports start automatically when you run the system.

## Stopping

- **Ctrl+C once** — graceful shutdown (waits for in-flight tasks)
- **Ctrl+C twice** — force exit immediately

## How It Works

### Architecture

```
                         +------------------+
                         |    63+ APIs &    |
                         |   Live Feeds     |
                         +--------+---------+
                                  |
                                  v
  +---------------------------------------------------------------+
  |                        INGEST LAYER                           |
  |  TfL . LAQN . Sentinel . GDELT . yfinance . Met Office . ... |
  +---------------------------+-----------------------------------+
                              | raw observations
                              v
  +---------------------------------------------------------------+
  |                     RETINA (Attention)                        |
  |                                                               |
  |  Fovea    -- full resolution, every observation analyzed      |
  |  Perifovea -- medium threshold                               |
  |  Periphery -- compressed, anomalies trigger saccades         |
  +---------------------------+-----------------------------------+
                              | filtered + prioritized
                              v
  +---------------------------------------------------------------+
  |                    INTELLIGENCE LAYER                         |
  |                                                               |
  |  Interpreters --> Connectors --> Brain (LLM)                  |
  |  vision, numeric,  spatial,      synthesizes signals,        |
  |  text, financial   narrative,    epistemic grounding,        |
  |                     statistical, generates discoveries       |
  |                     causal                                    |
  +----------+----------------------------+----------+-----------+
             |                            |                     |
             v                            v                     v
  +--------------------+     +--------------------+   +------------------+
  |  Validator         |     |  Curiosity &       |   |  Chronicler      |
  |  backtests         |     |  Discovery         |   |  multi-day       |
  |  predictions       |     |  self-directed     |   |  narratives      |
  +--------------------+     +--------------------+   +------------------+
             |                            |
             +-------------+--------------+
                           |
                           v
  +---------------------------------------------------------------+
  |                      PERSISTENCE                              |
  |  SQLite + NetworkX graph (500m grid) + EMA baselines          |
  +---------------------------------------------------------------+
```

### Key Concepts

- **Foveal attention (Retina)** — London's ~4,000 grid cells are divided into attention zones. Anomalies in the periphery trigger "saccades" that snap full attention to the area.
- **Epistemic integrity** — Confidence decays at each processing stage. Multi-source corroboration is required before anything surfaces as a discovery.
- **Pluggable LLM backend** — Supports Gemini and GLM 5.1. Rate-limited to prevent API errors.
- **Self-healing daemon** — Watches for failures and can spawn diagnostic processes to fix issues.
- **Adaptive scheduling** — Ingestors speed up near anomalies, slow down during quiet periods.

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

Ingestors with missing API keys are silently skipped at startup.

## Dashboard Features

- **Live log stream** — WebSocket-powered real-time feed of all agent activity
- **Interactive map** — Anomaly markers on London's 500m grid with severity levels
- **Source health** — Status indicators for each active data source
- **Channel view** — Agent conversation channels (#discoveries, #hypotheses, #anomalies, #requests, #meta)
- **Image viewer** — TfL JamCam snapshots with lightbox
- **Investigation** — Ask questions about observations and anomalies via the right panel
- **Settings** — Configure LLM provider and API keys from the dashboard

## LLM Backends

| Provider | Env Key | Models |
|----------|---------|--------|
| `gemini` (default) | `GEMINI_API_KEY` | Gemini 2.0 Flash, Gemini 2.5 Pro |
| `glm` | `GLM_API_KEY` | GLM 4 Flash, GLM 4 Plus |

Configure via `.env` file or the dashboard Settings panel.

## Project Structure

```
watchahack/
+-- cortex/ -> london-cortex/     # Symlink
+-- london-cortex/
|   +-- run.py                    # Entry point
|   +-- __main__.py               # python -m cortex support
|   +-- requirements.txt
|   +-- pyproject.toml
|   +-- .env                      # API keys (create this)
|   |
|   +-- core/                     # Infrastructure
|   |   +-- board.py              # SQLite message hub (7 tables)
|   |   +-- config.py             # Intervals, endpoints, LLM config
|   |   +-- coordinator.py        # Adaptive scheduler
|   |   +-- daemon.py             # Self-healing watcher
|   |   +-- dashboard.py          # aiohttp API + frontend server
|   |   +-- epistemics.py         # Confidence decay & grounding
|   |   +-- graph.py              # NetworkX spatial grid (500m)
|   |   +-- image_store.py        # Two-tier image storage
|   |   +-- llm.py                # Pluggable LLM backend
|   |   +-- memory.py             # EMA baselines + JSON persistence
|   |   +-- models.py             # All dataclasses
|   |   +-- retina.py             # Foveal attention zones
|   |   +-- scheduler.py          # Rate limiter
|   |
|   +-- agents/                   # Intelligence layer
|   |   +-- brain.py              # Main synthesis engine
|   |   +-- interpreters.py       # Vision, Numeric, Text, Financial
|   |   +-- connectors.py         # Spatial, Narrative, Statistical, Causal
|   |   +-- validator.py          # Prediction backtesting
|   |   +-- curiosity.py          # Self-directed investigation
|   |   +-- chronicler.py         # Multi-day narrative tracking
|   |   +-- discovery.py          # Cross-domain exploration
|   |   +-- explorers.py          # Hypothesis testing
|   |   +-- web_searcher.py       # Web verification
|   |   +-- investigator.py       # Interactive investigation
|   |
|   +-- ingestors/                # 63+ data source adapters
|   |   +-- registry.py           # Declarative registration table
|   |   +-- *.py                  # One per data source
|   |
|   +-- static/                   # Web dashboard
|   |   +-- dashboard.html        # Line Art Minimalistic UI
|   |   +-- london_grid.json      # 500m grid cell definitions
|   |
|   +-- data/                     # Runtime (auto-created)
|       +-- graph.db              # SQLite database
|       +-- images/               # Image cache
|       +-- memory/               # Memory persistence
|       +-- logs/                 # System logs
```

## Notes

- The SQLite database and image cache grow continuously. Images are auto-cleaned (thumbnails after 48h, full images after 7d).
- Most data source APIs work with free-tier keys. Some may incur costs at scale.
- The system is fully autonomous — it continuously ingests, analyzes, and discovers patterns without manual intervention.

## Built With

Python 3.11+ | asyncio | SQLite (aiosqlite) | NetworkX | Google Gemini / Z.ai GLM 5.1 | Leaflet.js | aiohttp | uv
