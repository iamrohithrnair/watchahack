# London Cortex

**Autonomous city intelligence — monitoring London through 106+ real-time data sources with a production-grade Next.js dashboard.**

London Cortex is an autonomous city intelligence platform that watches London in real time, spots unusual changes as they happen, and turns hundreds of fast-moving signals into something a human can actually understand.

If you want the short startup-pitch version: **it feels like a city that can explain itself.**

## Quick Start

### Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)**
- **Node.js 20+**
- **A Gemini API key** (or GLM if you want to switch providers)

### Setup

From the repo root:

```bash
uv venv
source .venv/bin/activate
uv pip install -r london-cortex/requirements.txt

cp london-cortex/.env.example london-cortex/.env
# Edit london-cortex/.env and add GEMINI_API_KEY
```

### Run the backend

```bash
source .venv/bin/activate
python -m cortex
```

Or with auto-restart:

```bash
cd london-cortex && bash run_loop.sh
```

### Run the frontend

In a second terminal:

```bash
cd london-cortex/frontend
npm install
npm run dev
```

- **Backend API:** http://localhost:8000
- **Frontend dashboard:** http://localhost:3000

## What makes it interesting

Most city dashboards show data. London Cortex behaves more like an intelligent operating system.

It is wired to **106 ingestors** today across transport, air quality, weather, energy, news, public services, finance, infrastructure, satellite feeds, and more. Instead of leaving you to manually connect the dots, it continuously ingests, compares, prioritizes, and explains what matters.

In short, London Cortex:

1. **Watches** London through 100+ live data ingestors
2. **Detects** what is genuinely unusual
3. **Focuses** attention where it matters most
4. **Shows** the evidence on a map and through live camera imagery
5. **Explains** what is happening in plain language

That combination is the real hook: **city-scale sensing, AI attention, and human-readable investigation in one product.**

## Demo walkthrough

### 1. The moment you open it, London is already alive

The first thing you see is not a static dashboard. It feels active. Counts are moving, sources are updating, and the live log is streaming what the system is doing right now.

**What is happening in the background:** every ingestor runs on its own cadence, writes observations into the shared SQLite message board, and updates rolling baselines in memory. The platform is not just collecting snapshots — it is building a sense of what "normal" looks like for each source, place, and metric.

### 2. It does not just collect data — it notices when the city behaves strangely

One of the strongest moments in the product is the anomaly view.

In London Cortex, an **anomaly** is not just "something interesting." It means a signal has moved far enough away from its usual pattern to be statistically unusual. Under the hood, the platform computes a z-score against a rolling baseline, and anything roughly **two standard deviations away from normal or more** gets elevated.

On the map, anomalies are color-coded by severity:

1. **Low**
2. **Notice**
3. **Medium**
4. **High**
5. **Critical**

So when you see the map change color, you are not looking at decoration. You are looking at the city telling you where something has genuinely shifted.

**What is happening in the background:** anomalies are stored with timestamps, severity, source, and location. If a source does not provide coordinates directly, London Cortex can fall back to the center of the relevant grid cell so the signal still appears on the map in a meaningful place.

### 3. The map is not passive — it is an attention engine

This is where London Cortex starts to feel like a startup demo with real depth.

The city is divided into a **500m spatial grid**, and the system treats it like a visual field. Quiet areas stay in the background. Hotspots get promoted toward the center of attention.

**What is happening in the background:** the Retina layer splits London into three attention zones:

| Zone | Meaning |
|------|---------|
| **Focus zone** | Full resolution — every observation stored, all agents process |
| **Awareness zone** | Moderate resolution — sampled observations, key agents only |
| **Background** | Minimal monitoring — only anomalies tracked |

When a serious anomaly appears — or when multiple sources agree that something unusual is happening in the same place — the system triggers a **saccade**. In plain English: it snaps attention toward that area, promotes the affected cell into focus, and expands attention to the surrounding neighborhood too.

That is a big part of the wow factor. London Cortex is not trying to look at every square meter with the same intensity all the time. It is dynamically deciding where to pay closer attention, more like an intelligent operating system than a passive dashboard.

### 4. You can open the camera view and instantly ground the signal in reality

If the map tells you *where* something unusual is happening, the camera view helps answer *what it actually looks like*.

Open the **Cameras** view and you get a grid of recent London traffic camera imagery — effectively a CCTV-style live visual layer for the city. It is a strong demo moment because it takes the platform from abstract intelligence to something tangible and immediate.

**What is happening in the background:** JamCam images are ingested, cached into thumbnail and full-image stores, and surfaced in the dashboard as a live grid. The system also tracks camera health over time, classifying feeds as **active**, **suspect**, or **dead** based on staleness and consecutive failures, so the visual layer can be trusted instead of blindly displayed.

### 5. The left side is where the city starts talking to itself

One of the most compelling product details is the channel system: **#discoveries, #hypotheses, #anomalies, #requests, and #meta**.

This makes the platform feel less like a database and more like an active intelligence team. You are not only seeing outputs — you are seeing the internal conversation that leads to those outputs.

**What is happening in the background:** specialist agents write into shared channels through the message board. Interpreters look at raw signals, connectors link events across domains, the Brain synthesizes bigger narratives, the validator checks predictions, and the chronicler tracks longer-running stories. The dashboard exposes that internal coordination in a human-readable way.

### 6. The investigation panel is where it stops being a dashboard and becomes a teammate

This is probably the feature that makes people lean in.

You can ask a natural language question like a real operator would: *What is happening in this part of London? Why are transport and air quality both spiking? Is this related to a public event?*

Then London Cortex investigates for you.

**What is happening in the background:** the Investigator agent plans the query first, searches the platform's own anomalies, observations, channels, and discoveries, then combines that with live web context. It can follow leads, pull related evidence, read article content, and stream findings back into the UI as the answer is forming. So the user experience feels conversational, but the backend behavior is structured, evidence-led, and multi-stage.

### 7. It keeps the human in control without breaking the magic

There is also a practical polish to the experience. You can see system stats, source health, live logs, and settings for the LLM backend in one place.

**What is happening in the background:** the backend exposes real-time API and WebSocket endpoints for health, logs, anomalies, channels, map data, cameras, and investigations. The frontend is effectively riding on top of a continuously running city intelligence engine, not polling a dead dataset.

## Architecture

```
  106+ Data Sources
        │
        ▼
  ┌──────────────────────────────────────┐
  │           INGEST LAYER               │
  │  TfL · LAQN · Sentinel · GDELT ·     │
  │  yfinance · Met Office · BBC · ...   │
  └──────────────┬───────────────────────┘
                 │ raw observations
                 ▼
  ┌──────────────────────────────────────┐
  │      ATTENTION ENGINE (Retina)       │
  │  Dynamic focus over London grid      │
  └──────────────┬───────────────────────┘
                 │ focused observations
                 ▼
  ┌──────────────────────────────────────┐
  │        INTERPRET LAYER               │
  │  Vision · Numeric · Text · Financial │
  └──────────────┬───────────────────────┘
                 │ anomalies
                 ▼
  ┌──────────────────────────────────────┐
  │        CONNECT LAYER                 │
  │  Spatial · Narrative · Statistical   │
  │  Causal chains                       │
  └──────────────┬───────────────────────┘
                 │ hypotheses
                 ▼
  ┌──────────────────────────────────────┐
  │           BRAIN                      │
  │  Synthesis · Discovery · Validation  │
  └──────────────┬───────────────────────┘
                 │ discoveries
                 ▼
  ┌──────────────────────────────────────┐
  │     Next.js Dashboard (:3000)        │
  │  Map · Logs · Investigation · Feed   │
  └──────────────────────────────────────┘
```

## Stack

| Layer | Technology |
|-------|-----------|
| Data sources | 106 ingestors (TfL, LAQN, GDELT, Sentinel, BBC, ...) |
| AI agents | 15 agents (Brain, Interpreters, Connectors, Validator, Chronicler, Investigator, ...) |
| LLM | Google Gemini (3.1 Flash-Lite + 3 Flash) or GLM |
| Backend | Python, aiohttp, SQLite, NetworkX |
| Frontend | Next.js 15, React 19, TypeScript, Tailwind CSS |
| Maps | Leaflet with dark CartoDB tiles |
| Real-time | SSE for logs, SWR polling for data |
| Deployment | Local dev or Docker Compose |

## Frontend pages

| Page | Description |
|------|-------------|
| `/` | Dashboard — live map with anomaly markers, attention overlay, and virtualized log stream |
| `/investigate` | Natural language investigation — ask questions and get streamed answers |
| `/channels` | Agent conversation feed — Slack-style view of #discoveries, #hypotheses, and #anomalies |
| `/cameras` | TfL JamCam thumbnail grid with lightbox viewer |

## LLM backends

| Provider | Env key | Models |
|----------|---------|--------|
| `gemini` (default) | `GEMINI_API_KEY` | Gemini 3.1 Flash-Lite, Gemini 3 Flash |
| `glm` | `GLM_API_KEY` | GLM 4 Flash, GLM 4 Plus |

Configure via `london-cortex/.env` or the dashboard settings panel.

## Project structure

```text
watchahack/
├── cortex/ -> london-cortex/     # Symlink
├── docs/                         # Demo notes and supporting docs
├── london-cortex/
│   ├── run.py                    # Entry point
│   ├── __main__.py               # python -m cortex support
│   ├── core/                     # Infrastructure
│   │   ├── board.py              # SQLite message hub
│   │   ├── config.py             # Intervals, endpoints, LLM config
│   │   ├── coordinator.py        # Adaptive scheduler
│   │   ├── dashboard.py          # aiohttp API server
│   │   ├── graph.py              # 500m spatial grid
│   │   ├── llm.py                # Pluggable LLM backend
│   │   ├── memory.py             # EMA baselines and persistence
│   │   ├── retina.py             # Attention engine
│   │   └── ...
│   ├── agents/                   # Intelligence layer
│   ├── ingestors/                # 106 data source adapters
│   ├── frontend/                 # Next.js app
│   └── data/                     # Runtime data
└── README.md
```

## Notes

- Missing API keys cause affected ingestors to be skipped at startup.
- The SQLite database and image cache grow continuously.
- Images are auto-cleaned: thumbnails after 48 hours, full images after 7 days.
- The backend and frontend are started separately in local development.
