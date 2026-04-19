# London Cortex

**Autonomous city intelligence — monitoring London through 106+ real-time data sources with a production-grade Next.js dashboard.**

London Cortex is an autonomous city intelligence platform that watches London in real time, spots unusual changes as they happen, and turns hundreds of fast-moving signals into something a human can actually understand. It feels like a city that can explain itself.

## Why it feels special

Most city dashboards show data. London Cortex behaves more like an intelligent operating system.

It is wired to **106 ingestors** today across transport, air quality, weather, energy, news, public services, finance, infrastructure, satellite feeds, and more. Instead of leaving you to manually connect the dots, it continuously ingests, compares, prioritizes, and explains what matters.

In short, London Cortex:

1. **Watches** London through 100+ live data ingestors
2. **Detects** what is genuinely unusual
3. **Focuses** attention where it matters most
4. **Shows** the evidence on a map and through live camera imagery
5. **Explains** what is happening in plain language

## Quick Start

```bash
# Set your Gemini API key
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY

# Option A: Docker (production)
docker compose up

# Option B: Local development
cd london-cortex
pip install -r requirements.txt
python -m cortex &
cd frontend && npm install && npm run dev
```

- Backend API: http://localhost:8000
- Frontend dashboard: http://localhost:3000

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

### Data flow

Every ingestor runs on its own cadence, writes observations into the shared SQLite message board, and updates rolling baselines in memory. The platform is not just collecting snapshots — it builds a sense of what "normal" looks like for each source, place, and metric.

### Anomaly detection

An anomaly is not just "something interesting." It means a signal has moved far enough away from its usual pattern to be statistically unusual. The platform computes a z-score against a rolling baseline, and anything roughly **two standard deviations away from normal or more** gets elevated.

Anomalies are stored with timestamps, severity, source, and location. On the map they are color-coded by severity: **Low → Notice → Medium → High → Critical**.

### Attention engine

The city is divided into a **500m spatial grid**, and the system treats it like a visual field. The Retina layer splits London into three attention zones:

| Zone | Map label | Behaviour |
|------|-----------|-----------|
| **Focus zone** | 🟠 Focus zone | Full resolution — every observation stored, all agents process |
| **Awareness zone** | 🔵 Awareness zone | Moderate resolution — sampled observations, key agents only |
| **Background** | ⚪ Background | Minimal — only anomalies tracked |

When a serious anomaly appears — or when multiple sources agree that something unusual is happening in the same place — the system triggers a **saccade**: it snaps attention toward that area, promotes the affected cell into focus, and expands attention to the surrounding neighborhood. London Cortex dynamically decides where to pay closer attention, more like an intelligent operating system than a passive dashboard.

### Agent conversation channels

Specialist agents write into shared channels through the message board: **#discoveries, #hypotheses, #anomalies, #requests, and #meta**. Interpreters look at raw signals, connectors link events across domains, the Brain synthesizes bigger narratives, the validator checks predictions, and the chronicler tracks longer-running stories. The dashboard exposes that internal coordination in a human-readable, Slack-style view.

### Investigation panel

You can ask a natural language question like a real operator would: *What is happening in this part of London? Why are transport and air quality both spiking? Is this related to a public event?*

The Investigator agent plans the query, searches the platform's own anomalies, observations, channels, and discoveries, combines that with live web context, follows leads, pulls related evidence, reads article content, and streams findings back into the UI as the answer forms. The user experience feels conversational, but the backend is structured, evidence-led, and multi-stage.

## Stack

| Layer | Technology |
|-------|-----------|
| Data sources | 106 ingestors (TfL, LAQN, GDELT, Sentinel, BBC, ...) |
| AI agents | 15 agents (Brain, Interpreters, Connectors, ...) |
| LLM | Google Gemini (3.1 Flash-Lite + 3 Flash) |
| Backend | Python, aiohttp, SQLite, NetworkX |
| Frontend | Next.js 15, React 19, TypeScript, Tailwind CSS |
| Maps | Leaflet with dark CartoDB tiles |
| Real-time | SSE for logs, SWR polling for data |
| Deployment | Docker Compose |

## Frontend Pages

| Page | Description |
|------|-------------|
| `/` | Dashboard — live map with anomaly markers, attention overlay, virtualized log stream |
| `/investigate` | Natural language investigation — ask questions, get SSE-streamed answers |
| `/channels` | Agent conversation feed — Slack-style view of #discoveries, #hypotheses, #anomalies |
| `/cameras` | CCTV camera grid — TfL JamCam thumbnail grid with lightbox viewer |

### Cameras

The camera view provides a grid of recent London traffic camera imagery — a live visual layer for the city. JamCam images are ingested, cached into thumbnail and full-image stores, and surfaced in the dashboard. The system tracks camera health over time, classifying feeds as **active**, **suspect**, or **dead** based on staleness and consecutive failures.

### System controls

System stats, source health, live logs, and LLM backend settings are available in one place. The backend exposes real-time API and WebSocket endpoints for health, logs, anomalies, channels, map data, cameras, and investigations. The frontend rides on top of a continuously running city intelligence engine, not a static dataset.

## Environment Variables

See `.env.example` for the full list. Only `GEMINI_API_KEY` is required — all others are optional.

## Project Structure

```
london-cortex/
├── run.py              # Main entry point
├── core/               # Backend infrastructure
│   ├── board.py        # SQLite message board
│   ├── config.py       # Configuration
│   ├── coordinator.py  # Adaptive scheduler
│   ├── dashboard.py    # API server (aiohttp :8000)
│   ├── graph.py        # Spatial graph (CortexGraph)
│   ├── retina.py       # Attention engine
│   └── ...
├── agents/             # 15 AI agents
├── ingestors/          # 106 data source adapters
│   └── registry.py     # Declarative ingestor registry
├── frontend/           # Next.js 15 app
│   ├── src/
│   │   ├── app/        # Pages (App Router)
│   │   ├── components/ # React components
│   │   ├── hooks/      # useSSE, useAPI, useInvestigate
│   │   └── lib/        # Types, API client, constants
│   └── Dockerfile
├── docker-compose.yml
└── Dockerfile.backend
```
