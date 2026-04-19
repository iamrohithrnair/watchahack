# London Cortex

**Autonomous city intelligence — monitoring London through 105+ real-time data sources with a production-grade Next.js dashboard.**

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
  105+ Data Sources
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
  │        RETINA (Attention)            │
  │  Foveal attention over London grid   │
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
| Data sources | 105 ingestors (TfL, LAQN, GDELT, Sentinel, BBC, ...) |
| AI agents | 15 agents (Brain, Interpreters, Connectors, ...) |
| LLM | Google Gemini (Flash + Pro) |
| Backend | Python, aiohttp, SQLite, NetworkX |
| Frontend | Next.js 15, React 19, TypeScript, Tailwind CSS |
| Maps | Leaflet with dark CartoDB tiles |
| Real-time | SSE for logs, SWR polling for data |
| Deployment | Docker Compose |

## Frontend Pages

| Page | Description |
|------|-------------|
| `/` | Dashboard — live map with anomaly markers, retina overlay, virtualized log stream |
| `/investigate` | Natural language investigation — ask questions, get SSE-streamed answers |
| `/channels` | Agent conversation feed — Slack-style view of #discoveries, #hypotheses, #anomalies |
| `/cameras` | CCTV camera grid — thumbnail grid with lightbox viewer |

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
│   └── ...
├── agents/             # 15 AI agents
├── ingestors/          # 105 data source adapters
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
