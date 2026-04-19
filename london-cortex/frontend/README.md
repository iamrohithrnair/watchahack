# London Cortex Dashboard

Next.js 15 frontend for the London Cortex autonomous city intelligence system.

## Development

```bash
npm install
npm run dev
```

The dashboard connects to the Python backend at `http://localhost:8000` via API rewrites configured in `next.config.ts`. Set `NEXT_PUBLIC_API_URL` to override.

## Design

The dashboard uses a "Tactical Urban Intelligence" aesthetic:
- **Typography**: Syne (display), DM Sans (body), JetBrains Mono (data)
- **Colors**: Deep midnight (#060610) with electric teal (#00e5c3) accent
- **Real-time**: SSE for log streaming, SWR polling for structured data

## Pages

- `/` — Live map, log stream, anomaly stats
- `/investigate` — Natural language query interface
- `/channels` — Agent conversation feed
- `/cameras` — CCTV camera grid

## Key Libraries

| Library | Purpose |
|---------|---------|
| SWR | Data fetching with polling |
| react-leaflet | Interactive map |
| @tanstack/react-virtual | Virtualized log list |
| Tailwind CSS 4 | Styling |
