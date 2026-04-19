# London Cortex Deployment Guide

## Recommended setup

For this repo, the cleanest production setup is:

| Part | Platform | Why |
| --- | --- | --- |
| Frontend (`london-cortex/frontend`) | **Vercel** | It is a standard Next.js app, which Vercel handles extremely well. |
| Backend (`python -m cortex`) | **Render Web Service** | The backend is an always-on Python process with schedulers, a daemon, WebSocket/SSE endpoints, and persistent local data. Render fits that model better than Vercel. |

## Why not put the backend on Vercel?

The backend is not just a small request/response API. It continuously runs ingestors, agents, maintenance jobs, WebSocket streaming, SSE investigation streams, and writes state into `london-cortex/data`.

That makes it a much better fit for an **always-on service** than a serverless deployment model.

---

## 1. Deploy the frontend on Vercel

### What Vercel is deploying

The frontend is a Next.js app in:

```bash
london-cortex/frontend
```

It already supports a backend base URL through:

```bash
NEXT_PUBLIC_API_URL
```

and rewrites `/api`, `/ws`, `/stream`, and `/images` to the backend.

### Step-by-step

1. Go to Vercel and import this Git repository.
2. When Vercel asks for the project directory, set the **Root Directory** to:

   ```bash
   london-cortex/frontend
   ```

3. Vercel should auto-detect **Next.js** as the framework.
4. Add this environment variable in Vercel:

   ```bash
   NEXT_PUBLIC_API_URL=https://your-backend-service.onrender.com
   ```

5. Deploy.

### What to expect

- Build command: Vercel should use the app's existing Next.js build automatically.
- Output: Vercel will serve the frontend and route frontend API calls to your backend URL.
- Preview deployments: if you want branch previews to hit a staging backend instead of production, set a separate Preview value for `NEXT_PUBLIC_API_URL`.

### Vercel notes for this repo

- This is a **monorepo-style import**, so the important setting is the frontend **Root Directory**.
- You do **not** need to move files around for Vercel.
- You do **not** need a custom `vercel.json` just to get this working.

### Official Vercel sections used

- **Monorepos** - choosing a project Root Directory
- **Configure a Build** - framework/build autodetection
- **Environment Variables** - setting `NEXT_PUBLIC_API_URL`

---

## 2. Deploy the backend on Render.com (recommended)

### Why Render is the best fit here

London Cortex behaves like a long-running city intelligence service, not a lightweight API function. Render gives you:

- an always-on web service
- a health check path
- a persistent disk
- easy Git-based deploys

That matches this backend very well.

### What Render is deploying

The backend is started from the repo root with:

```bash
python -m cortex
```

It serves HTTP on port `8000` and exposes:

- `/api/health`
- `/api/*`
- `/ws`
- `/stream`
- `/images/*`

It also persists runtime state under:

```bash
london-cortex/data
```

### Render service settings

Create a **Web Service** in Render with these settings:

| Setting | Value |
| --- | --- |
| Root Directory | repo root |
| Runtime | Python |
| Build Command | `pip install -r london-cortex/requirements.txt` |
| Start Command | `python -m cortex` |
| Health Check Path | `/api/health` |

### Persistent disk

This app writes SQLite, cached images, memory snapshots, and logs into `london-cortex/data`, so you should attach a persistent disk.

Use this mount path on Render:

```bash
/opt/render/project/src/london-cortex/data
```

That matches Render's Python source path layout and ensures the app's runtime data survives restarts and deploys.

### Required environment variables

At minimum, set **one** LLM provider:

```bash
GEMINI_API_KEY=...
```

or:

```bash
GLM_API_KEY=...
LLM_PROVIDER=glm
```

### Optional environment variables

Many ingestors are optional. If you do not set their keys, London Cortex will skip them at startup instead of crashing.

That means you can launch with only the core LLM key first, then add richer data sources later.

Examples of optional keys used by some ingestors:

- `BODS_API_KEY`
- `TOMTOM_API_KEY`
- `WAZE_FEED_URL`
- `PREDICTHQ_API_KEY`
- `ACLED_API_KEY`
- `ACLED_EMAIL`
- `CLOUDFLARE_RADAR_API_TOKEN`

### Important deployment note

The backend currently binds to its configured internal port `8000`, so make sure your Render web service is configured to route traffic to that port.

### Post-deploy check

Once deployed, this should return JSON:

```bash
https://your-backend-service.onrender.com/api/health
```

Expected response:

```json
{"status":"ok"}
```

### Official Render docs used

- **Web Services** - service type, build command, start command, port binding
- **Health Checks** - using `/api/health`
- **Disks** - attaching a persistent disk for `london-cortex/data`

---

## 3. Point Vercel at Render

After the Render backend is live, go back to Vercel and set:

```bash
NEXT_PUBLIC_API_URL=https://your-backend-service.onrender.com
```

Then redeploy the frontend.

At that point the Vercel frontend should talk to:

- Render `/api/*`
- Render `/ws`
- Render `/stream`
- Render `/images/*`

---

## 4. Fly.io as a strong alternative

If you want more infrastructure control than Render, **Fly.io** is a good second choice.

### Why Fly.io can work well

- It is a strong fit for long-running Python services.
- It supports persistent volumes.
- It supports health checks.

### Why I still prefer Render here

Render is the simpler path for this repo because it is more dashboard-driven and easier to set up quickly for a persistent Python web process.

### If you choose Fly.io

You will likely want:

- a `fly.toml`
- a mounted volume for the app's data directory
- health checks targeting `/api/health`
- the service listening on port `8000`

If you go with Fly, mount persistent storage where the app writes runtime state, not just at an arbitrary directory.

For this repo, that means aligning the mount with:

```bash
london-cortex/data
```

### Official Fly.io docs used

- **Python on Fly.io**
- **Fly Volumes**
- **Health Checks**

---

## 5. Railway: possible, but not my first recommendation

Railway is modern and pleasant to use, but for this repo it is not the easiest first deployment target.

### Why

Railway expects web services to listen on the platform-provided `PORT` environment variable, while this backend currently binds to a fixed internal port (`8000`).

So Railway is possible, but I would treat it as:

1. a **small code-change deployment**
2. plus **persistent volume setup**

That makes it a little less plug-and-play than Render for the current codebase.

### When Railway makes sense

Pick Railway if you want:

- a very fast Git-connected deployment flow
- centralized variables management
- a modern UI

But for this repo as it exists today, **Render is the smoother backend choice**.

### Official Railway docs used

- **Variables** - environment variables are available at build and runtime
- Railway web services expect the app to bind to the provided `PORT`

---

## Final recommendation

If you want the deployment stack with the least friction and the highest chance of working on the first pass:

1. **Frontend on Vercel**
2. **Backend on Render**
3. **Persistent disk on Render for `london-cortex/data`**
4. **`NEXT_PUBLIC_API_URL` in Vercel pointing to the Render backend**

That setup matches the architecture of London Cortex well: a polished frontend on a frontend-first platform, and a stateful always-on backend on a platform built for persistent services.
