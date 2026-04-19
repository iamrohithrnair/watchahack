# London Cortex Demo

## What it is, in one sentence

London Cortex is an autonomous city intelligence platform that watches London in real time, spots unusual changes as they happen, and turns hundreds of fast-moving signals into something a human can actually understand.

If you want the short startup-pitch version: **it feels like a city that can explain itself.**

## Why it feels special

Most city dashboards show data. London Cortex behaves more like an intelligent operating system.

It is wired to **106 ingestors** today across transport, air quality, weather, energy, news, public services, finance, infrastructure, satellite feeds, and more. Instead of leaving you to manually connect the dots, it continuously ingests, compares, prioritizes, and explains what matters.

## The walkthrough

### 1. The moment you open it, London is already alive

The first thing you see is not a static dashboard. It feels active. Counts are moving, sources are updating, and the live log is streaming what the system is doing right now.

**What is happening in the background:** every ingestor runs on its own cadence, writes observations into the shared SQLite message board, and updates rolling baselines in memory. That means the platform is not just collecting snapshots - it is building a sense of what “normal” looks like for each source, place, and metric.

### 2. It does not just collect data - it notices when the city behaves strangely

One of the strongest moments in the product is the anomaly view.

In London Cortex, an **anomaly** is not just “something interesting.” It means a signal has moved far enough away from its usual pattern to be statistically unusual. Under the hood, the platform computes a z-score against a rolling baseline, and anything roughly **two standard deviations away from normal or more** gets elevated.

On the map, you can view these anomalies across London. Each marker is color-coded by severity:

1. **Low**
2. **Notice**
3. **Medium**
4. **High**
5. **Critical**

So when you see the map change color, you are not looking at decoration. You are looking at the city telling you where something has genuinely shifted.

**What is happening in the background:** anomalies are stored with timestamps, severity, source, and location. If a source does not provide coordinates directly, London Cortex can fall back to the center of the relevant grid cell so the signal still appears on the map in a meaningful place.

### 3. The map is not passive - it is an attention engine

This is where London Cortex starts to feel like a startup demo with real depth.

The city is divided into a **500m spatial grid**, and the system treats it like a visual field. Quiet areas stay in the periphery. Hotspots get promoted toward the center of attention.

**What is happening in the background:** the Retina layer splits London into **fovea, perifovea, and periphery**. When a serious anomaly appears - or when multiple sources agree that something unusual is happening in the same place - the system triggers a **saccade**. In plain English: it snaps attention toward that area, promotes the affected cell into focus, and expands attention to the surrounding neighborhood too.

That is a big part of the wow factor. London Cortex is not trying to look at every square meter with the same intensity all the time. It is dynamically deciding where to pay closer attention, more like an intelligent operating system than a passive dashboard.

### 4. You can open the camera view and instantly ground the signal in reality

If the map tells you *where* something unusual is happening, the camera view helps answer *what it actually looks like*.

Open the **Cameras** view and you get a grid of recent London traffic camera imagery - effectively a CCTV-style live visual layer for the city. It is a very strong demo moment because it takes the platform from abstract intelligence to something tangible and immediate.

**What is happening in the background:** JamCam images are ingested, cached into thumbnail and full-image stores, and surfaced in the dashboard as a live grid. The system also tracks camera health over time, classifying feeds as **active**, **suspect**, or **dead** based on staleness and consecutive failures, so the visual layer can be trusted instead of blindly displayed.

### 5. The left side is where the city starts talking to itself

One of the most compelling product details is the channel system: **#discoveries, #hypotheses, #anomalies, #requests, and #meta**.

This makes the platform feel less like a database and more like an active intelligence team. You are not only seeing outputs - you are seeing the internal conversation that leads to those outputs.

**What is happening in the background:** specialist agents write into shared channels through the message board. Interpreters look at raw signals, connectors link events across domains, the Brain synthesizes bigger narratives, the validator checks predictions, and the chronicler tracks longer-running stories. The dashboard simply exposes that internal coordination in a very human-readable way.

### 6. The investigation panel is where it stops being a dashboard and becomes a teammate

This is probably the feature that makes people lean in.

You can ask a natural language question like a real operator would: *What is happening in this part of London? Why are transport and air quality both spiking? Is this related to a public event?*

Then London Cortex investigates for you.

**What is happening in the background:** the Investigator agent plans the query first, searches the platform’s own anomalies, observations, channels, and discoveries, then combines that with live web context. It can follow leads, pull related evidence, read article content, and stream findings back into the UI as the answer is forming. So the user experience feels conversational, but the backend behavior is structured, evidence-led, and multi-stage.

### 7. It keeps the human in control without breaking the magic

There is also a practical polish to the experience. You can see system stats, source health, live logs, and settings for the LLM backend in one place.

**What is happening in the background:** the backend exposes real-time API and WebSocket endpoints for health, logs, anomalies, channels, map data, cameras, and investigations. The front end is effectively riding on top of a continuously running city intelligence engine, not polling a dead dataset.

## Why this would land in a startup pitch

London Cortex is compelling because it turns a messy, overwhelming city into something legible.

It is not “yet another dashboard.” It is a system that:

1. **Watches** London through 100+ live data ingestors
2. **Detects** what is genuinely unusual
3. **Focuses** attention where it matters most
4. **Shows** the evidence on a map and through live camera imagery
5. **Explains** what is happening in plain language

That combination is the real hook: **city-scale sensing, AI attention, and human-readable investigation in one product.**
