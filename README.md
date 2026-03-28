# City Pulse — NYC Civic Data Intelligence Agent

> **Team:** City Pulse
> **Hackathon:** Google × NYU Tandon Build With AI 2026
> **Live Demo:** https://city-pulse-551379536248.us-central1.run.app

---

## What It Does

City Pulse is a real-time voice intelligence agent that lets you ask questions about NYC civic data — and get answers backed by actual datasets, not hallucinations.

Speak naturally. Gemini Live listens, understands, and responds by voice. Simultaneously, a multi-agent pipeline queries real NYC open datasets and renders precise charts and a live map — all in under 20 seconds.

**Three civic lenses:**

| Topic | Dataset | Key Finding |
|-------|---------|-------------|
| AI & Algorithmic Tools | NYC Open Data — 132 tools across 36 agencies | 76% of tools have unknown vendors; 11× growth since 2017 |
| Mortgage Access | HMDA 2017 — 114,676 applications | Bronx approval rate 68% vs Manhattan 82% — 14-point gap |
| Gig Worker Pay | NYC DCWP quarterly data | Wage law raised base pay 79% but tips collapsed 53% and 37,000 workers left the apps |

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │         Browser (UI)             │
                    │  Google Maps · Chart.js · WebSocket │
                    └────────────┬────────────────────┘
                                 │ WebSocket /ws/live
                    ┌────────────▼────────────────────┐
                    │      FastAPI Backend             │
                    │         main.py                  │
                    └──────┬──────────────┬───────────┘
                           │              │
              ┌────────────▼──┐    ┌──────▼──────────────┐
              │  Gemini Live   │    │    ADK Orchestrator   │
              │  (Real-time    │    │    (Intent routing)   │
              │   voice S2S)   │    └──────┬───────────────┘
              └────────────────┘           │
                                  ┌────────▼───────────┐
                                  │   ADK Sub-Agents    │
                                  │ algorithm · lending │
                                  │      · gig          │
                                  └────────┬───────────┘
                                           │ MCP (Streamable HTTP)
                                  ┌────────▼───────────┐
                                  │   FastMCP Server    │
                                  │   DuckDB + CSVs     │
                                  │ (NYC open datasets) │
                                  └────────────────────┘
```

**Two parallel answer paths:**
- **Voice** — Gemini Live responds immediately in natural speech
- **Chart + Map** — ADK agents query MCP/DuckDB, return precise numbers and visualizations

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Voice AI | Gemini Live API (`gemini-3.1-flash-live-preview`) — real-time speech-to-speech |
| Agent Orchestration | Google ADK (Agent Development Kit) — A2A routing, sub-agents |
| Data Protocol | MCP (Model Context Protocol) via FastMCP — streamable HTTP transport |
| Data Engine | DuckDB — in-process SQL on NYC CSV datasets |
| Maps | Google Maps JavaScript API — borough choropleth, location auto-pan |
| Charts | Chart.js — bar, line, pie with slide-in animation |
| Backend | FastAPI + uvicorn (Python 3.12) |
| Frontend | Vanilla JS + Tailwind CSS |
| Deployment | Google Cloud Run |

---

## Google Technologies Used

- **Gemini Live API** — Real-time bidirectional voice, manual VAD, input/output transcription
- **Google ADK** — Multi-agent orchestration with A2A routing and sub-agent delegation
- **MCP (Model Context Protocol)** — Standardized tool interface between agents and data
- **Gemini Flash Lite** — Powers each civic sub-agent (algorithm, lending, gig)
- **Google Maps JavaScript API** — Borough choropleth, Places API for restaurant ratings, geolocation
- **Google Cloud Run** — Serverless container deployment

---

## How It Works

### Voice Pipeline
1. User presses mic → browser captures PCM audio at 16kHz
2. Audio streams to Gemini Live via WebSocket with manual VAD
3. Silence detected → `ActivityEnd` sent → Gemini responds by voice
4. Input transcription captured → intent classified (algorithm / lending / gig / general)

### Data Pipeline (parallel)
5. Intent matched → ADK orchestrator routes to sub-agent
6. Sub-agent writes SQL → calls `query_dataset` tool via MCP
7. FastMCP server executes SQL on DuckDB against real CSV data
8. Agent formats result → `{spoken, chart}` JSON returned
9. Chart slides in on frontend, typewriter shows precise finding

### Map
- Lending query → borough choropleth colored by mortgage approval rate (real HMDA data)
- Location detected from speech → map auto-pans + drops pin
- 311 data → pulsing circle overlay at query location

---

## Datasets

| File | Source | Records |
|------|--------|---------|
| `mcp_server/data/hmda_nyc.csv` | CFPB Home Mortgage Disclosure Act 2017 | 114,676 |
| `mcp_server/data/algorithmic_tools.csv` | NYC Open Data — Automated Decision Systems | 132 tools |
| `mcp_server/data/delivery_workers.csv` | NYC DCWP quarterly earnings reports | 14 quarters |

---

## Running Locally

### Prerequisites
- Python 3.12+
- `uv` (or pip)
- Google AI API key with Gemini Live access
- Google Maps API key (optional, for map features)

### Setup

```bash
git clone https://github.com/hariharan-brucewayne220/city_pulse.git
cd city_pulse

# Create virtual environment
uv venv .venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your keys:
#   GEMINI_API_KEY=...
#   GOOGLE_MAPS_API_KEY=...  (optional)

# Run
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open http://localhost:8000 — click the mic button and ask a civic question.

---

## Demo Questions

**AI & Algorithms**
- *"Which NYC agencies use the most AI tools?"*
- *"How many of these tools have unknown vendors?"*
- *"How fast has NYC been deploying new AI tools?"*

**Mortgage Access**
- *"Which NYC boroughs have the lowest mortgage approval rates?"*
- *"What types of home loans get denied the most in NYC?"*
- *"What are the top reasons mortgages get denied?"*

**Gig Workers**
- *"How has delivery worker pay changed since NYC passed the minimum wage law?"*
- *"What happened to tips after the wage law?"*
- *"Did the number of delivery workers go up or down after the law?"*

**Street Level (uses your location)**
- *"What's the situation in Bushwick right now?"*
- *"Tell me about crime in the South Bronx."*

---

## Project Structure

```
city_pulse/
├── main.py                  # FastAPI app, WebSocket handler, Gemini Live loop
├── agents/
│   ├── orchestrator.py      # ADK routing — classifies intent, delegates to sub-agents
│   ├── algorithm_agent.py   # Queries algorithmic_tools via MCP
│   ├── lending_agent.py     # Queries hmda_nyc via MCP
│   └── gig_agent.py         # Queries delivery_workers via MCP
├── mcp_server/
│   ├── server.py            # FastMCP server — exposes query_dataset tool
│   └── data/                # Pre-generated NYC CSVs (committed for runtime)
├── data/
│   ├── nyc_311.py           # Live 311 complaint queries (Socrata API)
│   ├── restaurants.py       # NYC DOH restaurant inspection queries
│   └── crime.py             # NYPD crime incident queries
├── static/
│   └── index.html           # Full single-page app (maps, charts, audio, WebSocket)
├── intent.py                # Location extraction from transcripts
├── Dockerfile               # Cloud Run container
└── requirements.txt
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | Yes | Google AI API key with Gemini Live access |
| `GOOGLE_MAPS_API_KEY` | No | Enables map choropleth and Places ratings |
| `MCP_PORT` | No | Port for MCP server (default: 8001) |
| `PORT` | No | Port for FastAPI (default: 8080 on Cloud Run) |

---

## License

MIT
