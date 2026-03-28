import asyncio
import base64
import json
import logging
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from google import genai
from google.genai import types

import httpx

from data.nyc_311 import query_311
from data.restaurants import query_restaurants
from data.crime import query_crime
from intent import extract_location
from agents.orchestrator import route_query

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-3.1-flash-live-preview"
STATIC_DIR = Path(__file__).parent / "static"
MCP_PORT = int(os.environ.get("MCP_PORT", 8001))

SYSTEM_INSTRUCTION = (
    "You are City Pulse, an NYC civic data intelligence agent. "
    "You answer voice questions about NYC civic issues: AI/algorithmic tools used by city agencies, "
    "mortgage lending access by borough, and gig delivery worker pay. "
    "Greet ONLY once at session start with exactly: 'City Pulse ready.' "
    "Never repeat that greeting. Stay silent on empty or unclear input. "
    "Keep all answers to 1-2 sentences. Be specific with numbers when you have them. "
    "Never ask follow-up questions. Never offer a menu of topics. Just answer directly."
)

_mcp_proc: subprocess.Popen | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _mcp_proc
    mcp_path = Path(__file__).parent / "mcp_server" / "server.py"
    _mcp_proc = subprocess.Popen(
        [sys.executable, str(mcp_path), "--port", str(MCP_PORT)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    log.info("MCP server started (pid=%d, port=%d)", _mcp_proc.pid, MCP_PORT)
    await asyncio.sleep(2)
    yield
    if _mcp_proc:
        _mcp_proc.terminate()
        log.info("MCP server stopped")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

client = genai.Client(api_key=GEMINI_API_KEY)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/slides")
async def slides():
    return FileResponse(STATIC_DIR / "slides.html")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/config")
async def api_config():
    return {"mapsKey": os.environ.get("GOOGLE_MAPS_API_KEY", "")}


async def _drain_turn(session, websocket: WebSocket) -> tuple[str, str]:
    """Drain one Gemini turn, forward audio to browser. Returns (user_text, gemini_text)."""
    transcript_buf = []
    user_buf = []
    async for response in session.receive():
        if not response.server_content:
            continue
        sc = response.server_content
        if sc.input_transcription and sc.input_transcription.text:
            user_buf.append(sc.input_transcription.text)
        if sc.output_transcription and sc.output_transcription.text:
            transcript_buf.append(sc.output_transcription.text)
        if sc.model_turn:
            for p in sc.model_turn.parts:
                if hasattr(p, "inline_data") and p.inline_data and p.inline_data.data:
                    audio_b64 = base64.b64encode(p.inline_data.data).decode()
                    await websocket.send_text(json.dumps({
                        "type": "audio_output",
                        "data": audio_b64,
                    }))
        if sc.turn_complete:
            break
    return "".join(user_buf), "".join(transcript_buf)


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    log.info("WebSocket connected")

    location: dict = {}
    intent_hint: str | None = None

    config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        output_audio_transcription=types.AudioTranscriptionConfig(),
        input_audio_transcription=types.AudioTranscriptionConfig(),
        system_instruction=SYSTEM_INSTRUCTION,
        realtime_input_config=types.RealtimeInputConfig(
            automatic_activity_detection=types.AutomaticActivityDetection(disabled=True)
        ),
    )

    try:
        async with client.aio.live.connect(model=GEMINI_MODEL, config=config) as session:
            while True:
                # ── Phase 1: collect browser messages until audio_end ──────────
                while True:
                    raw = await websocket.receive_text()
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "audio_start":
                        await session.send_realtime_input(activity_start=types.ActivityStart())
                        log.info("activity_start sent")
                    elif t == "audio":
                        pcm = base64.b64decode(msg["data"])
                        await session.send_realtime_input(
                            audio=types.Blob(data=pcm, mime_type="audio/pcm;rate=16000")
                        )
                    elif t == "audio_end":
                        await session.send_realtime_input(activity_end=types.ActivityEnd())
                        log.info("activity_end sent")
                        break
                    elif t == "intent_hint":
                        intent_hint = msg.get("hint", "general")
                    elif t == "text_query":
                        # Demo buttons bypass voice — agent runs as background task
                        text = msg.get("text", "").strip()
                        hint = msg.get("intent_hint", "general")
                        if text:
                            log.info("text_query: %r intent=%s", text[:80], hint)
                            asyncio.create_task(
                                _handle_civic_query(websocket, text, hint)
                            )
                        continue
                    elif t == "location":
                        location["lat"] = float(msg["lat"])
                        location["lng"] = float(msg["lng"])

                # ── Phase 2: drain Gemini's acknowledgment ("Checking data.") ──
                user_text, gemini_ack = await _drain_turn(session, websocket)
                if gemini_ack:
                    log.info("Gemini ack: %s", gemini_ack[:80])
                if user_text:
                    log.info("User said: %s", user_text[:120])

                # ── Phase 3: route based on intent ────────────────────────────
                if not user_text:
                    continue

                current_hint = intent_hint
                intent_hint = None
                if not current_hint or current_hint == "general":
                    from agents.orchestrator import _classify_intent
                    current_hint = _classify_intent(user_text)

                if current_hint and current_hint != "general":
                    # Civic query — agent runs in background, sends chart + text when ready
                    log.info("A2A routing: intent=%s question=%r", current_hint, user_text[:80])
                    asyncio.create_task(_handle_civic_query(websocket, user_text, current_hint))
                else:
                    # Street-level query → 311/crime/restaurants
                    asyncio.create_task(_query_and_send(websocket, user_text, location))

    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.error("ws_live error: %s", e)
        try:
            await websocket.send_text(json.dumps({"type": "error", "message": str(e)}))
        except Exception:
            pass


async def _handle_civic_query(websocket: WebSocket, text: str, intent: str) -> None:
    """Run ADK agent → MCP → send chart + spoken text to frontend."""
    try:
        agent_result = await route_query(text, intent)
        spoken = agent_result.get("spoken", "")
        chart = agent_result.get("chart")
        await websocket.send_text(json.dumps({
            "type": "agent_response",
            "transcript": text,
            "spoken": spoken,
            "chart": chart,
            "intent": intent,
        }))
        log.info("agent_response sent: spoken=%s chart=%s intent=%s", spoken[:80], chart is not None, intent)
    except Exception as e:
        log.error("_handle_civic_query failed: %s", e)
        try:
            await websocket.send_text(json.dumps({
                "type": "agent_response",
                "transcript": text,
                "spoken": "Sorry, I had trouble querying the data.",
                "chart": None,
                "intent": intent,
            }))
        except Exception:
            pass


async def _geocode(place: str) -> tuple[float, float] | None:
    """Geocode a place name to (lat, lng) using Nominatim, restricted to NYC."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": f"{place}, New York City", "format": "json", "limit": 1},
                headers={"User-Agent": "CityWitness/1.0"},
            )
            results = r.json()
            if results:
                return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        log.warning("Geocode failed for %r: %s", place, e)
    return None


async def _query_and_send(websocket: WebSocket, gemini_text: str, location: dict) -> None:
    """Query NYC street-level data, push data_update messages to browser."""
    place = extract_location(gemini_text)
    if place:
        log.info("Geocoding place: %s", place)
        coords = await _geocode(place)
        if coords:
            location["lat"], location["lng"] = coords
            log.info("Geocoded %r → %.4f, %.4f", place, *coords)
            await websocket.send_text(json.dumps({
                "type": "location_detected",
                "place": place,
                "lat": location["lat"],
                "lng": location["lng"],
            }))

    lat = location.get("lat")
    lng = location.get("lng")
    if lat is None or lng is None:
        log.info("No location available, skipping data query")
        return

    async def _fetch_and_send(dataset: str, coro):
        try:
            result = await coro
            await websocket.send_text(json.dumps({
                "type": "data_update",
                "dataset": dataset,
                "value": result["value"],
                "count": result["count"],
                "detail": result["detail"],
                "breakdown": result.get("breakdown", []),
                "restaurants": result.get("restaurants", []),
            }))
        except Exception as e:
            log.warning("data query failed for %s: %s", dataset, e)

    await asyncio.gather(
        _fetch_and_send("311", query_311(lat, lng)),
        _fetch_and_send("restaurants", query_restaurants(lat, lng)),
        _fetch_and_send("crime", query_crime(lat, lng)),
        return_exceptions=True,
    )
