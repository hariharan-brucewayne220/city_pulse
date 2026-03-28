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
    "racial bias in mortgage lending, and gig delivery worker pay and conditions. "
    "Keep responses to 2-3 sentences. Be specific with numbers when you have them. "
    "Do not comment on locations, streets, or anything visual — focus purely on the civic data question being asked."
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
    await asyncio.sleep(2)  # Let MCP server initialize
    yield
    if _mcp_proc:
        _mcp_proc.terminate()
        log.info("MCP server stopped")


app = FastAPI(lifespan=lifespan)

client = genai.Client(api_key=GEMINI_API_KEY)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/config")
async def api_config():
    return {"mapsKey": os.environ.get("GOOGLE_MAPS_API_KEY", "")}


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    log.info("WebSocket connected")

    location: dict = {}

    config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        output_audio_transcription=types.AudioTranscriptionConfig(),
        input_audio_transcription=types.AudioTranscriptionConfig(),
        system_instruction=SYSTEM_INSTRUCTION,
        realtime_input_config=types.RealtimeInputConfig(
            automatic_activity_detection=types.AutomaticActivityDetection(disabled=True)
        ),
    )

    intent_hint: str | None = None

    try:
        async with client.aio.live.connect(model=GEMINI_MODEL, config=config) as session:
            # State machine: collect user audio → drain Gemini response → repeat.
            # Keeping session.receive() permanently open between turns breaks multi-turn,
            # so we only call it after activity_end, per turn.
            while True:
                # Phase 1: read browser messages until audio_end or text_query
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
                        # Direct text query bypasses voice pipeline entirely
                        text = msg.get("text", "").strip()
                        hint = msg.get("intent_hint", "general")
                        if text:
                            log.info("text_query: %r intent=%s", text[:80], hint)
                            asyncio.create_task(_handle_agent_query(websocket, text, hint))
                        continue
                    elif t == "location":
                        location["lat"] = float(msg["lat"])
                        location["lng"] = float(msg["lng"])

                # Phase 2: drain Gemini response until turn_complete
                transcript_buffer = []
                user_transcript_buffer = []
                async for response in session.receive():
                    if not response.server_content:
                        continue
                    sc = response.server_content
                    if sc.input_transcription and sc.input_transcription.text:
                        user_transcript_buffer.append(sc.input_transcription.text)
                    if sc.output_transcription and sc.output_transcription.text:
                        transcript_buffer.append(sc.output_transcription.text)
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

                full_text = "".join(transcript_buffer)
                user_text = "".join(user_transcript_buffer)
                if full_text:
                    log.info("Gemini said: %s", full_text[:120])
                if user_text:
                    log.info("User said: %s", user_text[:120])
                await websocket.send_text(json.dumps({"type": "transcript", "text": full_text}))

                # Phase 3: classify what the USER said (not Gemini's response)
                # This prevents Gemini's own replies from triggering spurious agent queries
                classify_text = user_text
                if classify_text:
                    current_hint = intent_hint
                    intent_hint = None  # consume hint
                    if not current_hint or current_hint == "general":
                        from agents.orchestrator import _classify_intent
                        current_hint = _classify_intent(classify_text)
                    if current_hint and current_hint != "general":
                        # Run agent chart query in background — voice loop stays free
                        asyncio.create_task(_handle_agent_query(websocket, classify_text, current_hint))
                    else:
                        # Street-level query → data cards update
                        asyncio.create_task(_query_and_send(websocket, classify_text, location))

    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.error("ws_live error: %s", e)
        try:
            await websocket.send_text(json.dumps({"type": "error", "message": str(e)}))
        except Exception:
            pass


async def _handle_agent_query(
    websocket: WebSocket, text: str, intent: str
) -> None:
    """Route to ADK orchestrator and send agent_response (chart + story card) to browser."""
    from agents.story_card import generate_story_card
    try:
        agent_result = await route_query(text, intent)
        spoken = agent_result.get("spoken", "")
        chart = agent_result.get("chart")

        # Generate story card — intent-aware prompt, timeout handled inside
        story_card = await generate_story_card(spoken, intent)

        await websocket.send_text(json.dumps({
            "type": "agent_response",
            "transcript": text,
            "spoken": spoken,
            "chart": chart,
            "story_card": story_card,
        }))
        log.info("agent_response sent: spoken=%s chart=%s story_card=%s",
                 spoken[:80], chart is not None, story_card is not None)
    except Exception as e:
        log.error("_handle_agent_query failed: %s", e)
        try:
            await websocket.send_text(json.dumps({
                "type": "agent_response",
                "transcript": text,
                "spoken": "Sorry, I had trouble querying the data.",
                "chart": None,
                "story_card": None,
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


async def _query_and_send(websocket: WebSocket, gemini_text: str, location: dict) -> str | None:
    """Query NYC data, push updates to browser, return a data summary string for Gemini narration."""
    place = extract_location(gemini_text)
    if place:
        log.info("Geocoding place from Gemini: %s", place)
        coords = await _geocode(place)
        if coords:
            location["lat"], location["lng"] = coords
            log.info("Geocoded %r → %.4f, %.4f", place, *coords)
            await websocket.send_text(json.dumps({"type": "location_detected", "place": place}))

    lat = location.get("lat")
    lng = location.get("lng")
    if lat is None or lng is None:
        log.info("No location available, skipping data query")
        return None

    results = {}

    async def _fetch_and_send(dataset: str, coro):
        try:
            result = await coro
            results[dataset] = result
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

    # Build a concise data summary for Gemini to narrate
    parts = []
    if "311" in results:
        r = results["311"]
        if r["count"] > 0:
            parts.append(f"311 complaints: {r['count']} in the last 30 days, top issue is {r['value']}")
        else:
            parts.append("No 311 complaints in the last 30 days")
    if "restaurants" in results:
        r = results["restaurants"]
        parts.append(f"Restaurants: {r['detail']}")
    if "crime" in results:
        r = results["crime"]
        if r["count"] > 0:
            parts.append(f"Crime: {r['count']} incidents in 30 days, most common is {r['value']}")
        else:
            parts.append("No crimes reported in the last 30 days")

    return ". ".join(parts) if parts else None
