"""
story_card.py — Generates a visual data story card using Gemini 2.5 Flash Image.
Called after an ADK sub-agent returns its {spoken, chart} response.
"""
import asyncio
import base64
import logging
import os

from google import genai
from google.genai import types

log = logging.getLogger(__name__)

_client: genai.Client | None = None


def _get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    return _client


def _build_prompt(spoken: str, intent: str) -> str:
    base = (
        "Dark navy background (#0D1B2A). No people, no faces. "
        "High contrast. White and amber color palette. "
        f"Data finding: {spoken[:300]}"
    )
    if intent == "lending":
        return (
            "NYC borough map infographic. Show the 5 boroughs (Manhattan, Brooklyn, Queens, "
            "Bronx, Staten Island) with color-coded shading representing mortgage denial rates. "
            "Higher denial = deeper red. Key stat in large white text overlay. "
            "Label: 'HMDA MORTGAGE LENDING BIAS'. " + base
        )
    if intent == "algorithm":
        return (
            "NYC agency AI decision pipeline flowchart. Clean horizontal flow diagram: "
            "DATA INPUT → AI MODEL → DECISION OUTPUT. Each node is a rounded rectangle. "
            "Agency name and tool count as prominent label. "
            "Amber accent lines connecting nodes. Label: 'NYC ALGORITHMIC TOOLS'. " + base
        )
    if intent == "gig":
        return (
            "NYC delivery worker earnings heatmap infographic. Five borough outlines with "
            "color gradient from dark (low pay) to bright amber (higher pay). "
            "Large stat showing hourly pay figure. Timeline arrow showing wage increase. "
            "Label: 'DELIVERY WORKER PAY'. " + base
        )
    return ""  # skip for general queries


async def generate_story_card(spoken: str, intent: str = "general") -> dict | None:
    """
    Generate a visual data story card image from the agent's spoken finding.
    Returns {"data": base64_str, "mime_type": "image/png"} or None on failure/timeout.
    Skips generation for general (non-civic) queries.
    """
    if not spoken or not spoken.strip() or intent == "general":
        return None
    prompt = _build_prompt(spoken, intent)
    if not prompt:
        return None
    try:
        return await asyncio.wait_for(_generate(prompt), timeout=10.0)
    except asyncio.TimeoutError:
        log.warning("story_card timed out")
        return None
    except Exception as e:
        log.warning("story_card failed: %s", e)
        return None


async def _generate(prompt: str) -> dict | None:
    client = _get_client()
    response = await client.aio.models.generate_content(
        model="gemini-2.5-flash-preview-image-generation",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_modalities=["IMAGE", "TEXT"],
        ),
    )
    for part in response.candidates[0].content.parts:
        if part.inline_data is not None:
            return {
                "data": base64.b64encode(part.inline_data.data).decode("utf-8"),
                "mime_type": part.inline_data.mime_type or "image/png",
            }
    return None
