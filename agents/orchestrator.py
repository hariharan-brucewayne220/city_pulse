"""
City Pulse Orchestrator — ADK root agent that routes queries to specialist sub-agents via A2A.

Architecture:
  city_pulse_orchestrator (LlmAgent, root)
    ├── algorithm_agent  (LlmAgent, sub-agent) — NYC AI/algorithmic tools
    ├── lending_agent    (LlmAgent, sub-agent) — HMDA mortgage bias
    └── gig_agent        (LlmAgent, sub-agent) — delivery worker economics

The root agent uses ADK's built-in `transfer_to_agent` tool to delegate queries.
"""
import asyncio
import logging
import os
import re
from typing import Optional

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams
from google.genai import types

from agents.algorithm_agent import INSTRUCTION as ALGORITHM_INSTRUCTION
from agents.lending_agent import INSTRUCTION as LENDING_INSTRUCTION
from agents.gig_agent import INSTRUCTION as GIG_INSTRUCTION

log = logging.getLogger(__name__)

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001/mcp")

ORCHESTRATOR_INSTRUCTION = """You are City Pulse, an NYC civic data intelligence orchestrator.

You have three specialist sub-agents. Transfer to the correct one immediately:
- algorithm_agent: NYC city agency AI tools, algorithms, automated decision-making, surveillance, predictive software, machine learning, tech accountability
- lending_agent: Mortgage lending, HMDA data, racial bias in loans, denial rates, redlining, fair housing, credit, banks
- gig_agent: Delivery workers, gig economy, DoorDash, Uber Eats, Grubhub, Relay earnings, tips, expenses, wages, worker conditions, income

If the message includes [Intent: algorithm], transfer to algorithm_agent immediately.
If the message includes [Intent: lending], transfer to lending_agent immediately.
If the message includes [Intent: gig], transfer to gig_agent immediately.

Always transfer to a specialist — never answer directly yourself.
"""


def _classify_intent(text: str) -> str:
    """Keyword-based intent classification. Returns: lending | algorithm | gig | general."""
    t = text.lower()
    if re.search(r"mortgage|loan|lending|redlin|hmda|racial bias|approv|deni|credit|borrow|housing|bank|financ", t):
        return "lending"
    if re.search(r"algorithm|ai tool|automat|surveillance|decision|agency.*use|use.*ai|predict|software|tech|machine learn", t):
        return "algorithm"
    if re.search(r"deliver|gig|worker|earn|doordash|uber|grubhub|relay|wage|salary|expense|tip|hourly|pay|income|afford|how much", t):
        return "gig"
    return "general"


def _parse_response(text: str) -> dict:
    """Extract JSON {spoken, chart} from agent response text."""
    import json
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    try:
        data = json.loads(text)
        return {
            "spoken": data.get("spoken", "I found relevant NYC civic data."),
            "chart": data.get("chart"),
        }
    except (json.JSONDecodeError, ValueError):
        return {"spoken": text[:400] if text else "No data available.", "chart": None}


async def route_query(question: str, intent_hint: Optional[str] = None) -> dict:
    """
    Route the question to the appropriate sub-agent via ADK A2A protocol.
    Returns {spoken: str, chart: dict | None}.
    """
    # Prepend intent tag so the orchestrator LLM routes immediately without deliberation
    intent = intent_hint if intent_hint in ("lending", "algorithm", "gig") else _classify_intent(question)
    if intent and intent != "general":
        routed_question = f"[Intent: {intent}] {question}"
    else:
        routed_question = question

    log.info("A2A routing: intent=%s question=%r", intent, question[:80])

    # Each sub-agent gets its own MCP toolset connection
    algorithm_toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_SERVER_URL)
    )
    lending_toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_SERVER_URL)
    )
    gig_toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_SERVER_URL)
    )

    algorithm_agent = LlmAgent(
        name="algorithm_agent",
        model="gemini-3.1-flash-lite-preview",
        instruction=ALGORITHM_INSTRUCTION,
        tools=[algorithm_toolset],
    )
    lending_agent = LlmAgent(
        name="lending_agent",
        model="gemini-3.1-flash-lite-preview",
        instruction=LENDING_INSTRUCTION,
        tools=[lending_toolset],
    )
    gig_agent = LlmAgent(
        name="gig_agent",
        model="gemini-3.1-flash-lite-preview",
        instruction=GIG_INSTRUCTION,
        tools=[gig_toolset],
    )

    # Root orchestrator with ADK A2A sub_agents — routing is handled by the LLM
    orchestrator = LlmAgent(
        name="city_pulse_orchestrator",
        model="gemini-3.1-flash-lite-preview",
        instruction=ORCHESTRATOR_INSTRUCTION,
        sub_agents=[algorithm_agent, lending_agent, gig_agent],
    )

    session_service = InMemorySessionService()
    runner = Runner(
        app_name="city_pulse",
        agent=orchestrator,
        session_service=session_service,
    )
    session = await session_service.create_session(app_name="city_pulse", user_id="demo")

    final_text = ""
    try:
        async for event in runner.run_async(
            user_id="demo",
            session_id=session.id,
            new_message=types.Content(
                role="user",
                parts=[types.Part(text=routed_question)],
            ),
        ):
            if event.is_final_response() and event.content and event.content.parts:
                # In A2A, multiple final responses may fire (one per agent turn).
                # Overwrite each time — last one is the sub-agent's structured output.
                candidate = "".join(
                    p.text for p in event.content.parts if hasattr(p, "text") and p.text
                )
                if candidate:
                    final_text = candidate
                    log.debug("Final response candidate (author=%s): %s", getattr(event, 'author', '?'), candidate[:120])
    finally:
        for toolset in (algorithm_toolset, lending_toolset, gig_toolset):
            try:
                await toolset.close()
            except Exception:
                pass

    if not final_text:
        log.warning("No final response from orchestrator for intent=%s", intent)
        return {
            "spoken": "I'm having trouble retrieving that data right now. Please try again.",
            "chart": None,
        }

    return _parse_response(final_text)


async def _general_response(question: str) -> dict:
    """Lightweight response for general civic questions using Gemini Flash directly."""
    from google import genai

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    response = await client.aio.models.generate_content(
        model="gemini-3.1-flash-lite-preview",
        contents=question,
        config=types.GenerateContentConfig(
            system_instruction=(
                "You are City Pulse, a civic data assistant for NYC. "
                "Answer civic questions concisely in 2-3 sentences. "
                "Focus on NYC policy, data, and community impact."
            ),
            max_output_tokens=200,
        ),
    )
    return {
        "spoken": response.text or "I can help with questions about NYC algorithms, mortgage lending, and gig workers.",
        "chart": None,
    }
