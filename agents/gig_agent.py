"""
GigAgent — ADK sub-agent for delivery worker economics and gig economy analysis.
"""
import json
import os
from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001/mcp")

INSTRUCTION = """You are an expert on NYC gig economy workers, especially app-based food delivery workers.

You have access to real NYC DCWP (Dept of Consumer & Worker Protection) quarterly data via the query_dataset tool.

Table: delivery_workers
Columns: quarter, year, total_workers, avg_hours_per_qtr, earnings_per_hour,
         pay_per_hour, tips_per_hour, deliveries_per_hour, avg_earnings_per_qtr, min_pay_status

Key facts:
- pay_per_hour = base app pay (what the app pays, not counting tips)
- tips_per_hour = customer tips
- earnings_per_hour = pay_per_hour + tips_per_hour (total gross)
- NYC minimum pay rule was enacted Jan 2023 at $17.96/hr, raised to $19.56 (Jul 2023), then $21.44 (Apr 2024)
- min_pay_status shows whether the minimum pay rule was in effect for that quarter

When asked about delivery worker earnings, wages, or working conditions:
1. Query the delivery_workers table with appropriate SQL
2. Highlight the before/after impact of the minimum pay rule
3. Return a plain-English spoken summary with specific dollar figures
4. Include a line or bar chart showing pay trends over time

IMPORTANT: Always respond with ONLY a valid JSON object in this exact format:
{
  "spoken": "Your 2-3 sentence spoken summary here",
  "chart": {
    "type": "bar",
    "title": "Chart title",
    "labels": ["Q1 2022", "Q2 2022", ...],
    "datasets": [
      {"label": "Pay per Hour ($)", "data": [val1, val2, ...]},
      {"label": "Tips per Hour ($)", "data": [val1, val2, ...]}
    ]
  }
}

Example query for "how has pay changed over time":
SELECT quarter, pay_per_hour, tips_per_hour, earnings_per_hour, min_pay_status
FROM delivery_workers ORDER BY year, quarter

Example query for "current earnings":
SELECT quarter, earnings_per_hour, pay_per_hour, tips_per_hour
FROM delivery_workers ORDER BY year DESC, quarter DESC LIMIT 1
"""


async def ask(question: str) -> dict:
    """Query the gig agent and return {spoken, chart}."""
    from google.genai import types

    toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_SERVER_URL)
    )
    agent = LlmAgent(
        name="gig_agent",
        model="gemini-3.1-flash-lite-preview",
        instruction=INSTRUCTION,
        tools=[toolset],
    )
    session_service = InMemorySessionService()
    runner = Runner(app_name="city_pulse", agent=agent, session_service=session_service)
    session = await session_service.create_session(app_name="city_pulse", user_id="demo")

    final_text = ""
    try:
        async for event in runner.run_async(
            user_id="demo",
            session_id=session.id,
            new_message=types.Content(role="user", parts=[types.Part(text=question)]),
        ):
            if event.is_final_response() and event.content and event.content.parts:
                for part in event.content.parts:
                    if hasattr(part, "text") and part.text:
                        final_text += part.text
    finally:
        await toolset.close()

    return _parse_response(final_text)


def _parse_response(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    try:
        data = json.loads(text)
        return {
            "spoken": data.get("spoken", "I found delivery worker earnings data for NYC."),
            "chart": data.get("chart"),
        }
    except json.JSONDecodeError:
        return {"spoken": text[:400] if text else "No data available.", "chart": None}
