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
- tips_per_hour = customer tips per hour
- earnings_per_hour = pay_per_hour + tips_per_hour (total gross hourly)
- avg_earnings_per_qtr = average quarterly earnings per worker
- deliveries_per_hour = how many deliveries per hour (workload intensity)
- NYC minimum pay rule: Jan 2023 at $17.96/hr → Jul 2023 at $19.56/hr → Apr 2024 at $21.44/hr
- min_pay_status = "Pre-minimum pay rule" or "Post-minimum pay rule"

Key findings you already know (query to confirm exact numbers):
- After the wage law, base pay rose 197% but tips COLLAPSED 52.7% (from $6.32/hr to ~$2.99/hr)
- Total workers DROPPED by ~36,840 (-36.8%) after the law — workers were pushed off the apps
- Remaining workers now do 60% more deliveries per hour (increased pace/exploitation)
- Net earnings_per_hour still rose ~79% overall, but the worker pool shrank dramatically

When asked about delivery worker pay, tips, the wage law impact, or worker counts:
1. Query the delivery_workers table with SQL to get exact numbers
2. Tell the REAL story: app pay went up, but tips collapsed and 37,000 workers were displaced
3. Return a plain-English spoken summary with specific dollar figures
4. Include a chart — use line chart for trends over time, bar chart for before/after comparisons

IMPORTANT: Always respond with ONLY a valid JSON object in this exact format:
{
  "spoken": "Your 2-3 sentence spoken summary here",
  "chart": {
    "type": "line",
    "title": "Chart title",
    "labels": ["Q1 2022", "Q2 2022", ...],
    "datasets": [
      {"label": "Total Pay ($/hr)", "data": [val1, val2, ...]},
      {"label": "Tips ($/hr)", "data": [val1, val2, ...]}
    ]
  }
}

Example query for "did the wage law help workers" (before vs after comparison):
SELECT min_pay_status,
  ROUND(AVG(pay_per_hour),2) as avg_base_pay,
  ROUND(AVG(tips_per_hour),2) as avg_tips,
  ROUND(AVG(earnings_per_hour),2) as avg_total,
  ROUND(AVG(total_workers),0) as avg_workers,
  ROUND(AVG(deliveries_per_hour),2) as avg_deliveries_per_hr
FROM delivery_workers GROUP BY min_pay_status

Example query for "full pay trend over time":
SELECT quarter, year, pay_per_hour, tips_per_hour, earnings_per_hour, total_workers
FROM delivery_workers ORDER BY year, quarter
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
