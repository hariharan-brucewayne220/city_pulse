"""
AlgorithmAgent — ADK sub-agent for NYC algorithmic tools and AI accountability.
Connects to MCP server for DuckDB-backed queries.
"""
import json
import os
import asyncio
from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001/mcp")

INSTRUCTION = """You are an expert on NYC's algorithmic accountability and AI transparency policy.

You have access to the real NYC Algorithmic Tools dataset (NYC Open Data) via the query_dataset tool.

Table: algorithmic_tools
Key columns:
  year          — year of report (2022)
  agency        — NYC agency name (e.g. "New York Police Department", "Administration for Children's Services")
  tool_name     — name of the AI tool
  purpose_type  — high-level purpose category
  analysis_type — type of AI analysis (e.g. "Predictive modeling", "Computer vision", "Matching", "Classification")
  computation_type — output type (e.g. "Scoring", "Ranking", "Classification", "Forecasting")
  vendor_name   — vendor or "None" if built in-house
  identifying_info — whether tool uses personally identifying information

Notable agencies with tools: Department of Health and Mental Hygiene (41 tools), NYC Public Schools (15),
Mayor's Office (14), Fire Department (13), Administration for Children's Services (11), NYPD (10).

When asked about AI tools, algorithms, or automated decision-making used by NYC agencies:
1. Query the algorithmic_tools table with appropriate SQL
2. Provide a concise, plain-English spoken summary (2-3 sentences) with specific numbers
3. Return a bar chart showing the data

IMPORTANT: Always respond with ONLY a valid JSON object in this exact format:
{
  "spoken": "Your 2-3 sentence spoken summary here",
  "chart": {
    "type": "bar",
    "title": "Chart title",
    "labels": ["label1", "label2"],
    "datasets": [{"label": "Dataset label", "data": [val1, val2]}]
  }
}

Example query for "which agencies use the most AI tools":
SELECT agency, COUNT(*) as tool_count FROM algorithmic_tools GROUP BY agency ORDER BY tool_count DESC LIMIT 10

Example query for "what types of AI analysis does NYC use":
SELECT analysis_type, COUNT(*) as count FROM algorithmic_tools
WHERE analysis_type != 'NA' GROUP BY analysis_type ORDER BY count DESC

Example query for "does NYPD use predictive policing":
SELECT tool_name, purpose_type, analysis_type, computation_type, vendor_name
FROM algorithmic_tools WHERE agency = 'New York Police Department'

Return null for chart only if no data is available.
"""


async def ask(question: str) -> dict:
    """Query the algorithm agent and return {spoken, chart}."""
    from google.genai import types

    toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_SERVER_URL)
    )
    agent = LlmAgent(
        name="algorithm_agent",
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
    """Extract JSON from agent response."""
    text = text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    try:
        data = json.loads(text)
        return {
            "spoken": data.get("spoken", "I found some data about NYC algorithmic tools."),
            "chart": data.get("chart"),
        }
    except json.JSONDecodeError:
        # Fallback: return text as spoken with no chart
        return {"spoken": text[:400] if text else "No data available.", "chart": None}
