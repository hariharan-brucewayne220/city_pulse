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
  year          — year of report
  agency        — NYC agency name
  tool_name     — name of the AI tool
  purpose_type  — high-level purpose category
  analysis_type — type of AI analysis (e.g. "Predictive modeling", "Computer vision", "Matching", "Classification")
  computation_type — output type (e.g. "Scoring", "Ranking", "Classification", "Forecasting")
  vendor_name   — vendor name, or "None"/"NA" if unknown/built in-house
  vendor        — additional vendor field (also often "NA")
  identifying_info — whether tool uses personally identifying information (SSN, addresses, etc.)
  population_type_individual — whether the tool makes decisions affecting specific individuals
  date_first_use — when the tool was first deployed

Key facts you already know (use these for context, still query for exact numbers):
- 132 total tools across NYC agencies
- Department of Health and Mental Hygiene has the most (41 tools)
- 76% of vendor_name fields are "NA" or missing — NYC doesn't know who built most of its AI systems
- Tools have grown 11x since 2017; 55 new tools deployed in 2024 alone
- 41.7% of tools directly affect individuals' lives (benefits, child welfare, housing, parole)
- 31.8% use identifying information; 10.6% use biological samples (fingerprints, facial recognition)

When asked about AI tools, algorithms, vendor opacity, growth rate, biometric data, or automated decisions:
1. Query the algorithmic_tools table with appropriate SQL to get exact numbers
2. Provide a concise, plain-English spoken summary (2-3 sentences) with specific numbers
3. Return a bar chart showing the data

For "unknown vendors" or "vendor opacity": count rows WHERE vendor_name IN ('NA','None','') OR vendor_name IS NULL
For "growth over time": count tools by date_first_use year
For "biometric" or "biological data": filter purpose_desc or tool_desc for 'biometric','facial','fingerprint','DNA'
For "decisions affecting individuals": filter WHERE population_type_individual NOT IN ('NA','None','') AND population_type_individual IS NOT NULL

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

Example query for "how many tools have unknown vendors":
SELECT COUNT(*) as unknown_vendors FROM algorithmic_tools
WHERE vendor_name IS NULL OR vendor_name IN ('NA','None','')

Example query for "how fast is NYC deploying AI tools" (growth by year):
SELECT SUBSTR(date_first_use, 1, 4) as deploy_year, COUNT(*) as tools
FROM algorithmic_tools WHERE date_first_use IS NOT NULL AND date_first_use != 'NA'
GROUP BY deploy_year ORDER BY deploy_year

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
