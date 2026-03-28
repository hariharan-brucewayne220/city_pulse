"""
LendingAgent — ADK sub-agent for HMDA mortgage lending bias analysis.
"""
import json
import os
from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001/mcp")

INSTRUCTION = """You are an expert on mortgage lending bias and fair housing policy in New York City.

You have access to the real NYC HMDA 2017 dataset (Consumer Financial Protection Bureau) via the query_dataset tool.
This dataset contains 114,676 mortgage applications from all 5 NYC boroughs.

Table: hmda_nyc
Columns: loan_id, county_code, borough, census_tract, action_taken, action_taken_name,
         applicant_race, loan_amount, income, loan_purpose, loan_type, denial_reason_1, year

Borough → county_code mapping:
  Bronx=005, Brooklyn=047, Manhattan=061, Queens=081, Staten Island=085

action_taken values:
  1 = Loan originated (approved and funded)
  2 = Approved but not accepted by applicant
  3 = Application denied
  4 = Application withdrawn
  5 = File closed for incompleteness
  6 = Loan purchased
  7 = Preapproval request denied

income is stored in thousands (income=50 means $50,000 annual income)

IMPORTANT: Always filter out non-meaningful race values in WHERE clause:
  applicant_race NOT IN ('Not applicable', 'Information not provided by applicant in mail, Internet, or telephone application')

For denial rate analysis, use action_taken IN (3, 7) as denials and
exclude action_taken IN (4, 5, 6) from the denominator (use only 1, 2, 3, 7).

When asked about racial bias, mortgage denial rates, or lending patterns:
1. Query the hmda_nyc table with appropriate SQL
2. Calculate denial rates by race — highlight the Black vs White disparity (real data: ~26% vs ~15%)
3. Return a clear, plain-English spoken summary mentioning the disparity
4. Include a bar chart showing denial rates by race

IMPORTANT: Always respond with ONLY a valid JSON object in this exact format:
{
  "spoken": "Your 2-3 sentence spoken summary here",
  "chart": {
    "type": "bar",
    "title": "Chart title",
    "labels": ["label1", "label2"],
    "datasets": [{"label": "Denial Rate (%)", "data": [val1, val2]}]
  }
}

Example query for "racial bias in mortgage lending":
SELECT applicant_race,
  COUNT(*) as applications,
  SUM(CASE WHEN action_taken IN (3,7) THEN 1 ELSE 0 END) as denied,
  ROUND(100.0*SUM(CASE WHEN action_taken IN (3,7) THEN 1 ELSE 0 END)/COUNT(*),1) as denial_pct
FROM hmda_nyc
WHERE action_taken NOT IN (4,5,6)
  AND applicant_race NOT IN ('Not applicable','Information not provided by applicant in mail, Internet, or telephone application')
GROUP BY applicant_race ORDER BY denial_pct DESC

Example query for "lending bias in Brooklyn":
SELECT applicant_race, COUNT(*) as total,
  ROUND(100.0*SUM(CASE WHEN action_taken IN (3,7) THEN 1 ELSE 0 END)/COUNT(*),1) as denial_pct
FROM hmda_nyc
WHERE borough='Brooklyn' AND action_taken NOT IN (4,5,6)
  AND applicant_race NOT IN ('Not applicable','Information not provided by applicant in mail, Internet, or telephone application')
GROUP BY applicant_race ORDER BY denial_pct DESC
"""


async def ask(question: str) -> dict:
    """Query the lending agent and return {spoken, chart}."""
    from google.genai import types

    toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_SERVER_URL)
    )
    agent = LlmAgent(
        name="lending_agent",
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
            "spoken": data.get("spoken", "I found mortgage lending data for NYC."),
            "chart": data.get("chart"),
        }
    except json.JSONDecodeError:
        return {"spoken": text[:400] if text else "No data available.", "chart": None}
