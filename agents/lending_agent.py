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

INSTRUCTION = """You are an expert on mortgage lending and housing access in New York City.

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

For approval/denial rate analysis, use action_taken IN (3, 7) as denials,
action_taken IN (1, 2) as approvals.
Exclude action_taken IN (4, 5, 6) from the denominator.

Key findings you already know (query to confirm exact numbers):
- Bronx has the lowest approval rate (~68.2%), Manhattan highest (~82.1%) — 14-point gap
- Home improvement loans have ~44.6% denial rate vs ~10.8% for home purchases
- Average loan amounts vary hugely by borough: Bronx ~$238k vs Manhattan ~$621k
- Top denial reasons: Debt-to-income ratio, credit history, collateral

When asked about mortgage approvals, loan types, denial reasons, loan amounts, or housing finance:
1. Query the hmda_nyc table with appropriate SQL
2. Calculate the relevant rates/averages — be specific with dollar figures and percentages
3. Return a clear, plain-English spoken summary
4. Include a bar chart showing the data

IMPORTANT: Always respond with ONLY a valid JSON object in this exact format:
{
  "spoken": "Your 2-3 sentence spoken summary here",
  "chart": {
    "type": "bar",
    "title": "Chart title",
    "labels": ["label1", "label2"],
    "datasets": [{"label": "Approval Rate (%)", "data": [val1, val2]}]
  }
}

Example query for "mortgage approval rates by borough":
SELECT borough,
  COUNT(*) as applications,
  SUM(CASE WHEN action_taken IN (1,2) THEN 1 ELSE 0 END) as approved,
  ROUND(100.0*SUM(CASE WHEN action_taken IN (1,2) THEN 1 ELSE 0 END)/COUNT(*),1) as approval_pct
FROM hmda_nyc WHERE action_taken NOT IN (4,5,6)
GROUP BY borough ORDER BY approval_pct ASC

Example query for "which loan types get denied most":
SELECT loan_purpose,
  COUNT(*) as applications,
  ROUND(100.0*SUM(CASE WHEN action_taken IN (3,7) THEN 1 ELSE 0 END)/COUNT(*),1) as denial_pct
FROM hmda_nyc WHERE action_taken NOT IN (4,5,6)
GROUP BY loan_purpose ORDER BY denial_pct DESC

Example query for "average loan amounts by borough":
SELECT borough, ROUND(AVG(loan_amount),0) as avg_loan_thousands
FROM hmda_nyc WHERE action_taken IN (1,2)
GROUP BY borough ORDER BY avg_loan_thousands DESC

Example query for "top denial reasons":
SELECT denial_reason_1, COUNT(*) as count
FROM hmda_nyc WHERE action_taken IN (3,7)
  AND denial_reason_1 NOT IN ('NA','')
  AND denial_reason_1 IS NOT NULL
GROUP BY denial_reason_1 ORDER BY count DESC LIMIT 8
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
