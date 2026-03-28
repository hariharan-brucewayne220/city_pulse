"""
City Pulse MCP Server — exposes query_dataset tool.
Runs on streamable-http transport (port 8001) for ADK agent integration.

Run standalone: python mcp_server/server.py
ADK agents connect via: StreamableHTTPConnectionParams(url='http://localhost:8001/mcp')
"""
import sys
import os
import argparse

# Ensure project root is on path when spawned as subprocess
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastmcp import FastMCP
from mcp_server.init_db import init_database, TABLES

mcp = FastMCP("city-pulse-data")

# Initialize DuckDB once at startup
_conn = init_database()

ALLOWED_TABLES = set(TABLES.keys())


@mcp.tool()
def query_dataset(dataset_name: str, sql: str) -> dict:
    """
    Query a pre-cached NYC civic dataset using SQL.

    Available datasets:
    - algorithmic_tools: NYC agency AI/algorithmic tools (columns: year, agency, tool_name, tool_desc, purpose_type, vendor_name, vendor)
    - hmda_nyc: NYC mortgage loan applications (columns: county_code, borough, action_taken, applicant_race, loan_amount, income, denial_reason_1)
    - delivery_workers: NYC gig delivery worker earnings (columns: platform, borough, vehicle_type, hours_per_week, gross_hourly_earnings, net_hourly_earnings)
    - ppp_nyc: SBA PPP loans for NYC (columns: borough, loan_amount, jobs_retained, race_ethnicity, fully_forgiven, business_type)

    Args:
        dataset_name: One of the dataset names listed above
        sql: A SELECT SQL query (no INSERT/UPDATE/DELETE/DROP allowed)

    Returns:
        dict with columns (list), rows (list of lists), row_count (int)
    """
    if dataset_name not in ALLOWED_TABLES:
        return {
            "error": f"Unknown dataset '{dataset_name}'. Available: {sorted(ALLOWED_TABLES)}",
            "columns": [], "rows": [], "row_count": 0,
        }

    sql_stripped = sql.strip().upper()
    if not sql_stripped.startswith("SELECT"):
        return {"error": "Only SELECT queries allowed.", "columns": [], "rows": [], "row_count": 0}

    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"):
        if forbidden in sql_stripped:
            return {"error": f"'{forbidden}' not allowed.", "columns": [], "rows": [], "row_count": 0}

    try:
        result = _conn.execute(sql).fetchall()
        cols = [desc[0] for desc in _conn.description]
        rows = [list(row) for row in result]
        return {"columns": cols, "rows": rows, "row_count": len(rows)}
    except Exception as e:
        return {"error": str(e), "columns": [], "rows": [], "row_count": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--transport", default="streamable-http", choices=["streamable-http", "sse", "stdio"])
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        print(f"City Pulse MCP server starting on port {args.port} ({args.transport})")
        mcp.run(transport=args.transport, port=args.port)
