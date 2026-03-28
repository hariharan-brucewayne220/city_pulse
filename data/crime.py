import httpx
from datetime import datetime, timedelta

ENDPOINT = "https://data.cityofnewyork.us/resource/5uac-w243.json"


async def query_crime(lat: float, lng: float) -> dict:
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
    params = {
        "$where": f"within_circle(geocoded_column, {lat}, {lng}, 500) AND rpt_dt > '{thirty_days_ago}'",
        "$limit": 50,
        "$select": "ofns_desc,pd_desc,boro_nm,rpt_dt",
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(ENDPOINT, params=params)
        resp.raise_for_status()
        records = resp.json()

    count = len(records)
    if count == 0:
        return {"value": "None", "count": 0, "detail": "No crimes reported in the last 30 days within 500m", "breakdown": []}

    offenses: dict[str, int] = {}
    for r in records:
        o = r.get("ofns_desc", "Unknown")
        offenses[o] = offenses.get(o, 0) + 1

    sorted_offenses = sorted(offenses.items(), key=lambda x: x[1], reverse=True)
    top_offense, top_count = sorted_offenses[0]
    top3 = " · ".join(f"{o} ({n})" for o, n in sorted_offenses[:3])
    breakdown = [{"type": o, "count": n} for o, n in sorted_offenses]

    return {"value": top_offense, "count": count, "detail": top3, "breakdown": breakdown}
