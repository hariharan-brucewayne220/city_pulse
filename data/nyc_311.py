import httpx
from datetime import datetime, timedelta

ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


async def query_311(lat: float, lng: float) -> dict:
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
    params = {
        "$where": f"within_circle(location, {lat}, {lng}, 500) AND created_date > '{thirty_days_ago}'",
        "$limit": 50,
        "$select": "complaint_type,descriptor,created_date,incident_address",
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(ENDPOINT, params=params)
        resp.raise_for_status()
        records = resp.json()

    count = len(records)
    if count == 0:
        return {"value": "None", "count": 0, "detail": "No 311 complaints in the last 30 days within 500m", "breakdown": []}

    # Tally by complaint type
    types: dict[str, int] = {}
    for r in records:
        t = r.get("complaint_type", "Unknown")
        types[t] = types.get(t, 0) + 1

    sorted_types = sorted(types.items(), key=lambda x: x[1], reverse=True)
    top_type, top_count = sorted_types[0]

    # Top 3 as summary line
    top3 = " · ".join(f"{t} ({n})" for t, n in sorted_types[:3])
    breakdown = [{"type": t, "count": n} for t, n in sorted_types]

    return {"value": top_type, "count": count, "detail": top3, "breakdown": breakdown}
