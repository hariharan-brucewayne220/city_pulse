import httpx

ENDPOINT = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "Z": 3, "P": 4}


async def query_restaurants(lat: float, lng: float) -> dict:
    params = {
        "$where": f"within_circle(location, {lat}, {lng}, 400)",
        "$limit": 50,
        "$select": "dba,grade,score,cuisine_description,building,street",
        "$order": "score ASC",
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(ENDPOINT, params=params)
        resp.raise_for_status()
        records = resp.json()

    graded = [r for r in records if r.get("grade")]
    if not graded:
        return {"value": "N/A", "count": 0, "detail": "No graded restaurants within 400m", "breakdown": [], "restaurants": []}

    # Deduplicate by name, keep best grade per restaurant
    seen: dict[str, dict] = {}
    for r in graded:
        name = r.get("dba", "").strip().title()
        if not name:
            continue
        existing = seen.get(name)
        if not existing or GRADE_ORDER.get(r.get("grade", "Z"), 99) < GRADE_ORDER.get(existing.get("grade", "Z"), 99):
            seen[name] = r

    restaurants = sorted(seen.values(), key=lambda r: GRADE_ORDER.get(r.get("grade", "Z"), 99))

    top_grade = restaurants[0].get("grade", "?")
    count = len(restaurants)

    # Summary detail
    grade_counts: dict[str, int] = {}
    for r in restaurants:
        g = r.get("grade", "?")
        grade_counts[g] = grade_counts.get(g, 0) + 1
    detail = " · ".join(f"Grade {g}: {n}" for g, n in sorted(grade_counts.items(), key=lambda x: GRADE_ORDER.get(x[0], 99)))

    # Individual restaurant list for dynamic cards
    restaurant_list = [
        {
            "name": r.get("dba", "").strip().title(),
            "grade": r.get("grade", "?"),
            "score": r.get("score", ""),
            "cuisine": r.get("cuisine_description", "").title(),
            "address": f"{r.get('building', '')} {r.get('street', '')}".strip().title(),
        }
        for r in restaurants[:10]
    ]

    return {
        "value": top_grade,
        "count": count,
        "detail": detail,
        "breakdown": [],
        "restaurants": restaurant_list,
    }
