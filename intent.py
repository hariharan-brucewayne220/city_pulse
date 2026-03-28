import re

# Patterns that suggest Gemini identified a specific place
_PLACE_PATTERNS = [
    # "That looks like Joe's Pizza" / "This appears to be..."
    r"(?:looks like|appears to be|that(?:'s| is)|this is|i(?:'m| am) seeing)\s+([A-Z][^\.\,\!\?]{3,60})",
    # "I can see [Place Name]"
    r"(?:i can see|i see)\s+([A-Z][^\.\,\!\?]{3,60})",
    # "at [address]" — street address pattern
    r"\bat\s+(\d+\s+[A-Z][a-zA-Z\s]+(?:Street|St|Avenue|Ave|Blvd|Boulevard|Road|Rd|Place|Pl|Lane|Ln|Drive|Dr|Court|Ct|Way|Broadway|Park))\b",
    # Named places in quotes or title case
    r'"([^"]{3,60})"',
    # "the [Name] restaurant/building/store/cafe/deli/..."
    r"the\s+([A-Z][a-zA-Z\s\-\']{3,50})\s+(?:restaurant|cafe|deli|store|building|hotel|bar|shop|market|bakery|pharmacy|gym|park|station)",
]


def extract_location(gemini_text: str) -> str | None:
    """
    Try to extract a place name or address from Gemini's spoken response.
    Returns a string suitable for searching, or None if nothing found.
    """
    if not gemini_text:
        return None

    for pattern in _PLACE_PATTERNS:
        match = re.search(pattern, gemini_text, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            # Reject candidates that are too generic or too short
            generic = {"nyc", "new york", "the city", "this area", "the street", "the block"}
            if candidate.lower() not in generic and len(candidate) >= 4:
                return candidate

    return None
