import hashlib

def make_event_id(payload: dict, topic: str) -> str:
    # Stable event id from key fields (edit keys to match your payload)
    key = f"{topic}|{payload.get('PriceArea')}|{payload.get('HourUTC')}|{payload.get('HourDK')}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def validate_and_normalize(payload: dict, topic: str):
    """
    Returns: (clean_payload, reason_if_invalid)
    clean_payload: dict
    reason_if_invalid: str
    """
    # Required fields per topic
    if topic == "elspot_topic":
        required = ["PriceArea", "SpotPriceDKK", "HourUTC"]
    elif topic == "declaration_topic":
        required = ["PriceArea", "HourUTC"]
    else:
        return None, f"Unknown topic: {topic}"

    for k in required:
        if payload.get(k) in (None, "", []):
            return None, f"Missing field: {k}"

    # Convert price or production to float
    if topic == "elspot_topic":
        value_key = "SpotPriceDKK"
        try:
            value = float(payload[value_key])
        except Exception:
            return None, f"{value_key} is not a number"

        # Sanity range (adjust if needed)
        if value < -5000 or value > 50000:
            return None, f"{value_key} out of range: {value}"

        clean = dict(payload)
        clean["SpotPriceDKK"] = value
    else:
        # For declaration, no value check
        clean = dict(payload)

    clean["event_id"] = make_event_id(clean, topic)

    return clean, None
