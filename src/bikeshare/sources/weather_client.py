"""Open-Meteo archive API client (free, no API key) + the mock used for offline/deterministic runs."""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from datetime import date

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "relative_humidity_2m", "weather_code"]


def fetch_open_meteo(lat: float, lon: float, start: date, end: date, tz: str = "Australia/Melbourne",
                     retries: int = 4, timeout: int = 30) -> dict:
    """GET hourly history with exponential back-off on 429/5xx. Returns the raw JSON payload unchanged."""
    qs = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon, "start_date": start.isoformat(), "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS), "timezone": tz,
    })
    url = f"{ARCHIVE_URL}?{qs}"
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Open-Meteo request failed after {retries} attempts: {last}")
