# ingestion/fear_greed.py
import requests
import logging
from datetime import datetime, timezone
from typing import Optional
import os

logger = logging.getLogger(__name__)

FEAR_GREED_URL = os.getenv(
    "FEAR_GREED_URL",
    "https://api.alternative.me/fng/"
)


def fetch_fear_greed_index(limit: int = 365) -> Optional[dict]:
    """
    Fetch Fear & Greed Index historical data.
    limit=0 returns all available history.
    """
    params = {"limit": limit, "format": "json"}

    try:
        response = requests.get(FEAR_GREED_URL, params=params, timeout=30)
        response.raise_for_status()
        raw = response.json()

        return {
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "alternative.me",
            "data": raw.get("data", []),
            # [{"value": "72", "value_classification": "Greed",
            #   "timestamp": "1234567890", "time_until_update": "..."}, ...]
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch Fear & Greed Index: {e}")
        return None