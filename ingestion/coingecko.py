# ingestion/coingecko.py
import requests
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional
import os

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")

COINS = [
    {"id": "bitcoin",  "symbol": "BTC"},
    {"id": "ethereum", "symbol": "ETH"},
    {"id": "binancecoin", "symbol": "BNB"},
    {"id": "solana",   "symbol": "SOL"},
    {"id": "cardano",  "symbol": "ADA"},
]


def fetch_historical_ohlcv(
    coin_id: str,
    vs_currency: str = "usd",
    days: int = 365,
) -> Optional[dict]:

    url    = f"{BASE_URL}/coins/{coin_id}/ohlc"
    params = {"vs_currency": vs_currency, "days": days}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        raw_data = response.json()

        # ── Log sample เพื่อ debug format ──────────────────
        if raw_data:
            logger.info(
                f"{coin_id} OHLCV sample: {raw_data}, "
                f"type: {type(raw_data)}"
            )

        return {
            "coin_id":        coin_id,
            "vs_currency":    vs_currency,
            "days_requested": days,
            "extracted_at":   datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"    # ← ไม่มี + ใน format
            ),
            "source": "coingecko",
            "data":   raw_data,
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch OHLCV for {coin_id}: {e}")
        return None


def fetch_market_data(coin_ids: list[str]) -> Optional[dict]:
    """
    Fetch current market data (market cap, volume, etc.)
    """
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        return {
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "coingecko",
            "data": response.json(),
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch market data: {e}")
        return None


def fetch_coin_metadata(coin_id: str) -> Optional[dict]:
    """
    Fetch static coin info for dim_coin.
    """
    url = f"{BASE_URL}/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        raw = response.json()

        return {
            "coin_id": coin_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "coingecko",
            "data": {
                "id": raw.get("id"),
                "symbol": raw.get("symbol", "").upper(),
                "name": raw.get("name"),
                "categories": raw.get("categories", []),
                "genesis_date": raw.get("genesis_date"),
                "description": raw.get("description", {}).get("en", "")[:500],
                "hashing_algorithm": raw.get("hashing_algorithm"),
            },
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch metadata for {coin_id}: {e}")
        return None
    finally:
        # CoinGecko Free tier rate limit = 30 calls/min
        time.sleep(2)