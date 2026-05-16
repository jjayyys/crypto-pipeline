# ingestion/yfinance_connector.py
import requests
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

INDICES = {
    '^SPX': 'SP500',
    '^NDX': 'NASDAQ100',
    '^DJI': 'DOW_JONES',
    'GC.F': 'GOLD_FUTURES',
}


def fetch_index_history(
    ticker: str,
    period: str = "1y",
    interval: str = "1d",
) -> Optional[dict]:
    """
    ดึง market index จาก stooq.com
    (ทำงานได้ใน Docker ไม่มี rate limit)
    """
    url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()

        lines = resp.text.strip().split('\n')
        if len(lines) < 2:
            logger.warning(f"No data for {ticker}")
            return None

        rows = []
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) >= 5:
                rows.append({
                    'Date':   parts,
                    'Open':   float(parts) if parts else None,
                    'High':   float(parts) if parts else None,
                    'Low':    float(parts) if parts else None,
                    'Close':  float(parts) if parts else None,
                    'Volume': float(parts) if len(parts) > 5
                              and parts else None,
                })

        # เอาแค่ 365 วันล่าสุด
        rows = rows[-365:] if len(rows) > 365 else rows

        return {
            'ticker':       ticker,
            'name':         INDICES.get(ticker, ticker),
            'period':       period,
            'extracted_at': datetime.now(timezone.utc).strftime(
                '%Y-%m-%dT%H:%M:%SZ'
            ),
            'source': 'stooq',
            'data':   rows,
        }

    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None