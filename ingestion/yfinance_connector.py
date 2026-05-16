# ingestion/yfinance_connector.py
import yfinance as yf
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

INDICES = {
    "^GSPC":  "SP500",
    "^IXIC":  "NASDAQ",
    "^DJI":   "DOW_JONES",
    "GC=F":   "GOLD_FUTURES",
}


def fetch_index_history(
    ticker: str,
    period: str = "1y",
    interval: str = "1d",
) -> Optional[dict]:
    """
    Fetch historical data for market indices.
    """
    try:
        tk = yf.Ticker(ticker)
        df = tk.history(period=period, interval=interval)

        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return None

        df = df.reset_index()
        df["Date"] = df["Date"].astype(str)

        return {
            "ticker": ticker,
            "name": INDICES.get(ticker, ticker),
            "period": period,
            "interval": interval,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "yahoo_finance",
            "data": df[["Date", "Open", "High", "Low", "Close", "Volume"]].to_dict(
                orient="records"
            ),
        }
    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None