"""
scraper.py — Data collection module
Fetches stock prices, crypto data, and weather data from public APIs.
No API key required for default sources.
"""

import requests
import pandas as pd
from datetime import datetime
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Stock / Finance ──────────────────────────────────────────────────────────

STOCK_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META"]

def fetch_stock_data(tickers: list[str] = STOCK_TICKERS) -> pd.DataFrame:
    """Fetch latest stock quotes from Yahoo Finance (no key needed)."""
    records = []
    for ticker in tickers:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            result = r.json()["chart"]["result"][0]
            meta   = result["meta"]
            records.append({
                "ticker":         ticker,
                "price":          round(meta.get("regularMarketPrice", 0), 2),
                "prev_close":     round(meta.get("chartPreviousClose", 0), 2),
                "currency":       meta.get("currency", "USD"),
                "exchange":       meta.get("exchangeName", ""),
                "fetched_at":     datetime.utcnow().isoformat(),
            })
            logger.info(f"  ✓ {ticker}: ${records[-1]['price']}")
            time.sleep(0.3)          # polite rate-limit
        except Exception as e:
            logger.warning(f"  ✗ {ticker}: {e}")
    df = pd.DataFrame(records)
    if not df.empty:
        df["change"]  = df["price"] - df["prev_close"]
        df["pct_chg"] = ((df["change"] / df["prev_close"]) * 100).round(2)
    return df


# ── Crypto ───────────────────────────────────────────────────────────────────

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]

def fetch_crypto_data(coin_ids: list[str] = CRYPTO_IDS) -> pd.DataFrame:
    """Fetch crypto prices from CoinGecko public API (no key needed, 10–30 req/min)."""
    ids_str = ",".join(coin_ids)
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        f"?vs_currency=usd&ids={ids_str}&order=market_cap_desc"
        "&sparkline=false&price_change_percentage=24h"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data)[[
            "id", "symbol", "name", "current_price",
            "market_cap", "price_change_percentage_24h",
            "total_volume", "high_24h", "low_24h",
        ]].rename(columns={
            "id":                           "coin_id",
            "current_price":                "price_usd",
            "price_change_percentage_24h":  "pct_chg_24h",
            "total_volume":                 "volume_24h",
        })
        df["fetched_at"] = datetime.utcnow().isoformat()
        df["pct_chg_24h"] = df["pct_chg_24h"].round(2)
        logger.info(f"  ✓ Fetched {len(df)} crypto records")
        return df
    except Exception as e:
        logger.error(f"  ✗ Crypto fetch failed: {e}")
        return pd.DataFrame()


# ── Weather ──────────────────────────────────────────────────────────────────

CITIES = [
    {"name": "New York",    "lat": 40.71, "lon": -74.01},
    {"name": "London",      "lat": 51.51, "lon": -0.13},
    {"name": "Tokyo",       "lat": 35.69, "lon": 139.69},
    {"name": "Sydney",      "lat": -33.87, "lon": 151.21},
    {"name": "Berlin",      "lat": 52.52, "lon": 13.40},
    {"name": "São Paulo",   "lat": -23.55, "lon": -46.63},
]

def fetch_weather_data(cities: list[dict] = CITIES) -> pd.DataFrame:
    """Fetch current weather from Open-Meteo (completely free, no key)."""
    records = []
    for city in cities:
        try:
            url = (
                "https://api.open-meteo.com/v1/forecast"
                f"?latitude={city['lat']}&longitude={city['lon']}"
                "&current_weather=true&hourly=relativehumidity_2m,windspeed_10m"
            )
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            cw = r.json()["current_weather"]
            records.append({
                "city":         city["name"],
                "temp_c":       cw["temperature"],
                "windspeed":    cw["windspeed"],
                "weathercode":  cw["weathercode"],
                "lat":          city["lat"],
                "lon":          city["lon"],
                "fetched_at":   datetime.utcnow().isoformat(),
            })
            logger.info(f"  ✓ {city['name']}: {cw['temperature']}°C")
            time.sleep(0.2)
        except Exception as e:
            logger.warning(f"  ✗ {city['name']}: {e}")
    return pd.DataFrame(records)


# ── Orchestrator ─────────────────────────────────────────────────────────────

def run_all() -> dict[str, pd.DataFrame]:
    """Run all scrapers and return a dict of DataFrames."""
    logger.info("═══ Starting data collection ═══")

    logger.info("▸ Stocks")
    stocks = fetch_stock_data()

    logger.info("▸ Crypto")
    crypto = fetch_crypto_data()

    logger.info("▸ Weather")
    weather = fetch_weather_data()

    logger.info("═══ Collection complete ═══")
    return {"stocks": stocks, "crypto": crypto, "weather": weather}


if __name__ == "__main__":
    results = run_all()
    for name, df in results.items():
        print(f"\n── {name.upper()} ──")
        print(df.to_string(index=False))
