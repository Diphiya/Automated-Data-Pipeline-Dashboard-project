"""
processor.py — Data cleaning & transformation layer
Validates, normalises, and enriches raw DataFrames before storage.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


# ── Weather-code → description lookup ────────────────────────────────────────
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm + hail", 99: "Thunderstorm + heavy hail",
}


# ── Stocks ────────────────────────────────────────────────────────────────────

def clean_stocks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Stocks DataFrame is empty – skipping clean.")
        return df

    original_len = len(df)

    # Drop rows with missing price
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]

    # Clip unrealistic percentage changes (±50% in a single session)
    df["pct_chg"] = df["pct_chg"].clip(-50, 50)

    # Sentiment label
    df["sentiment"] = np.where(
        df["pct_chg"] > 1,  "🟢 Bullish",
        np.where(df["pct_chg"] < -1, "🔴 Bearish", "🟡 Neutral")
    )

    df["ticker"] = df["ticker"].str.upper().str.strip()
    df["price"]  = df["price"].round(2)

    logger.info(f"Stocks cleaned: {original_len} → {len(df)} rows")
    return df.reset_index(drop=True)


# ── Crypto ────────────────────────────────────────────────────────────────────

def clean_crypto(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Crypto DataFrame is empty – skipping clean.")
        return df

    original_len = len(df)

    df = df.dropna(subset=["price_usd"])
    df = df[df["price_usd"] > 0]

    # Format market cap into readable tiers
    def cap_tier(mc):
        if pd.isna(mc):       return "Unknown"
        if mc >= 1e11:        return "Mega Cap (>$100B)"
        if mc >= 1e10:        return "Large Cap ($10B–$100B)"
        if mc >= 1e9:         return "Mid Cap ($1B–$10B)"
        return "Small Cap (<$1B)"

    df["cap_tier"]   = df["market_cap"].apply(cap_tier)
    df["symbol"]     = df["symbol"].str.upper().str.strip()
    df["price_usd"]  = df["price_usd"].round(4)
    df["pct_chg_24h"] = df["pct_chg_24h"].round(2)

    # Sentiment
    df["sentiment"] = np.where(
        df["pct_chg_24h"] > 2,  "🟢 Bullish",
        np.where(df["pct_chg_24h"] < -2, "🔴 Bearish", "🟡 Neutral")
    )

    # Remove cap_tier (not in DB schema) before storing — keep it only for display
    # We add it back after load from DB via clean_crypto_for_display()

    logger.info(f"Crypto cleaned: {original_len} → {len(df)} rows")
    return df.reset_index(drop=True)


# ── Weather ───────────────────────────────────────────────────────────────────

def clean_weather(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Weather DataFrame is empty – skipping clean.")
        return df

    original_len = len(df)

    df = df.dropna(subset=["temp_c"])

    # Clip absurd temperatures
    df["temp_c"]    = df["temp_c"].clip(-80, 60)
    df["windspeed"] = df["windspeed"].clip(0, 400)

    # Human-readable weather description
    df["condition"]  = df["weathercode"].map(WMO_CODES).fillna("Unknown")
    df["temp_f"]     = (df["temp_c"] * 9 / 5 + 32).round(1)
    df["feels_like"] = _feels_like(df["temp_c"], df["windspeed"])

    logger.info(f"Weather cleaned: {original_len} → {len(df)} rows")
    return df.reset_index(drop=True)


def _feels_like(temp_c: pd.Series, wind_kph: pd.Series) -> pd.Series:
    """Wind-chill formula (valid for temp < 10°C and wind > 4.8 kph)."""
    wc = (
        13.12
        + 0.6215 * temp_c
        - 11.37 * (wind_kph ** 0.16)
        + 0.3965 * temp_c * (wind_kph ** 0.16)
    ).round(1)
    # Use wind-chill only where conditions apply, else use actual temp
    return pd.Series(
        [wc.iloc[i] if (temp_c.iloc[i] <= 10 and wind_kph.iloc[i] >= 4.8) else temp_c.iloc[i]
         for i in range(len(temp_c))],
        index=temp_c.index
    )


# ── Orchestrator ──────────────────────────────────────────────────────────────

def process_all(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {
        "stocks":  clean_stocks(raw.get("stocks",  pd.DataFrame())),
        "crypto":  clean_crypto(raw.get("crypto",  pd.DataFrame())),
        "weather": clean_weather(raw.get("weather", pd.DataFrame())),
    }


if __name__ == "__main__":
    import scraper
    raw = scraper.run_all()
    cleaned = process_all(raw)
    for name, df in cleaned.items():
        print(f"\n── {name.upper()} (cleaned) ──")
        print(df.to_string(index=False))
