"""
tests/test_processor.py — Unit tests for the cleaning layer.
Run: pytest tests/ -v
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
import pytest
from processor import clean_stocks, clean_crypto, clean_weather


# ── Stocks ────────────────────────────────────────────────────────────────────

class TestCleanStocks:
    def _make(self, **overrides):
        base = dict(
            ticker="AAPL", price=175.0, prev_close=172.0,
            change=3.0, pct_chg=1.74, currency="USD",
            exchange="NASDAQ", fetched_at="2024-01-01T12:00:00",
        )
        base.update(overrides)
        return pd.DataFrame([base])

    def test_valid_row_passes_through(self):
        df = clean_stocks(self._make())
        assert len(df) == 1
        assert df.iloc[0]["price"] == 175.0

    def test_zero_price_dropped(self):
        df = clean_stocks(self._make(price=0))
        assert df.empty

    def test_negative_price_dropped(self):
        df = clean_stocks(self._make(price=-5))
        assert df.empty

    def test_pct_chg_clipped(self):
        df = clean_stocks(self._make(pct_chg=200))
        assert df.iloc[0]["pct_chg"] == 50

    def test_sentiment_bullish(self):
        df = clean_stocks(self._make(pct_chg=2.5))
        assert "Bullish" in df.iloc[0]["sentiment"]

    def test_sentiment_bearish(self):
        df = clean_stocks(self._make(pct_chg=-2.5))
        assert "Bearish" in df.iloc[0]["sentiment"]

    def test_sentiment_neutral(self):
        df = clean_stocks(self._make(pct_chg=0.5))
        assert "Neutral" in df.iloc[0]["sentiment"]

    def test_ticker_normalised(self):
        df = clean_stocks(self._make(ticker=" aapl "))
        assert df.iloc[0]["ticker"] == "AAPL"

    def test_empty_df_returns_empty(self):
        df = clean_stocks(pd.DataFrame())
        assert df.empty


# ── Crypto ────────────────────────────────────────────────────────────────────

class TestCleanCrypto:
    def _make(self, **overrides):
        base = dict(
            coin_id="bitcoin", symbol="btc", name="Bitcoin",
            price_usd=65000.0, market_cap=1.2e12,
            pct_chg_24h=2.3, volume_24h=3.0e10,
            high_24h=66000.0, low_24h=64000.0,
            fetched_at="2024-01-01T12:00:00",
        )
        base.update(overrides)
        return pd.DataFrame([base])

    def test_valid_row_passes(self):
        df = clean_crypto(self._make())
        assert len(df) == 1

    def test_zero_price_dropped(self):
        df = clean_crypto(self._make(price_usd=0))
        assert df.empty

    def test_symbol_uppercased(self):
        df = clean_crypto(self._make(symbol="btc"))
        assert df.iloc[0]["symbol"] == "BTC"

    def test_mega_cap_tier(self):
        df = clean_crypto(self._make(market_cap=2e11))
        assert "Mega" in df.iloc[0]["cap_tier"]

    def test_small_cap_tier(self):
        df = clean_crypto(self._make(market_cap=5e8))
        assert "Small" in df.iloc[0]["cap_tier"]

    def test_empty_returns_empty(self):
        assert clean_crypto(pd.DataFrame()).empty


# ── Weather ───────────────────────────────────────────────────────────────────

class TestCleanWeather:
    def _make(self, **overrides):
        base = dict(
            city="Berlin", temp_c=15.0, windspeed=20.0,
            weathercode=3, lat=52.52, lon=13.40,
            fetched_at="2024-01-01T12:00:00",
        )
        base.update(overrides)
        return pd.DataFrame([base])

    def test_condition_mapped(self):
        df = clean_weather(self._make(weathercode=0))
        assert df.iloc[0]["condition"] == "Clear sky"

    def test_temp_f_calculated(self):
        df = clean_weather(self._make(temp_c=0))
        assert df.iloc[0]["temp_f"] == 32.0

    def test_temp_clipped_max(self):
        df = clean_weather(self._make(temp_c=999))
        assert df.iloc[0]["temp_c"] == 60

    def test_temp_clipped_min(self):
        df = clean_weather(self._make(temp_c=-999))
        assert df.iloc[0]["temp_c"] == -80

    def test_unknown_weathercode(self):
        df = clean_weather(self._make(weathercode=999))
        assert df.iloc[0]["condition"] == "Unknown"

    def test_empty_returns_empty(self):
        assert clean_weather(pd.DataFrame()).empty
