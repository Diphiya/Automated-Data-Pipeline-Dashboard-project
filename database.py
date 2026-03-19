"""
database.py — SQLite persistence layer
Creates tables, inserts cleaned data, and exposes query helpers.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

DB_PATH = Path("data/pipeline.db")


# ── Schema ────────────────────────────────────────────────────────────────────

CREATE_STOCKS = """
CREATE TABLE IF NOT EXISTS stocks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    price       REAL,
    prev_close  REAL,
    change      REAL,
    pct_chg     REAL,
    currency    TEXT,
    exchange    TEXT,
    fetched_at  TEXT,
    inserted_at TEXT DEFAULT (datetime('now'))
);
"""

CREATE_CRYPTO = """
CREATE TABLE IF NOT EXISTS crypto (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id         TEXT NOT NULL,
    symbol          TEXT,
    name            TEXT,
    price_usd       REAL,
    market_cap      REAL,
    pct_chg_24h     REAL,
    volume_24h      REAL,
    high_24h        REAL,
    low_24h         REAL,
    fetched_at      TEXT,
    inserted_at     TEXT DEFAULT (datetime('now'))
);
"""

CREATE_WEATHER = """
CREATE TABLE IF NOT EXISTS weather (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    city        TEXT NOT NULL,
    temp_c      REAL,
    windspeed   REAL,
    weathercode INTEGER,
    lat         REAL,
    lon         REAL,
    fetched_at  TEXT,
    inserted_at TEXT DEFAULT (datetime('now'))
);
"""

CREATE_RUN_LOG = """
CREATE TABLE IF NOT EXISTS run_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at      TEXT DEFAULT (datetime('now')),
    stocks_rows INTEGER DEFAULT 0,
    crypto_rows INTEGER DEFAULT 0,
    weather_rows INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'ok'
);
"""


# ── Connection & Init ─────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create all tables if they don't exist."""
    with get_connection() as conn:
        for ddl in (CREATE_STOCKS, CREATE_CRYPTO, CREATE_WEATHER, CREATE_RUN_LOG):
            conn.execute(ddl)
    logger.info(f"Database ready at {DB_PATH}")


# ── Insert helpers ────────────────────────────────────────────────────────────

def _insert_df(df: pd.DataFrame, table: str, conn: sqlite3.Connection) -> int:
    if df.empty:
        return 0
    df.to_sql(table, conn, if_exists="append", index=False)
    return len(df)


def save_all(stocks: pd.DataFrame, crypto: pd.DataFrame, weather: pd.DataFrame) -> dict:
    """Persist all DataFrames and log the run."""
    init_db()
    with get_connection() as conn:
        n_stocks  = _insert_df(stocks,  "stocks",  conn)
        n_crypto  = _insert_df(crypto,  "crypto",  conn)
        n_weather = _insert_df(weather, "weather", conn)
        conn.execute(
            "INSERT INTO run_log (stocks_rows, crypto_rows, weather_rows) VALUES (?,?,?)",
            (n_stocks, n_crypto, n_weather)
        )
    summary = {"stocks": n_stocks, "crypto": n_crypto, "weather": n_weather}
    logger.info(f"Saved to DB: {summary}")
    return summary


# ── Query helpers (used by dashboard) ────────────────────────────────────────

def load_latest_stocks() -> pd.DataFrame:
    """Return only the most recent fetch per ticker."""
    sql = """
        SELECT s.*
        FROM stocks s
        INNER JOIN (
            SELECT ticker, MAX(fetched_at) AS max_ts
            FROM stocks GROUP BY ticker
        ) latest ON s.ticker = latest.ticker AND s.fetched_at = latest.max_ts
        ORDER BY pct_chg DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn)


def load_latest_crypto() -> pd.DataFrame:
    sql = """
        SELECT c.*
        FROM crypto c
        INNER JOIN (
            SELECT coin_id, MAX(fetched_at) AS max_ts
            FROM crypto GROUP BY coin_id
        ) latest ON c.coin_id = latest.coin_id AND c.fetched_at = latest.max_ts
        ORDER BY market_cap DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn)


def load_latest_weather() -> pd.DataFrame:
    sql = """
        SELECT w.*
        FROM weather w
        INNER JOIN (
            SELECT city, MAX(fetched_at) AS max_ts
            FROM weather GROUP BY city
        ) latest ON w.city = latest.city AND w.fetched_at = latest.max_ts
        ORDER BY temp_c DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn)


def load_stock_history(ticker: str, limit: int = 50) -> pd.DataFrame:
    sql = """
        SELECT fetched_at, price, pct_chg
        FROM stocks
        WHERE ticker = ?
        ORDER BY fetched_at DESC
        LIMIT ?
    """
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=(ticker, limit))


def load_run_log(limit: int = 20) -> pd.DataFrame:
    sql = "SELECT * FROM run_log ORDER BY run_at DESC LIMIT ?"
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=(limit,))


def db_stats() -> dict:
    with get_connection() as conn:
        stats = {}
        for tbl in ("stocks", "crypto", "weather", "run_log"):
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()
            stats[tbl] = row["n"]
    return stats


if __name__ == "__main__":
    init_db()
    print("DB stats:", db_stats())
