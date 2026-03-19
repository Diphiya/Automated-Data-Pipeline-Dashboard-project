"""
pipeline.py — Orchestrates scrape → clean → store cycle.
Run once manually or schedule with cron / APScheduler.

Usage:
    python pipeline.py            # run once
    python pipeline.py --schedule # run every 15 minutes
"""

import argparse
import logging
import time
import sys
from datetime import datetime

import scraper
import processor
import database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)


def run_pipeline() -> dict:
    """One full ETL cycle. Returns row-count summary."""
    start = datetime.utcnow()
    logger.info("━━━ Pipeline run started ━━━")

    # 1. Extract
    raw = scraper.run_all()

    # 2. Transform
    cleaned = processor.process_all(raw)

    # 3. Load
    # Strip extra columns that aren't in the DB schema
    stocks_db  = cleaned["stocks"].drop(columns=["sentiment"], errors="ignore")
    crypto_db  = cleaned["crypto"].drop(columns=["sentiment", "cap_tier"], errors="ignore")
    weather_db = cleaned["weather"].drop(columns=["condition", "temp_f", "feels_like"], errors="ignore")

    summary = database.save_all(stocks_db, crypto_db, weather_db)

    elapsed = (datetime.utcnow() - start).total_seconds()
    logger.info(f"━━━ Pipeline complete in {elapsed:.1f}s | {summary} ━━━")
    return summary


def schedule_pipeline(interval_minutes: int = 15):
    """Run the pipeline on a fixed interval (blocking loop)."""
    logger.info(f"Scheduler started — interval: {interval_minutes} min")
    while True:
        try:
            run_pipeline()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user.")
            break
        except Exception as e:
            logger.error(f"Pipeline run failed: {e}", exc_info=True)
        logger.info(f"Next run in {interval_minutes} minutes …")
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Pipeline Runner")
    parser.add_argument("--schedule", action="store_true",
                        help="Run on a repeating schedule instead of once")
    parser.add_argument("--interval", type=int, default=15,
                        help="Schedule interval in minutes (default: 15)")
    args = parser.parse_args()

    # Always init DB first
    database.init_db()

    if args.schedule:
        schedule_pipeline(args.interval)
    else:
        summary = run_pipeline()
        print("\nSummary:", summary)
        print("DB stats:", database.db_stats())
