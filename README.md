# 📊 Automated Data Pipeline & Dashboard

A full-stack data engineering project that **automatically collects**, **cleans**, **stores**, and **visualises** real-time data from public web sources — no API keys required.


---

##  Features

| Layer | What it does |
|---|---|
| **Scraper** (`scraper.py`) | Fetches live stock prices (Yahoo Finance), crypto data (CoinGecko), and global weather (Open-Meteo) |
| **Processor** (`processor.py`) | Validates, normalises, clips outliers, adds derived fields (sentiment labels, wind-chill, cap tiers) |
| **Database** (`database.py`) | Persists all data into a local SQLite database with indexed query helpers |
| **Pipeline** (`pipeline.py`) | Orchestrates ETL in one command; supports one-shot and scheduled modes |
| **Dashboard** (`dashboard.py`) | Interactive Streamlit app with Plotly charts, KPI cards, and world maps |

---


##  Project Structure

```
data-pipeline-dashboard/
├── scraper.py          # Data collection (stocks, crypto, weather)
├── processor.py        # Cleaning & transformation
├── database.py         # SQLite persistence layer
├── pipeline.py         # ETL orchestrator + scheduler
├── dashboard.py        # Streamlit dashboard
├── requirements.txt    # Python dependencies
├── data/               # Auto-created — DB + logs live here
├── tests/
│   └── test_processor.py   # Pytest unit tests
├── .github/
│   └── workflows/ci.yml    # GitHub Actions CI
└── README.md
```

---


### 1. Clone & set up

```bash
git clone https://github.com/YOUR_USERNAME/data-pipeline-dashboard.git
cd data-pipeline-dashboard

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the pipeline (fetch live data)

```bash
python pipeline.py
```

Sample output:
```
2024-03-01 10:00:00 [INFO] ━━━ Pipeline run started ━━━
2024-03-01 10:00:01 [INFO] ▸ Stocks
2024-03-01 10:00:01 [INFO]   ✓ AAPL: $175.23
2024-03-01 10:00:02 [INFO]   ✓ GOOGL: $141.08
...
2024-03-01 10:00:07 [INFO] Saved to DB: {'stocks': 7, 'crypto': 5, 'weather': 6}
Summary: {'stocks': 7, 'crypto': 5, 'weather': 6}
```

### 3. Launch the dashboard

```bash
streamlit run dashboard.py
```

Open **http://localhost:8501** in your browser.

### 4. Schedule automatic refreshes (optional)

```bash
python pipeline.py --schedule --interval 15   # runs every 15 minutes
```

---

## Data Sources

| Source | API | Rate Limit | Key Required |
|---|---|---|---|
| Stock prices | Yahoo Finance (unofficial chart API) | ~2 req/s | ❌ No |
| Crypto market | CoinGecko `/coins/markets` | 10–30 req/min | ❌ No |
| Weather | Open-Meteo | ~10 req/min | ❌ No |

---

##  Running Tests

```bash
pytest tests/ -v
```

All 21 unit tests cover the cleaning and transformation layer.

---

##  Database Schema

```sql
stocks  (ticker, price, prev_close, change, pct_chg, currency, exchange, fetched_at)
crypto  (coin_id, symbol, name, price_usd, market_cap, pct_chg_24h, volume_24h, high_24h, low_24h, fetched_at)
weather (city, temp_c, windspeed, weathercode, lat, lon, fetched_at)
run_log (run_at, stocks_rows, crypto_rows, weather_rows, status)
```

---

##  Customisation

**Add more stock tickers** — edit `STOCK_TICKERS` in `scraper.py`:
```python
STOCK_TICKERS = ["AAPL", "GOOGL", "MSFT", ...]
```

**Add more cities** — edit `CITIES` in `scraper.py`:
```python
CITIES = [{"name": "Paris", "lat": 48.85, "lon": 2.35}, ...]
```

**Change schedule interval**:
```bash
python pipeline.py --schedule --interval 30   # every 30 minutes
```

---

##  Skills Demonstrated

- **Web Scraping / API Integration** — `requests`, multiple public data sources
- **Data Engineering** — ETL pipeline, cleaning, validation, outlier handling
- **Database Design** — SQLite schema, query helpers, run logging
- **Data Visualisation** — Plotly bar charts, scatter plots, treemaps, geo maps
- **Dashboard Development** — Streamlit layout, KPI cards, sidebar controls
- **Software Engineering** — modular architecture, logging, error handling
- **Testing** — pytest unit tests with 21 test cases
- **CI/CD** — GitHub Actions workflow for automated testing

---

