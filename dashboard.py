"""
dashboard.py — Real-time Streamlit dashboard
Run: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import database
import pipeline

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Market & Weather Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #2a2a3e 100%);
        border: 1px solid #383854;
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 10px;
    }
    .up   { color: #4ade80; font-weight: 700; }
    .down { color: #f87171; font-weight: 700; }
    .neutral { color: #facc15; font-weight: 700; }
    div[data-testid="stMetricValue"] { font-size: 1.6rem !important; }
</style>
""", unsafe_allow_html=True)

PLOTLY_TEMPLATE = "plotly_dark"
BG_COLOR        = "#0f0f1a"
CARD_BG         = "#1e1e2e"

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/combo-chart.png", width=60)
    st.title("Pipeline Control")
    st.markdown("---")

    if st.button("🔄 Run Pipeline Now", use_container_width=True):
        with st.spinner("Fetching live data …"):
            try:
                summary = pipeline.run_pipeline()
                st.success(f"Done! {summary}")
            except Exception as e:
                st.error(f"Pipeline error: {e}")

    st.markdown("---")
    auto_refresh = st.checkbox("Auto-refresh (60 s)", value=False)
    if auto_refresh:
        st.info("Page refreshes every 60 s")
        st.markdown(
            '<meta http-equiv="refresh" content="60">',
            unsafe_allow_html=True,
        )

    st.markdown("---")
    stats = database.db_stats()
    st.markdown("### DB Stats")
    for tbl, count in stats.items():
        st.markdown(f"- **{tbl}**: {count} rows")

    st.markdown("---")
    st.caption(f"Last rendered: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("# 📊 Market & Weather Dashboard")
st.markdown("Real-time data pipeline — stocks · crypto · global weather")
st.markdown("---")

# ── Load data ─────────────────────────────────────────────────────────────────
stocks  = database.load_latest_stocks()
crypto  = database.load_latest_crypto()
weather = database.load_latest_weather()

has_stocks  = not stocks.empty
has_crypto  = not crypto.empty
has_weather = not weather.empty

if not any([has_stocks, has_crypto, has_weather]):
    st.warning("⚠️ No data yet. Click **Run Pipeline Now** in the sidebar to fetch live data.")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# STOCKS
# ══════════════════════════════════════════════════════════════════════════════
if has_stocks:
    st.subheader("📈 Stocks — Latest Snapshot")

    # KPI row
    cols = st.columns(len(stocks))
    for col, (_, row) in zip(cols, stocks.iterrows()):
        arrow = "▲" if row["pct_chg"] > 0 else ("▼" if row["pct_chg"] < 0 else "–")
        color = "up" if row["pct_chg"] > 0 else ("down" if row["pct_chg"] < 0 else "neutral")
        col.metric(
            label=row["ticker"],
            value=f"${row['price']:,.2f}",
            delta=f"{row['pct_chg']:+.2f}%",
        )

    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(
            stocks, x="ticker", y="pct_chg",
            color="pct_chg",
            color_continuous_scale=["#f87171", "#facc15", "#4ade80"],
            title="Daily % Change by Ticker",
            labels={"pct_chg": "% Change", "ticker": "Ticker"},
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(paper_bgcolor=BG_COLOR, plot_bgcolor=CARD_BG,
                          coloraxis_showscale=False)
        fig.add_hline(y=0, line_dash="dot", line_color="grey")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig2 = px.scatter(
            stocks, x="prev_close", y="price",
            text="ticker", size=[20]*len(stocks),
            color="pct_chg",
            color_continuous_scale=["#f87171", "#facc15", "#4ade80"],
            title="Previous Close vs Current Price",
            labels={"prev_close": "Prev Close ($)", "price": "Current Price ($)"},
            template=PLOTLY_TEMPLATE,
        )
        # Diagonal reference line
        mn = min(stocks["prev_close"].min(), stocks["price"].min()) * 0.98
        mx = max(stocks["prev_close"].max(), stocks["price"].max()) * 1.02
        fig2.add_shape(type="line", x0=mn, y0=mn, x1=mx, y1=mx,
                       line=dict(color="grey", dash="dot"))
        fig2.update_traces(textposition="top center")
        fig2.update_layout(paper_bgcolor=BG_COLOR, plot_bgcolor=CARD_BG,
                           coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("📋 Raw stocks data"):
        st.dataframe(stocks, use_container_width=True)

    st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# CRYPTO
# ══════════════════════════════════════════════════════════════════════════════
if has_crypto:
    st.subheader("🪙 Crypto — Market Overview")

    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(
            crypto.sort_values("pct_chg_24h"),
            x="pct_chg_24h", y="symbol",
            orientation="h",
            color="pct_chg_24h",
            color_continuous_scale=["#f87171", "#facc15", "#4ade80"],
            title="24h % Change",
            labels={"pct_chg_24h": "24h %", "symbol": ""},
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(paper_bgcolor=BG_COLOR, plot_bgcolor=CARD_BG,
                          coloraxis_showscale=False)
        fig.add_vline(x=0, line_dash="dot", line_color="grey")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig2 = px.treemap(
            crypto, path=["name"], values="market_cap",
            color="pct_chg_24h",
            color_continuous_scale=["#f87171", "#facc15", "#4ade80"],
            title="Market Cap Tree Map",
            template=PLOTLY_TEMPLATE,
        )
        fig2.update_layout(paper_bgcolor=BG_COLOR)
        st.plotly_chart(fig2, use_container_width=True)

    # Price table
    display_crypto = crypto[["symbol","name","price_usd","pct_chg_24h","market_cap","volume_24h"]].copy()
    display_crypto.columns = ["Symbol","Name","Price (USD)","24h %","Market Cap","Volume 24h"]
    display_crypto["Price (USD)"] = display_crypto["Price (USD)"].apply(lambda x: f"${x:,.4f}")
    display_crypto["Market Cap"]  = display_crypto["Market Cap"].apply(lambda x: f"${x/1e9:.2f}B")
    display_crypto["Volume 24h"]  = display_crypto["Volume 24h"].apply(lambda x: f"${x/1e6:.1f}M")

    st.dataframe(display_crypto, use_container_width=True, hide_index=True)

    st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# WEATHER
# ══════════════════════════════════════════════════════════════════════════════
if has_weather:
    st.subheader("🌍 Global Weather — Current Conditions")

    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(
            weather.sort_values("temp_c", ascending=False),
            x="city", y="temp_c",
            color="temp_c",
            color_continuous_scale="RdYlBu_r",
            title="Temperature by City (°C)",
            labels={"temp_c": "°C", "city": ""},
            template=PLOTLY_TEMPLATE,
            text="temp_c",
        )
        fig.update_traces(texttemplate="%{text:.1f}°C", textposition="outside")
        fig.update_layout(paper_bgcolor=BG_COLOR, plot_bgcolor=CARD_BG,
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig2 = px.scatter_geo(
            weather,
            lat="lat", lon="lon",
            hover_name="city",
            size=[30]*len(weather),
            color="temp_c",
            color_continuous_scale="RdYlBu_r",
            hover_data={"temp_c": True, "windspeed": True, "lat": False, "lon": False},
            title="World Map — Temperature",
            template=PLOTLY_TEMPLATE,
        )
        fig2.update_geos(showland=True, landcolor="#1e1e2e",
                         showocean=True, oceancolor="#0f0f1a",
                         showcountries=True, countrycolor="#383854")
        fig2.update_layout(paper_bgcolor=BG_COLOR, geo_bgcolor=BG_COLOR,
                           coloraxis_showscale=False, margin=dict(l=0,r=0,t=40,b=0))
        st.plotly_chart(fig2, use_container_width=True)

    # Cards
    cols = st.columns(len(weather))
    for col, (_, row) in zip(cols, weather.iterrows()):
        col.metric(
            label=f"🏙️ {row['city']}",
            value=f"{row['temp_c']:.1f} °C",
            delta=f"💨 {row['windspeed']:.0f} km/h",
        )

    st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# RUN LOG
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("🗂️ Pipeline Run History")
run_log = database.load_run_log(limit=10)
if not run_log.empty:
    st.dataframe(run_log, use_container_width=True, hide_index=True)
else:
    st.info("No runs recorded yet.")
