"""Streamlit dashboard for the market stream monitor.

Reads the same database the CLI writes to, so a monitor process can
run in another terminal while this view refreshes on its own:

    uv run streamlit run app.py
"""

import os
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.database import SessionLocal, init_db
from db.models import StoredAlert, StoredBar

st.set_page_config(page_title="Market Stream Monitor", page_icon="📡", layout="wide")
st.title("Market Stream Monitor")

init_db()
db: Session = SessionLocal()


def list_products() -> list[str]:
    """Distinct products with stored minute bars, sorted alphabetically."""
    return list(db.scalars(select(StoredBar.product).distinct().order_by(StoredBar.product)).all())


def load_bars(product: str, limit: int) -> pd.DataFrame:
    """Most recent ``limit`` bars for ``product`` ordered by minute ascending."""
    rows = db.scalars(
        select(StoredBar)
        .where(StoredBar.product == product)
        .order_by(StoredBar.minute.desc())
        .limit(limit)
    ).all()
    records = [
        {
            "minute": row.minute,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "vwap": row.vwap,
            "trade_count": row.trade_count,
        }
        for row in reversed(rows)
    ]
    return pd.DataFrame(records)


def load_alerts(limit: int = 20) -> pd.DataFrame:
    """Most recent alerts across all products, newest first."""
    rows = db.scalars(select(StoredAlert).order_by(StoredAlert.ts.desc()).limit(limit)).all()
    records = [
        {
            "ts": row.ts,
            "product": row.product,
            "rule": row.rule,
            "message": row.message,
            "value": row.value,
        }
        for row in rows
    ]
    return pd.DataFrame(records)


def candlestick_chart(product: str, bars: pd.DataFrame) -> go.Figure:
    figure = go.Figure(
        data=[
            go.Candlestick(
                x=bars["minute"],
                open=bars["open"],
                high=bars["high"],
                low=bars["low"],
                close=bars["close"],
                name="OHLC",
            ),
            go.Scatter(
                x=bars["minute"],
                y=bars["vwap"],
                mode="lines",
                name="VWAP",
                line={"width": 1.5},
            ),
        ]
    )
    figure.update_layout(
        title=f"{product} one-minute bars",
        xaxis_rangeslider_visible=False,
        height=420,
        margin={"l": 40, "r": 20, "t": 50, "b": 20},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.0},
    )
    return figure


def volume_chart(bars: pd.DataFrame) -> go.Figure:
    figure = go.Figure(data=[go.Bar(x=bars["minute"], y=bars["volume"])])
    figure.update_layout(
        title="Volume",
        height=220,
        margin={"l": 40, "r": 20, "t": 50, "b": 20},
    )
    return figure


# Sidebar controls for the auto-refresh loop.
auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
refresh_interval = st.sidebar.number_input(
    "Refresh interval (seconds)", min_value=2, value=5, step=1
)

products = list_products()
if not products:
    st.info(
        "No data is stored yet. Watch the live feed with "
        "`python main.py monitor BTC-USD ETH-USD` or run the offline demo with "
        "`python main.py replay data/sample-stream.jsonl`, then come back."
    )
    st.stop()

controls = st.columns([2, 3])
product = controls[0].selectbox("Product", products)
bars_to_show = int(controls[1].number_input("Bars to show", min_value=10, value=120, step=10))

bars = load_bars(product, bars_to_show)
latest = bars.iloc[-1]

metrics = st.columns(4)
metrics[0].metric("Latest close", f"{latest['close']:g}")
metrics[1].metric("Latest VWAP", f"{latest['vwap']:.2f}")
metrics[2].metric("Latest minute volume", f"{latest['volume']:.4f}")
metrics[3].metric("Total trades (window)", f"{int(bars['trade_count'].sum())}")

st.plotly_chart(candlestick_chart(product, bars))
st.plotly_chart(volume_chart(bars))

st.subheader("Alerts")
st.caption("The 20 most recent alerts across all products.")
alerts = load_alerts(20)
if not alerts.empty:
    st.dataframe(alerts, hide_index=True)
else:
    st.caption("No alerts yet")

# Deliberately simple auto-refresh without third-party packages: sleep then rerun
# as the very last step. The guard keeps it from looping forever under AppTest,
# which sets STREAM_MONITOR_NO_REFRESH so the test does not hang.
if auto_refresh and not os.getenv("STREAM_MONITOR_NO_REFRESH"):
    time.sleep(refresh_interval)
    st.rerun()
