#!/usr/bin/env python3
"""Streamlit dashboard that visualises 1 s OHLCV bars and pipeline latency."""

from __future__ import annotations

import os
import shlex
import sys
import time
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd  # type: ignore
import plotly.graph_objects as go  # type: ignore
import streamlit as st  # type: ignore

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from dashboard.cache import BarCache, CacheConfig, CacheConsumer

REFRESH_DEFAULT_MS = 1000
RERUN_CALLBACK = getattr(st, "rerun", None)
if RERUN_CALLBACK is None:
    RERUN_CALLBACK = getattr(st, "experimental_rerun", None)
if RERUN_CALLBACK is None:
    RERUN_CALLBACK = getattr(st, "_rerun", None)



def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (
            (value.startswith("\"") and value.endswith("\""))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = shlex.split(value)[0]
        os.environ.setdefault(key, value)


def create_cache() -> Tuple[BarCache, CacheConsumer]:
    env_path = Path(os.environ.get("DASHBOARD_ENV_FILE", ".env"))
    load_env(env_path)
    config = CacheConfig(
        brokers=os.environ.get("KAFKA_BROKERS", "localhost:9092"),
        topic=os.environ.get("AGG_OUTPUT_TOPIC", "ticks.agg.1s"),
        group_id=os.environ.get("DASHBOARD_GROUP_ID", "dashboard-cache"),
        max_per_symbol=int(os.environ.get("DASHBOARD_CACHE_BARS", "600")),
        poll_timeout_ms=int(os.environ.get("DASHBOARD_POLL_TIMEOUT_MS", "500")),
        idle_sleep_s=float(os.environ.get("DASHBOARD_IDLE_SLEEP_S", "0.05")),
    )
    consumer = CacheConsumer(config)
    cache = consumer.start()
    return cache, consumer


@st.cache_resource(show_spinner=False)
def get_cache() -> Tuple[BarCache, CacheConsumer]:
    return create_cache()


def format_duration_ms(value_ms: float) -> str:
    if value_ms >= 1000:
        return f"{value_ms / 1000.0:.2f} s"
    return f"{value_ms:.0f} ms"


def build_candlestick(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["start_time"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                increasing_line_color="#4caf50",
                decreasing_line_color="#f44336",
            )
        ]
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=0),
        height=400,
        xaxis_title="Time",
        yaxis_title="Price",
    )
    return fig


def schedule_refresh(enabled: bool, interval_ms: int) -> None:
    if not (enabled and interval_ms > 0 and callable(RERUN_CALLBACK)):
        return
    time.sleep(interval_ms / 1000.0)
    RERUN_CALLBACK()


def render_dashboard() -> None:
    st.set_page_config(page_title="Stock Stream", layout="wide")

    refresh_ms = max(int(os.environ.get("DASHBOARD_REFRESH_MS", str(REFRESH_DEFAULT_MS))), 0)
    auto_refresh = st.sidebar.checkbox(
        "Auto-refresh",
        value=True,
        help=f"Re-query Kafka every {refresh_ms} ms; disable to pause updates.",
    )
    if auto_refresh and not callable(RERUN_CALLBACK):
        st.sidebar.warning(
            "Auto-refresh requires Streamlit 1.10+. Upgrade streamlit or disable auto-refresh."
        )
        auto_refresh = False

    cache, _consumer = get_cache()
    st.title("Real-time OHLCV Dashboard")
    st.caption(
        "1 s aggregates from ticks.agg.1s with latency and backlog monitors (toggle auto-refresh to pause)."
    )

    symbols = cache.symbols()
    if not symbols:
        st.info("Waiting for bars from Kafka... ensure replay + aggregator are running.")
        schedule_refresh(auto_refresh, refresh_ms)
        return

    default_symbol = os.environ.get("DASHBOARD_DEFAULT_SYMBOL")
    default_index = symbols.index(default_symbol) if default_symbol in symbols else 0
    symbol = st.sidebar.selectbox("Symbol", symbols, index=default_index)
    history_seconds = st.sidebar.slider(
        "History (seconds)", min_value=60, max_value=900, value=300, step=60
    )

    snapshot = cache.snapshot(symbol)
    bars = snapshot.get(symbol, [])
    if not bars:
        st.warning("No bars cached yet for selected symbol.")
        schedule_refresh(auto_refresh, refresh_ms)
        return

    df = pd.DataFrame(bars).sort_values("start_ts_ms")
    df["start_time"] = pd.to_datetime(df["start_ts_ms"], unit="ms")
    cutoff = df["start_time"].max() - pd.Timedelta(seconds=history_seconds)
    df_filtered = df[df["start_time"] >= cutoff].copy()

    metrics = cache.metrics()
    last_bar: Dict[str, object] = df.iloc[-1].to_dict()

    latency_ms = metrics.get("last_latency_ms", 0.0)
    freshness_ms = metrics.get("last_freshness_ms", 0.0)
    log_lag_ms = metrics.get("last_log_lag_ms", 0.0)

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "End-to-end latency",
        format_duration_ms(latency_ms),
        delta=metrics.get("avg_latency_ms", 0.0) - latency_ms,
    )
    col2.metric(
        "Data freshness",
        format_duration_ms(freshness_ms),
        delta=metrics.get("avg_freshness_ms", 0.0) - freshness_ms,
    )
    col3.metric(
        "Kafka log lag",
        format_duration_ms(log_lag_ms),
        delta=metrics.get("avg_log_lag_ms", 0.0) - log_lag_ms,
    )

    st.plotly_chart(build_candlestick(df_filtered), use_container_width=True)

    with st.expander("Latest bar details", expanded=True):
        st.write(
            {
                "bar_id": last_bar.get("bar_id"),
                "start_ts": pd.to_datetime(last_bar.get("start_ts_ms", 0), unit="ms"),
                "end_ts": pd.to_datetime(last_bar.get("end_ts_ms", 0), unit="ms"),
                "open": last_bar.get("open"),
                "high": last_bar.get("high"),
                "low": last_bar.get("low"),
                "close": last_bar.get("close"),
                "volume": last_bar.get("volume"),
                "bin_count": last_bar.get("bin_count"),
                "max_lateness_ms": last_bar.get("max_lateness_ms"),
                "ingest_latency_ms": latency_ms,
            }
        )

    st.subheader("Aggregates snapshot")
    st.dataframe(
        df_filtered.set_index("start_time")[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "notional",
                "vwap",
                "bin_count",
                "max_lateness_ms",
            ]
        ],
        use_container_width=True,
    )

    errors = cache.errors()
    if errors:
        st.sidebar.error("\n".join(errors[-5:]))

    st.sidebar.metric("Bars cached", f"{int(metrics.get('bars_cached', 0))}")
    st.sidebar.metric("Backlog", format_duration_ms(metrics.get("backlog_ms", 0.0)))
    last_update_ms = metrics.get("last_update_ts_ms", 0.0)
    last_update = (
        pd.to_datetime(last_update_ms, unit="ms").strftime("%H:%M:%S") if last_update_ms else "n/a"
    )
    st.sidebar.metric("Last update", last_update)

    st.caption(
        "SLA targets: producer publish <20 ms, processor <750 ms, dashboard refresh 1 s. Badges above should stay below thresholds."
    )

    schedule_refresh(auto_refresh, refresh_ms)


if __name__ == "__main__":
    render_dashboard()
