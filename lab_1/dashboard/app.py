import streamlit as st
import pandas as pd
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta

st.set_page_config(layout="wide")
st.title("Servers Cluster Failure Monitoring")

# Auto-refresh every 5 seconds
st_autorefresh(interval=5000, key="datarefresh")

BROKERS = ["broker1:9092", "broker2:9093"]
TOPIC = "visualization"

# Keep only last N minutes in memory
ROLLING_WINDOW_MINUTES = 30


@st.cache_resource
def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BROKERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="dashboard-group",
            )
            return consumer
        except NoBrokersAvailable:
            time.sleep(5)


def main():
    consumer = create_consumer()

    if "data" not in st.session_state:
        st.session_state.data = []

    # Poll Kafka (non-blocking)
    records = consumer.poll(timeout_ms=1000)

    for _, messages in records.items():
        for message in messages:
            st.session_state.data.append(message.value)

    if not st.session_state.data:
        st.warning("Waiting for data from Kafka...")
        st.stop()

    df = pd.DataFrame(st.session_state.data)

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], unit="s", errors="coerce")

    numeric_cols = [
        "duration",
        "page_cache_memory",
        "cpu_max",
        "cycles_per_instruction",
        "assigned_memory",
        "fail_probability",
        "failed",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["cluster"])

    if "time" in df.columns:
        df["time"] = pd.to_datetime(
            df["time"],
            unit="s",
            errors="coerce",
            utc=True,
        )

    if df.empty:
        st.warning("No recent data in rolling window.")
        st.stop()

    st.header("Global Statistics")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Samples", len(df))
    col2.metric("Failure Rate (%)", f"{df['failed'].mean() * 100:.2f}")
    col3.metric("Avg Fail Probability", f"{df['fail_probability'].mean():.3f}")
    col4.metric("Avg CPU Max", f"{df['cpu_max'].mean():.2f}")

    st.header("Cluster Instability Ranking")

    cluster_stats = (
        df.groupby("cluster")
        .agg(
            samples=("failed", "count"),
            failure_rate=("failed", "mean"),
            avg_probability=("fail_probability", "mean"),
            avg_cpu=("cpu_max", "mean"),
            avg_duration=("duration", "mean"),
            avg_memory=("assigned_memory", "mean"),
        )
        .reset_index()
    )

    # Convert failure rate to %
    cluster_stats["failure_rate"] *= 100

    # Normalize CPU for scoring
    cpu_min = cluster_stats["avg_cpu"].min()
    cpu_max = cluster_stats["avg_cpu"].max()

    if cpu_max != cpu_min:
        cluster_stats["cpu_norm"] = (
            (cluster_stats["avg_cpu"] - cpu_min) /
            (cpu_max - cpu_min)
        )
    else:
        cluster_stats["cpu_norm"] = 0

    # Instability Score
    cluster_stats["instability_score"] = (
        0.6 * (cluster_stats["failure_rate"] / 100) +
        0.3 * cluster_stats["avg_probability"] +
        0.1 * cluster_stats["cpu_norm"]
    )

    cluster_stats = cluster_stats.sort_values(
        "instability_score", ascending=False
    )

    st.dataframe(cluster_stats, width=True)

    st.subheader("Instability Score by Cluster")
    st.bar_chart(
        cluster_stats.set_index("cluster")["instability_score"]
    )

    st.header("Visual Insights")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Failure Rate by Cluster")
        st.bar_chart(
            cluster_stats.set_index("cluster")["failure_rate"]
        )

    with col2:
        st.subheader("Average CPU by Cluster")
        st.bar_chart(
            cluster_stats.set_index("cluster")["avg_cpu"]
        )

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Failure Probability Distribution")
        hist_data = df["fail_probability"].dropna()
        st.bar_chart(hist_data.value_counts().sort_index())

    with col4:
        st.subheader("Failures Over Time")
        if "time" in df.columns:
            time_series = (
                df.set_index("time")["failed"]
                .resample("1min")
                .mean()
            )
            st.line_chart(time_series)

    st.header("Latest Records")
    st.dataframe(df.sort_values("time").tail(20), width=True)


if __name__ == "__main__":
    main()