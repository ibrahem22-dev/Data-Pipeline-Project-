import sys
import os

# Add src directory to path so local imports work when running from project root
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from datetime import datetime, timedelta

from database import engine, init_database
from config import CITIES

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Weather Pipeline Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Metric card styling */
    [data-testid="metric-container"] {
        background: #1e2130;
        border: 1px solid #2d3250;
        border-radius: 12px;
        padding: 20px 24px;
    }
    [data-testid="metric-container"] label {
        color: #8892b0 !important;
        font-size: 0.78rem !important;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #ccd6f6 !important;
        font-size: 2rem !important;
        font-weight: 700;
    }
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #0d1117;
    }
    /* Main background */
    .stApp {
        background: #0a0e1a;
    }
    /* Section headers */
    .section-header {
        color: #64ffda;
        font-size: 1rem;
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        margin-bottom: 0.5rem;
    }
    /* Divider */
    hr { border-color: #2d3250; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_data() -> pd.DataFrame:
    """Load all weather readings from PostgreSQL."""
    try:
        query = text("""
            SELECT id, city, temperature, feels_like, humidity,
                   pressure, wind_speed, description, clouds, recorded_at
            FROM weather_readings
            ORDER BY recorded_at ASC
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        df["recorded_at"] = pd.to_datetime(df["recorded_at"])
        # Strip country suffix from city names for cleaner display
        df["city_label"] = df["city"].str.split(",").str[0]
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def run_pipeline():
    """Trigger a single pipeline run (extract → transform → load)."""
    try:
        from extract import fetch_all_cities
        from transform import process_data
        from load import save_to_db, save_to_csv
        from config import API_KEY

        if not API_KEY:
            return False, "API key not configured. Add OPENWEATHER_API_KEY to your .env file."

        init_database()
        records = fetch_all_cities(CITIES)
        if not records:
            return False, "No data fetched — check your API key or network."

        df = process_data(records)
        save_to_db(df)
        save_to_csv(df)
        return True, f"Pipeline ran successfully — {len(df)} records inserted."
    except Exception as e:
        return False, f"Pipeline error: {e}"


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Weather Pipeline")
    st.caption("Real-time weather data for Israeli cities")
    st.divider()

    # Trigger pipeline
    st.markdown('<p class="section-header">Pipeline</p>', unsafe_allow_html=True)
    if st.button("▶ Fetch New Data", use_container_width=True, type="primary"):
        with st.spinner("Running pipeline…"):
            ok, msg = run_pipeline()
        if ok:
            st.success(msg)
            st.cache_data.clear()
            st.rerun()
        else:
            st.error(msg)

    st.divider()

    # Load data for filter options
    raw_df = load_data()

    # City filter
    st.markdown('<p class="section-header">City Filter</p>', unsafe_allow_html=True)
    if raw_df.empty:
        available_cities = []
    else:
        available_cities = sorted(raw_df["city_label"].unique().tolist())

    all_option = "All Cities"
    city_choice = st.selectbox(
        "Select city",
        options=[all_option] + available_cities,
        label_visibility="collapsed",
    )

    st.divider()

    # Date range filter
    st.markdown('<p class="section-header">Date Range</p>', unsafe_allow_html=True)
    if raw_df.empty:
        min_date = datetime.now().date() - timedelta(days=7)
        max_date = datetime.now().date()
    else:
        min_date = raw_df["recorded_at"].min().date()
        max_date = raw_df["recorded_at"].max().date()

    date_from = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date)
    date_to = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

    st.divider()
    st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("↻ Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ── Apply filters ─────────────────────────────────────────────────────────────
df = raw_df.copy()

if not df.empty:
    # Date filter
    df = df[
        (df["recorded_at"].dt.date >= date_from) &
        (df["recorded_at"].dt.date <= date_to)
    ]
    # City filter
    if city_choice != all_option:
        df = df[df["city_label"] == city_choice]


# ── Main layout ───────────────────────────────────────────────────────────────
st.title("Weather Pipeline Dashboard")

if raw_df.empty:
    st.warning("No data found. Click **Fetch New Data** in the sidebar to run the pipeline.")
    st.stop()

if df.empty:
    st.warning("No records match the selected filters.")
    st.stop()


# ── Metric cards ──────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_records = len(df)
cities_tracked = df["city_label"].nunique()
avg_temp = df["temperature"].mean()
avg_humidity = df["humidity"].mean()

# Deltas vs. overall dataset (for context)
overall_avg_temp = raw_df["temperature"].mean()
overall_avg_humidity = raw_df["humidity"].mean()

col1.metric("Total Records", f"{total_records:,}")
col2.metric("Cities Tracked", cities_tracked)
col3.metric(
    "Avg Temperature",
    f"{avg_temp:.1f} °C",
    delta=f"{avg_temp - overall_avg_temp:+.1f} vs all" if city_choice != all_option else None,
)
col4.metric(
    "Avg Humidity",
    f"{avg_humidity:.1f} %",
    delta=f"{avg_humidity - overall_avg_humidity:+.1f} vs all" if city_choice != all_option else None,
)

st.divider()


# ── Chart row 1: Temperature over time ───────────────────────────────────────
st.markdown('<p class="section-header">Temperature Over Time</p>', unsafe_allow_html=True)

fig_line = px.line(
    df,
    x="recorded_at",
    y="temperature",
    color="city_label",
    markers=True,
    labels={"recorded_at": "Date / Time", "temperature": "Temperature (°C)", "city_label": "City"},
    color_discrete_sequence=px.colors.qualitative.Bold,
)
fig_line.update_layout(
    plot_bgcolor="#1e2130",
    paper_bgcolor="#1e2130",
    font_color="#ccd6f6",
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        bgcolor="rgba(0,0,0,0)",
        font_color="#8892b0",
    ),
    xaxis=dict(gridcolor="#2d3250", linecolor="#2d3250"),
    yaxis=dict(gridcolor="#2d3250", linecolor="#2d3250"),
    margin=dict(l=0, r=0, t=40, b=0),
    hovermode="x unified",
)
fig_line.update_traces(line_width=2, marker_size=5)
st.plotly_chart(fig_line, use_container_width=True)

st.divider()


# ── Chart row 2: Humidity bar + feels-like scatter ────────────────────────────
col_bar, col_scatter = st.columns(2)

with col_bar:
    st.markdown('<p class="section-header">Avg Humidity by City</p>', unsafe_allow_html=True)
    humidity_stats = (
        df.groupby("city_label", as_index=False)["humidity"]
        .mean()
        .sort_values("humidity", ascending=False)
    )
    fig_bar = px.bar(
        humidity_stats,
        x="city_label",
        y="humidity",
        color="humidity",
        color_continuous_scale="Blues",
        labels={"city_label": "City", "humidity": "Avg Humidity (%)"},
        text_auto=".1f",
    )
    fig_bar.update_layout(
        plot_bgcolor="#1e2130",
        paper_bgcolor="#1e2130",
        font_color="#ccd6f6",
        coloraxis_showscale=False,
        xaxis=dict(gridcolor="#2d3250", linecolor="#2d3250"),
        yaxis=dict(gridcolor="#2d3250", linecolor="#2d3250"),
        margin=dict(l=0, r=0, t=10, b=0),
    )
    fig_bar.update_traces(textfont_color="#0a0e1a", textposition="outside")
    st.plotly_chart(fig_bar, use_container_width=True)

with col_scatter:
    st.markdown('<p class="section-header">Temperature vs Feels Like</p>', unsafe_allow_html=True)
    scatter_df = df.dropna(subset=["feels_like"])
    fig_scatter = px.scatter(
        scatter_df,
        x="temperature",
        y="feels_like",
        color="city_label",
        size_max=10,
        opacity=0.8,
        labels={
            "temperature": "Actual Temp (°C)",
            "feels_like": "Feels Like (°C)",
            "city_label": "City",
        },
        color_discrete_sequence=px.colors.qualitative.Bold,
        trendline=None,
    )
    # Reference line: y = x
    if not scatter_df.empty:
        axis_min = min(scatter_df["temperature"].min(), scatter_df["feels_like"].min())
        axis_max = max(scatter_df["temperature"].max(), scatter_df["feels_like"].max())
        fig_scatter.add_trace(
            go.Scatter(
                x=[axis_min, axis_max],
                y=[axis_min, axis_max],
                mode="lines",
                line=dict(color="#64ffda", dash="dot", width=1),
                name="Feels = Actual",
                showlegend=True,
            )
        )
    fig_scatter.update_layout(
        plot_bgcolor="#1e2130",
        paper_bgcolor="#1e2130",
        font_color="#ccd6f6",
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#8892b0"),
        xaxis=dict(gridcolor="#2d3250", linecolor="#2d3250"),
        yaxis=dict(gridcolor="#2d3250", linecolor="#2d3250"),
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()


# ── Latest readings table ─────────────────────────────────────────────────────
st.markdown('<p class="section-header">Latest Reading per City</p>', unsafe_allow_html=True)

latest = (
    df.sort_values("recorded_at")
    .groupby("city_label", as_index=False)
    .last()[["city_label", "temperature", "feels_like", "humidity",
             "wind_speed", "description", "recorded_at"]]
    .rename(columns={
        "city_label": "City",
        "temperature": "Temp (°C)",
        "feels_like": "Feels Like (°C)",
        "humidity": "Humidity (%)",
        "wind_speed": "Wind (m/s)",
        "description": "Conditions",
        "recorded_at": "Recorded At",
    })
)
latest["Recorded At"] = latest["Recorded At"].dt.strftime("%Y-%m-%d %H:%M")

st.dataframe(
    latest,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Temp (°C)": st.column_config.NumberColumn(format="%.1f °C"),
        "Feels Like (°C)": st.column_config.NumberColumn(format="%.1f °C"),
        "Humidity (%)": st.column_config.NumberColumn(format="%.1f %%"),
        "Wind (m/s)": st.column_config.NumberColumn(format="%.1f m/s"),
    },
)
