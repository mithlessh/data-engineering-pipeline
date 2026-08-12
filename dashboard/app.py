from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PROJECT PATHS & CONSTANTS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_PATH = PROJECT_ROOT / "data" / "processed" / "gold"

DAILY_PATH = GOLD_PATH / "daily_metrics"
HOURLY_PATH = GOLD_PATH / "hourly_metrics"
VENDOR_PATH = GOLD_PATH / "vendor_metrics"

ALL_VENDOR_KEY = "ALL"

METRIC_COLUMNS = [
    "total_trips",
    "total_passengers",
    "avg_trip_duration_sec",
    "avg_trip_distance_km",
    "avg_trip_speed_kmh",
]

DAILY_COLUMNS = ["pickup_date"] + METRIC_COLUMNS
HOURLY_COLUMNS = ["pickup_date", "pickup_hour"] + METRIC_COLUMNS
VENDOR_COLUMNS = ["vendor_id"] + METRIC_COLUMNS

COLORS = {
    "background": "#07111F",
    "panel": "#0D1726",
    "card": "#111D2E",
    "border": "#223047",
    "primary": "#38BDF8",
    "purple": "#8B5CF6",
    "green": "#22C55E",
    "warning": "#F59E0B",
    "text": "#F8FAFC",
    "muted": "#94A3B8",
    "chart_text": "#CBD5E1",
    "grid": "rgba(148, 163, 184, 0.10)",
}

PLOT_CONFIG = {
    "displayModeBar": False,
    "responsive": True,
}

MAX_HEATMAP_DAYS = 45


# ============================================================
# STREAMLIT PAGE CONFIG & CSS
# ============================================================

st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

CSS = """
<style>
:root { color-scheme: dark; }
html, body { background-color: #07111F !important; }
[data-testid="stAppViewContainer"], [data-testid="stApp"], .main, section.main { background-color: transparent !important; }
.block-container { padding-top: 2.1rem; padding-bottom: 2.2rem; max-width: 1550px; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
h1, h2, h3, h4, h5 { color: #F8FAFC !important; letter-spacing: -0.02em; }
p, li, span, div { color: #CBD5E1; }
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p, .header-mini, .kpi-sub { color: #94A3B8 !important; }
[data-testid="stSidebar"] { background-color: #0D1726; border-right: 1px solid #223047; }
[data-testid="stSidebar"] label, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span { color: #E2E8F0; }
.animated-bg {
    position: fixed; inset: 0; z-index: -10; pointer-events: none;
    background: radial-gradient(1100px 520px at 12% 0%, rgba(56, 189, 248, 0.10), transparent 60%),
                radial-gradient(900px 480px at 88% 18%, rgba(139, 92, 246, 0.08), transparent 58%),
                linear-gradient(180deg, #07111F 0%, #07111F 100%);
    background-size: 180% 180%; animation: dashboardBackground 35s ease-in-out infinite alternate;
}
@keyframes dashboardBackground { from { background-position: 0% 0%; } to { background-position: 100% 100%; } }
.header-right { display: flex; flex-direction: column; align-items: flex-end; gap: 8px; }
.status-pill { display: inline-flex; align-items: center; padding: 8px 12px; border-radius: 999px; font-size: 12px; font-weight: 700; letter-spacing: 0.05em; border: 1px solid transparent; white-space: nowrap; }
.status-pill.ok { color: #22C55E; border-color: rgba(34, 197, 94, 0.35); background: rgba(34, 197, 94, 0.08); }
.status-pill.error { color: #EF4444; border-color: rgba(239, 68, 68, 0.35); background: rgba(239, 68, 68, 0.08); }
.kpi-card { background: #111D2E; border: 1px solid #223047; border-radius: 16px; padding: 18px; min-height: 116px; transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease; }
.kpi-card:hover { transform: translateY(-2px); border-color: rgba(56, 189, 248, 0.45); box-shadow: 0 10px 24px rgba(2, 6, 23, 0.28); }
.kpi-label { color: #94A3B8; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.09em; margin-bottom: 8px; }
.kpi-value { color: #F8FAFC; font-size: 28px; font-weight: 750; line-height: 1.15; }
.kpi-sub { color: #94A3B8; font-size: 12px; margin-top: 8px; }
.stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom: 1px solid #223047; }
.stTabs [data-baseweb="tab"] { background: transparent; color: #94A3B8 !important; border-radius: 10px 10px 0 0; padding: 8px 14px; }
.stTabs [aria-selected="true"] { background: rgba(56, 189, 248, 0.08) !important; color: #F8FAFC !important; border-bottom: 2px solid #38BDF8; }
[data-testid="stMetric"] { background: #111D2E; border: 1px solid #223047; border-radius: 14px; padding: 14px; }
[data-testid="stMetricLabel"] { color: #94A3B8; }
[data-testid="stMetricValue"] { color: #F8FAFC; }
.stButton > button, .stSelectbox label, .stDateInput label { color: #E2E8F0; }
div[data-baseweb="select"] > div, div[data-baseweb="input"] { background-color: #111D2E; border-color: #223047; color: #F8FAFC; }
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)
st.markdown('<div class="animated-bg" aria-hidden="true"></div>', unsafe_allow_html=True)


# ============================================================
# UTILITIES
# ============================================================

def spark_uri(path: Path) -> str:
    return path.as_uri()

def has_parquet_files(path: Path) -> bool:
    try:
        return any(path.rglob("*.parquet")) or any(path.rglob("*.PARQUET"))
    except OSError:
        return False

def validate_gold_data() -> Tuple[List[str], List[str]]:
    missing: List[str] = []
    empty: List[str] = []
    for label, path in (("daily_metrics", DAILY_PATH), ("hourly_metrics", HOURLY_PATH), ("vendor_metrics", VENDOR_PATH)):
        if not path.is_dir():
            missing.append(str(path))
        elif not has_parquet_files(path):
            empty.append(str(path))
    return missing, empty

def natural_key(value: str):
    try:
        return (0, float(value), value)
    except (TypeError, ValueError):
        return (1, float("inf"), str(value).lower())

def vendor_key_from_value(value) -> str:
    try:
        if isinstance(value, float) and value.is_integer(): return str(int(value))
        if isinstance(value, np.integer): return str(int(value))
    except Exception: pass
    return str(value)

def ensure_date(value):
    if value is None: return None
    if isinstance(value, dt.datetime): return value.date()
    if isinstance(value, dt.date): return value
    try: return pd.to_datetime(value).date()
    except Exception: return None

def parse_date_range(value, min_date: dt.date, max_date: dt.date) -> Tuple[dt.date, dt.date]:
    if isinstance(value, (tuple, list)):
        if len(value) == 2: start_date, end_date = value
        elif len(value) == 1: start_date = end_date = value[0]
        else: start_date, end_date = min_date, max_date
    elif value is None: start_date, end_date = min_date, max_date
    else: start_date = end_date = value
    
    start_date = ensure_date(start_date) or min_date
    end_date = ensure_date(end_date) or max_date
    return start_date, end_date

def rgba(hex_color: str, alpha: float) -> str:
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 3: hex_color = "".join([c * 2 for c in hex_color])
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def metric_card(label: str, value: str, subtext: str) -> None:
    st.markdown(f"""<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div><div class="kpi-sub">{subtext}</div></div>""", unsafe_allow_html=True)

def render_header(status_ok: bool) -> None:
    left, right = st.columns([0.72, 0.28])
    with left:
        st.markdown("## 🚕 NYC Analytics")
        st.caption("Gold Layer Explorer · PySpark Data Engineering Platform")
    with right:
        pill_class = "status-pill ok" if status_ok else "status-pill error"
        pill_text = "● DATA PIPELINE READY" if status_ok else "● GOLD DATA MISSING"
        st.markdown(f"""<div class="header-right"><span class="{pill_class}">{pill_text}</span><div class="header-mini">Gold Analytics Layer</div></div>""", unsafe_allow_html=True)

def section_header(title: str, subtitle: str) -> None:
    st.markdown(f"#### {title}")
    st.caption(subtitle)

def zero_kpis():
    return {"total_trips": 0, "total_passengers": 0, "avg_duration": 0.0, "avg_distance": 0.0, "avg_speed": 0.0}


# ============================================================
# SPARK & DATA LOADING
# ============================================================

@st.cache_resource(show_spinner=False)
def get_spark() -> SparkSession:
    return (SparkSession.builder.appName("NYCTaxiDashboard").master("local[1]")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())

@st.cache_data(show_spinner=False, ttl=3600)
def load_date_bounds() -> Tuple[dt.date, dt.date]:
    spark = get_spark()
    row = spark.read.parquet(spark_uri(DAILY_PATH)).select(
        F.min(F.to_date("pickup_date")).alias("min_date"), F.max(F.to_date("pickup_date")).alias("max_date")
    ).first()
    if not row or row["min_date"] is None or row["max_date"] is None:
        today = dt.date.today(); return today, today
    min_d, max_d = row["min_date"], row["max_date"]
    if isinstance(min_d, dt.datetime): min_d = min_d.date()
    if isinstance(max_d, dt.datetime): max_d = max_d.date()
    if min_d > max_d: min_d, max_d = max_d, min_d
    return min_d, max_d

@st.cache_data(show_spinner=False, ttl=3600)
def load_vendor_keys() -> List[str]:
    spark = get_spark()
    rows = spark.read.parquet(spark_uri(VENDOR_PATH)).select("vendor_id").filter(F.col("vendor_id").isNotNull()).distinct().orderBy("vendor_id").limit(1000).collect()
    keys, seen = [], set()
    for r in rows:
        k = vendor_key_from_value(r["vendor_id"])
        if k not in seen: seen.add(k); keys.append(k)
    keys.sort(key=natural_key)
    return keys

def normalize_time_dataframe(pdf: pd.DataFrame, include_hour: bool) -> pd.DataFrame:
    if pdf.empty: return pdf
    pdf = pdf.copy()
    if "pickup_date" in pdf.columns: pdf["pickup_date"] = pd.to_datetime(pdf["pickup_date"], errors="coerce").dt.date
    for col in METRIC_COLUMNS:
        if col in pdf.columns: pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0)
    if include_hour and "pickup_hour" in pdf.columns: pdf["pickup_hour"] = pd.to_numeric(pdf["pickup_hour"], errors="coerce").fillna(0).astype(int)
    subset = ["pickup_date"] + (["pickup_hour"] if include_hour else [])
    subset = [c for c in subset if c in pdf.columns]
    return pdf.dropna(subset=subset).sort_values(subset).reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def load_daily_df(start_iso: str, end_iso: str) -> pd.DataFrame:
    spark = get_spark()
    df = spark.read.parquet(spark_uri(DAILY_PATH)).withColumn("pickup_date", F.to_date("pickup_date")) \
        .filter(F.col("pickup_date").between(F.lit(start_iso), F.lit(end_iso))) \
        .select(DAILY_COLUMNS).orderBy("pickup_date")
    return normalize_time_dataframe(df.toPandas(), False)

@st.cache_data(show_spinner=False, ttl=3600)
def load_hourly_df(start_iso: str, end_iso: str) -> pd.DataFrame:
    spark = get_spark()
    df = spark.read.parquet(spark_uri(HOURLY_PATH)).withColumn("pickup_date", F.to_date("pickup_date")) \
        .filter(F.col("pickup_date").between(F.lit(start_iso), F.lit(end_iso))) \
        .select(HOURLY_COLUMNS).orderBy("pickup_date", "pickup_hour")
    return normalize_time_dataframe(df.toPandas(), True)

@st.cache_data(show_spinner=False, ttl=3600)
def load_vendor_df() -> pd.DataFrame:
    spark = get_spark()
    df = spark.read.parquet(spark_uri(VENDOR_PATH)).select(VENDOR_COLUMNS).filter(F.col("vendor_id").isNotNull()).orderBy("vendor_id").limit(10000)
    pdf = df.toPandas()
    if pdf.empty: return pd.DataFrame(columns=VENDOR_COLUMNS + ["vendor_id_str", "vendor_label"])
    for col in METRIC_COLUMNS: pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0)
    pdf["vendor_id_str"] = pdf["vendor_id"].map(vendor_key_from_value)
    pdf["vendor_label"] = "Vendor " + pdf["vendor_id_str"]
    return pdf.sort_values("vendor_id_str", key=lambda s: s.map(natural_key)).reset_index(drop=True)


# ============================================================
# KPI LOGIC
# ============================================================

def weighted_metric(df: pd.DataFrame, metric: str) -> float:
    if df.empty or metric not in df.columns or "total_trips" not in df.columns: return 0.0
    work = df[[metric, "total_trips"]].copy()
    work[metric] = pd.to_numeric(work[metric], errors="coerce").fillna(0.0)
    work["total_trips"] = pd.to_numeric(work["total_trips"], errors="coerce").fillna(0.0)
    w_sum = work["total_trips"].sum()
    if w_sum <= 0: return float(work[metric].mean()) if not work.empty else 0.0
    return float((work[metric] * work["total_trips"]).sum() / w_sum)

def compute_network_kpis(daily_df: pd.DataFrame):
    if daily_df.empty or not set(DAILY_COLUMNS).issubset(daily_df.columns): return None
    return {
        "total_trips": int(pd.to_numeric(daily_df["total_trips"], errors="coerce").fillna(0).sum()),
        "total_passengers": int(pd.to_numeric(daily_df["total_passengers"], errors="coerce").fillna(0).sum()),
        "avg_duration": weighted_metric(daily_df, "avg_trip_duration_sec"),
        "avg_distance": weighted_metric(daily_df, "avg_trip_distance_km"),
        "avg_speed": weighted_metric(daily_df, "avg_trip_speed_kmh"),
    }

def compute_vendor_kpis(vendor_df: pd.DataFrame, vendor_key: str):
    if vendor_df.empty or "vendor_id_str" not in vendor_df.columns: return None
    row = vendor_df[vendor_df["vendor_id_str"] == vendor_key]
    if row.empty: return None
    r = row.iloc[0]
    def si(v): 
        try: return int(float(v))
        except: return 0
    def sf(v): 
        try: return float(v)
        except: return 0.0
    return {"total_trips": si(r.get("total_trips")), "total_passengers": si(r.get("total_passengers")),
            "avg_duration": sf(r.get("avg_trip_duration_sec")), "avg_distance": sf(r.get("avg_trip_distance_km")),
            "avg_speed": sf(r.get("avg_trip_speed_kmh"))}


# ============================================================
# PLOTLY HELPERS (FIXED FOR UNDEFINED TITLE BUG)
# ============================================================

def apply_base_layout(fig: go.Figure, title=None, x_title=None, y_title=None, height: int = 360, show_legend: bool = False, hover_mode="x unified") -> None:
    """Apply consistent styling. Uses empty strings instead of None to prevent 'undefined' rendering."""
    title_text = title if title else ""
    x_axis_title = x_title if x_title else ""
    y_axis_title = y_title if y_title else ""
    top_margin = 56 if title else 30

    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=COLORS["chart_text"], family="Segoe UI, Inter, Arial, sans-serif", size=12),
        title_text=title_text, title_font=dict(color=COLORS["text"], size=16), title_x=0.02,
        height=height, margin=dict(l=56, r=16, t=top_margin, b=48), showlegend=show_legend,
        hoverlabel=dict(bgcolor=COLORS["panel"], bordercolor=COLORS["border"], font=dict(color=COLORS["text"])),
        colorway=[COLORS["primary"], COLORS["purple"], COLORS["green"], COLORS["warning"]],
        xaxis=dict(title=x_axis_title, gridcolor=COLORS["grid"], zeroline=False, showgrid=True, tickfont=dict(color=COLORS["muted"])),
        yaxis=dict(title=y_axis_title, gridcolor=COLORS["grid"], zeroline=False, showgrid=True, tickfont=dict(color=COLORS["muted"])),
    )
    if hover_mode: fig.update_layout(hovermode=hover_mode)

def empty_figure(message: str = "No data available for the selected filters.") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, font=dict(color=COLORS["muted"], size=14))
    apply_base_layout(fig, height=300, hover_mode=None)
    fig.update_xaxes(visible=False); fig.update_yaxes(visible=False)
    return fig

def time_series_chart(df, x_col, y_col, color, y_title, height=360, hover_y_format=":,.2f", title=None):
    if df.empty: return empty_figure()
    safe_y = y_title or "Metric"
    hover = "%{x|%Y-%m-%d}: %{y" + hover_y_format + "}<extra></extra>"
    fig = go.Figure(go.Scatter(x=df[x_col], y=df[y_col], mode="lines", line=dict(color=color, width=2.5),
                               fill="tozeroy", fillcolor=rgba(color, 0.08), name=safe_y, hovertemplate=hover))
    apply_base_layout(fig, title=title, y_title=safe_y, height=height)
    return fig

def category_line_chart(x_vals, y_vals, color, y_title, height=320, hover_y_format=":,.2f", title=None):
    if len(x_vals) == 0: return empty_figure()
    safe_y = y_title or "Metric"
    hover = "%{x}: %{y" + hover_y_format + "}<extra></extra>"
    fig = go.Figure(go.Scatter(x=x_vals, y=y_vals, mode="lines+markers", line=dict(color=color, width=2.5),
                               marker=dict(size=6), name=safe_y, hovertemplate=hover))
    apply_base_layout(fig, title=title, y_title=safe_y, height=height)
    return fig

def bar_chart(x_vals, y_vals, color, y_title, height=360, hover_y_format=":,.0f", title=None, x_title=None):
    if len(x_vals) == 0: return empty_figure()
    safe_y = y_title or "Metric"
    hover = "%{x}: %{y" + hover_y_format + "}<extra></extra>"
    fig = go.Figure(go.Bar(x=x_vals, y=y_vals, marker_color=color, marker_line_width=0, hovertemplate=hover))
    apply_base_layout(fig, title=title, x_title=x_title, y_title=safe_y, height=height)
    return fig

def vendor_bar_chart(vendor_df, y_col, y_title, color, selected_key, hover_y_format=":,.0f", height=360, title=None):
    if vendor_df.empty: return empty_figure()
    safe_y = y_title or "Metric"
    if selected_key == ALL_VENDOR_KEY: colors = [color] * len(vendor_df)
    else: colors = [color if vk == selected_key else rgba(COLORS["muted"], 0.25) for vk in vendor_df["vendor_id_str"]]
    return bar_chart(vendor_df["vendor_label"], vendor_df[y_col], colors, safe_y, height, hover_y_format, title, "Vendor")

def aggregate_hourly(hourly_df):
    if hourly_df.empty: return pd.DataFrame(columns=["pickup_hour","total_trips","total_passengers","avg_trip_duration_sec","avg_trip_distance_km","avg_trip_speed_kmh"])
    df = hourly_df.copy()
    df["dw"] = df["avg_trip_duration_sec"] * df["total_trips"]
    df["diw"] = df["avg_trip_distance_km"] * df["total_trips"]
    df["sw"] = df["avg_trip_speed_kmh"] * df["total_trips"]
    agg = df.groupby("pickup_hour").agg(total_trips=("total_trips","sum"), total_passengers=("total_passengers","sum"),
                                        dw=("dw","sum"), diw=("diw","sum"), sw=("sw","sum")).reset_index()
    agg = agg.set_index("pickup_hour").reindex(range(24), fill_value=0).reset_index().rename(columns={"index":"pickup_hour"})
    agg["avg_trip_duration_sec"] = np.where(agg["total_trips"]>0, agg["dw"]/agg["total_trips"], 0.0)
    agg["avg_trip_distance_km"] = np.where(agg["total_trips"]>0, agg["diw"]/agg["total_trips"], 0.0)
    agg["avg_trip_speed_kmh"] = np.where(agg["total_trips"]>0, agg["sw"]/agg["total_trips"], 0.0)
    return agg[["pickup_hour","total_trips","total_passengers","avg_trip_duration_sec","avg_trip_distance_km","avg_trip_speed_kmh"]]

def make_heatmap(hourly_df):
    if hourly_df.empty: return pd.DataFrame()
    pivot = hourly_df.pivot_table(index="pickup_date", columns="pickup_hour", values="total_trips", aggfunc="sum", fill_value=0)
    return pivot.reindex(columns=range(24), fill_value=0).sort_index()

def heatmap_chart(pivot):
    if pivot.empty: return empty_figure()
    x_l = [f"{int(h):02d}:00" for h in pivot.columns]
    y_l = [d.strftime("%Y-%m-%d") if isinstance(d, dt.date) else str(d) for d in pivot.index]
    fig = go.Figure(go.Heatmap(z=pivot.values, x=x_l, y=y_l, colorscale=[[0,"#0D1726"],[0.45,"#38BDF8"],[1,"#8B5CF6"]],
                               hovertemplate="Date %{y}<br>Hour %{x}<br>Trips %{z:,.0f}<extra></extra>",
                               colorbar=dict(outlinewidth=0, tickfont=dict(color=COLORS["muted"]))))
    apply_base_layout(fig, x_title="Hour", y_title="Date", height=max(320, 18*len(y_l)+120), hover_mode="closest")
    fig.update_yaxes(autorange="reversed")
    return fig


# ============================================================
# TAB RENDERING
# ============================================================

def render_overview(daily_df, selected_vendor_key):
    if daily_df.empty: st.info("No data available for the selected filters."); return
    if selected_vendor_key != ALL_VENDOR_KEY:
        st.info("Daily time-series charts use the all-vendor `daily_metrics` dataset. `vendor_metrics` does not contain `pickup_date`, so vendor-specific time-series data is not available.")
    st.markdown("**Trip Volume**")
    st.plotly_chart(time_series_chart(daily_df, "pickup_date", "total_trips", COLORS["primary"], "Total trips", 380, ":,.0f"), use_container_width=True, config=PLOT_CONFIG)
    st.markdown("**Performance**")
    c1, c2, c3 = st.columns(3)
    with c1: st.plotly_chart(time_series_chart(daily_df, "pickup_date", "avg_trip_distance_km", COLORS["green"], "Avg distance (km)", 300, ":,.2f"), use_container_width=True, config=PLOT_CONFIG)
    with c2: st.plotly_chart(time_series_chart(daily_df, "pickup_date", "avg_trip_speed_kmh", COLORS["purple"], "Avg speed (km/h)", 300, ":,.2f"), use_container_width=True, config=PLOT_CONFIG)
    with c3: st.plotly_chart(time_series_chart(daily_df, "pickup_date", "avg_trip_duration_sec", COLORS["warning"], "Avg duration (sec)", 300, ":,.0f"), use_container_width=True, config=PLOT_CONFIG)

def render_daily_trends(daily_df, selected_vendor_key):
    if selected_vendor_key != ALL_VENDOR_KEY: st.info("Vendor filter is not applied to daily trends because `daily_metrics` does not contain `vendor_id`.")
    if daily_df.empty: st.info("No data available for the selected filters."); return
    mc = st.selectbox("Metric", ["Trip Volume", "Avg Distance", "Avg Speed", "Avg Duration"], key="daily_trend_metric")
    mm = {"Trip Volume": ("total_trips", COLORS["primary"], "Total trips", ":,.0f"), "Avg Distance": ("avg_trip_distance_km", COLORS["green"], "Avg distance (km)", ":,.2f"),
          "Avg Speed": ("avg_trip_speed_kmh", COLORS["purple"], "Avg speed (km/h)", ":,.2f"), "Avg Duration": ("avg_trip_duration_sec", COLORS["warning"], "Avg duration (sec)", ":,.0f")}
    yc, co, yt, hf = mm[mc]
    st.plotly_chart(time_series_chart(daily_df, "pickup_date", yc, co, yt, 420, hf), use_container_width=True, config=PLOT_CONFIG)
    st.caption("Source: `daily_metrics` · Grain: one row per `pickup_date` · Vendor scope: all vendors only.")

def render_hourly_analysis(hourly_df, selected_vendor_key):
    if selected_vendor_key != ALL_VENDOR_KEY: st.info("Vendor filter is not applied to hourly analysis because `hourly_metrics` does not contain `vendor_id`.")
    if hourly_df.empty: st.info("No data available for the selected filters."); return
    ha = aggregate_hourly(hourly_df)
    hl = [f"{int(h):02d}:00" for h in ha["pickup_hour"]]
    st.markdown("**Trips by Hour**")
    st.plotly_chart(bar_chart(hl, ha["total_trips"], COLORS["primary"], "Total trips", 340, ":,.0f", x_title="Pickup hour"), use_container_width=True, config=PLOT_CONFIG)
    c1, c2 = st.columns(2)
    with c1: st.plotly_chart(category_line_chart(hl, ha["avg_trip_speed_kmh"], COLORS["purple"], "Avg speed (km/h)", 320, ":,.2f"), use_container_width=True, config=PLOT_CONFIG)
    with c2: st.plotly_chart(category_line_chart(hl, ha["avg_trip_distance_km"], COLORS["green"], "Avg distance (km)", 320, ":,.2f"), use_container_width=True, config=PLOT_CONFIG)
    st.markdown("**Daily Heatmap**")
    pv = make_heatmap(hourly_df)
    if pv.empty: st.info("No heatmap data available.")
    elif pv.shape[0] > MAX_HEATMAP_DAYS: st.info(f"Heatmap hidden because the selected date range contains more than {MAX_HEATMAP_DAYS} days. Narrow the date range to view the heatmap.")
    else: st.plotly_chart(heatmap_chart(pv), use_container_width=True, config=PLOT_CONFIG)

def render_vendor_analysis(vendor_df, selected_vendor_key):
    st.info("Vendor metrics are overall totals and averages. They cannot be filtered by date because `vendor_metrics` does not contain `pickup_date`.")
    if vendor_df.empty: st.info("No vendor metrics available."); return
    c1, c2 = st.columns(2)
    with c1: st.plotly_chart(vendor_bar_chart(vendor_df, "total_trips", "Total trips", COLORS["primary"], selected_vendor_key, ":,.0f", 360), use_container_width=True, config=PLOT_CONFIG)
    with c2: st.plotly_chart(vendor_bar_chart(vendor_df, "total_passengers", "Total passengers", COLORS["purple"], selected_vendor_key, ":,.0f", 360), use_container_width=True, config=PLOT_CONFIG)
    st.markdown("**Performance Comparison**")
    c1, c2, c3 = st.columns(3)
    with c1: st.plotly_chart(vendor_bar_chart(vendor_df, "avg_trip_duration_sec", "Avg duration (sec)", COLORS["warning"], selected_vendor_key, ":,.0f", 320), use_container_width=True, config=PLOT_CONFIG)
    with c2: st.plotly_chart(vendor_bar_chart(vendor_df, "avg_trip_distance_km", "Avg distance (km)", COLORS["green"], selected_vendor_key, ":,.2f", 320), use_container_width=True, config=PLOT_CONFIG)
    with c3: st.plotly_chart(vendor_bar_chart(vendor_df, "avg_trip_speed_kmh", "Avg speed (km/h)", COLORS["primary"], selected_vendor_key, ":,.2f", 320), use_container_width=True, config=PLOT_CONFIG)
    st.markdown("**Vendor Dataset**")
    st.dataframe(vendor_df[["vendor_id_str"] + METRIC_COLUMNS].rename(columns={"vendor_id_str": "vendor_id"}), use_container_width=True, hide_index=True)

def render_data_explorer(daily_df, hourly_df, vendor_df, selected_vendor_key):
    ds = st.selectbox("Gold dataset", ["Daily Metrics", "Hourly Metrics", "Vendor Metrics"], key="data_explorer_dataset")
    if ds == "Daily Metrics": df = daily_df; st.caption("Date filter applied. Vendor filter is not applied because `daily_metrics` does not contain `vendor_id`.")
    elif ds == "Hourly Metrics": df = hourly_df; st.caption("Date filter applied. Vendor filter is not applied because `hourly_metrics` does not contain `vendor_id`.")
    else:
        w = vendor_df.copy()
        if selected_vendor_key != ALL_VENDOR_KEY: w = w[w["vendor_id_str"] == selected_vendor_key]
        df = w[VENDOR_COLUMNS] if set(VENDOR_COLUMNS).issubset(w.columns) else w
        st.caption("Date filter is not applied because `vendor_metrics` does not contain `pickup_date`.")
    if df.empty: st.info("No data available for the selected filters."); return
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", f"{len(df):,}"); c2.metric("Columns", f"{len(df.columns):,}"); c3.metric("Preview", "First 500")
    with st.expander("Columns and data types", expanded=False):
        st.dataframe(pd.DataFrame({"column": df.columns, "type": [str(t) for t in df.dtypes]}), use_container_width=True, hide_index=True)
    st.dataframe(df.head(500), use_container_width=True, hide_index=True)
    if len(df) > 500: st.caption("Showing first 500 rows.")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    missing, empty = validate_gold_data()
    load_error, date_bounds, vendor_keys, vendor_df = None, (dt.date.today(), dt.date.today()), [], pd.DataFrame()
    
    if not missing and not empty:
        try:
            date_bounds = load_date_bounds(); vendor_keys = load_vendor_keys(); vendor_df = load_vendor_df()
        except Exception as e: load_error = str(e)

    status_ok = not missing and not empty and load_error is None
    render_header(status_ok)

    if missing or empty:
        st.error("Gold analytics data is unavailable.")
        if missing: st.write("Missing directories:", missing)
        if empty: st.write("Empty directories with no Parquet files:", empty)
        st.stop()
    if load_error:
        st.error("Unable to initialize Spark or read the Gold layer.")
        st.caption("Check PySpark/Java configuration and verify that the Gold Parquet files are valid.")
        st.stop()

    min_date, max_date = date_bounds
    with st.sidebar:
        st.markdown("### ANALYTICS CONTROLS"); st.caption("Explore the Gold layer through interactive analytics.")
        dr = st.sidebar.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date, help="Filters daily and hourly network metrics.")
        sd, ed = parse_date_range(dr, min_date, max_date)
        if sd > ed: st.sidebar.error("Start date must be earlier than or equal to end date."); sd, ed = min_date, max_date
        st.caption(f"Selected: {sd:%Y/%m/%d} → {ed:%Y/%m/%d}")
        
        vo = [ALL_VENDOR_KEY] + vendor_keys
        svk = st.sidebar.selectbox("Vendor", options=vo, format_func=lambda k: "All Vendors" if k == ALL_VENDOR_KEY else f"Vendor {k}",
                                   help="Vendor metrics are overall metrics from vendor_metrics. They are not date-filterable because vendor_metrics has no pickup_date.")
        st.divider(); st.markdown("**DATA SOURCES**"); st.markdown("🟦 Daily Metrics"); st.markdown("🟪 Hourly Metrics"); st.markdown("🟩 Vendor Metrics")
        st.divider(); st.markdown("**PIPELINE STATUS**"); st.success("Gold layer available")
        st.caption(f"Date coverage: {min_date:%Y/%m/%d} – {max_date:%Y/%m/%d}")
        if vendor_keys: st.caption(f"Vendors detected: {len(vendor_keys)}")

    try: daily_df = load_daily_df(sd.isoformat(), ed.isoformat()); hourly_df = load_hourly_df(sd.isoformat(), ed.isoformat())
    except Exception: daily_df, hourly_df = pd.DataFrame(), pd.DataFrame(); st.error("No data available for the selected filters.")

    section_header("Network Overview", "High-level performance across the selected analytics window.")
    if svk != ALL_VENDOR_KEY: st.info("Vendor selected: KPI cards use overall `vendor_metrics`. `vendor_metrics` has no `pickup_date`, so the date range is not applied to vendor KPIs.")
    
    kpis = compute_network_kpis(daily_df) if svk == ALL_VENDOR_KEY else compute_vendor_kpis(vendor_df, svk)
    if kpis is None: st.warning("No data available for the selected filters."); kpis = zero_kpis()

    cols = st.columns(5)
    with cols[0]: metric_card("Total Trips", f"{kpis['total_trips']:,}", "Completed rides")
    with cols[1]: metric_card("Passengers", f"{kpis['total_passengers']:,}", "Recorded passengers")
    with cols[2]: metric_card("Avg Duration", f"{kpis['avg_duration']:,.0f} sec", "Trip duration")
    with cols[3]: metric_card("Avg Distance", f"{kpis['avg_distance']:,.2f} km", "Trip distance")
    with cols[4]: metric_card("Avg Speed", f"{kpis['avg_speed']:,.2f} km/h", "Average speed")

    t1, t2, t3, t4, t5 = st.tabs(["Overview", "Daily Trends", "Hourly Analysis", "Vendor Analysis", "Data Explorer"])
    with t1: render_overview(daily_df, svk)
    with t2: render_daily_trends(daily_df, svk)
    with t3: render_hourly_analysis(hourly_df, svk)
    with t4: render_vendor_analysis(vendor_df, svk)
    with t5: render_data_explorer(daily_df, hourly_df, vendor_df, svk)

main()