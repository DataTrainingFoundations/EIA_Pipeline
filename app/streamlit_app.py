import streamlit as st

from data_access import (
    get_backfill_status,
    get_connection,
    get_grid_operations_coverage,
    get_planning_coverage,
    get_summary_coverage,
    table_has_rows,
)


def _room_card(title: str, summary: str, bullets: list[str], tone: str) -> str:
    bullet_html = "".join(f"<li>{item}</li>" for item in bullets)
    return f"""
    <div class="landing-card {tone}">
        <div class="landing-card-title">{title}</div>
        <div class="landing-card-copy">{summary}</div>
        <ul>{bullet_html}</ul>
    </div>
    """


st.set_page_config(page_title="EIA Analytics", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top right, rgba(24,95,165,0.10), transparent 24%),
            linear-gradient(180deg, #f8fafc 0%, #edf4fb 100%);
    }
    .landing-hero {
        background: linear-gradient(135deg, #123b61 0%, #1d5f96 55%, #4d88b8 100%);
        color: #f8fafc;
        border-radius: 22px;
        padding: 1.45rem 1.55rem;
        margin-bottom: 1rem;
        box-shadow: 0 16px 34px rgba(18,59,97,0.18);
    }
    .landing-kicker {
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-size: 0.74rem;
        opacity: 0.8;
        margin-bottom: 0.5rem;
    }
    .landing-title {
        font-size: 2.05rem;
        font-weight: 700;
        line-height: 1.12;
        margin-bottom: 0.5rem;
    }
    .landing-copy {
        max-width: 60rem;
        font-size: 1rem;
        opacity: 0.94;
    }
    .landing-card {
        background: rgba(255,255,255,0.95);
        border: 1px solid #d8e3ef;
        border-radius: 18px;
        padding: 1.05rem 1.1rem;
        min-height: 242px;
        box-shadow: 0 10px 22px rgba(16,38,58,0.07);
    }
    .landing-grid { border-top: 4px solid #1d9e75; }
    .landing-strategy { border-top: 4px solid #c65d2e; }
    .landing-planning { border-top: 4px solid #ef9f27; }
    .landing-card-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #10263a;
        margin-bottom: 0.4rem;
    }
    .landing-card-copy {
        color: #4d5f73;
        line-height: 1.45;
        margin-bottom: 0.8rem;
    }
    .landing-card ul {
        margin: 0;
        padding-left: 1.15rem;
        color: #4d5f73;
    }
    .landing-card li { margin-bottom: 0.4rem; }
    .landing-status {
        border: 1px solid #d8e3ef;
        border-radius: 16px;
        padding: 0.95rem 1rem;
        background: rgba(255,255,255,0.92);
        color: #10263a;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select now()")
            server_time = cur.fetchone()[0]
except Exception as exc:  # pragma: no cover
    st.error(f"Database connection failed: {exc}")
    st.stop()

st.markdown(
    """
    <div class="landing-hero">
        <div class="landing-kicker">Decision support</div>
        <div class="landing-title">Choose the decision room that matches the horizon.</div>
        <div class="landing-copy">
            Grid Operations Manager is for immediate operating pressure. Utility Strategy Director
            is for board-facing transition posture. Resource Planning Lead is for structural
            planning follow-through.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

status_left, status_right = st.columns([1.2, 1])
status_left.markdown(
    f"""
    <div class="landing-status">
        <strong>Platform connected.</strong><br>
        PostgreSQL server time: {server_time}
    </div>
    """,
    unsafe_allow_html=True,
)
status_right.markdown(
    """
    <div class="landing-status">
        <strong>Navigation note.</strong><br>
        Open a page from the links below or use the Streamlit sidebar page list.
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Decision rooms")
room1, room2, room3 = st.columns(3)

with room1:
    st.markdown(
        _room_card(
            "Grid Operations Manager",
            "Short-horizon triage for demand stress, coverage gaps, and active alerts.",
            [
                "Identify which respondents need intervention now.",
                "Validate forecast misses, ramp stress, and alert evidence.",
                "Use trends after the current watchlist is clear.",
            ],
            "landing-grid",
        ),
        unsafe_allow_html=True,
    )
    if hasattr(st, "page_link"):
        st.page_link("pages/grid_operations_manager.py", label="Open Grid Operations Manager")

with room2:
    st.markdown(
        _room_card(
            "Utility Strategy Director",
            "Board-facing transition view for structural portfolio exposure and resilience.",
            [
                "Spot utilities with the biggest transition gaps.",
                "Compare carbon exposure, clean coverage, and gas reliance.",
                "Use the board packet to explain one utility in detail.",
            ],
            "landing-strategy",
        ),
        unsafe_allow_html=True,
    )
    if hasattr(st, "page_link"):
        st.page_link("pages/utility_strategy_director.py", label="Open Utility Strategy Director")

with room3:
    st.markdown(
        _room_card(
            "Resource Planning Lead",
            "Structural planning watchlist for medium-horizon action and follow-through.",
            [
                "Rank which respondents need planning attention first.",
                "Explain the primary driver behind each priority label.",
                "Use supporting evidence only after the watchlist is clear.",
            ],
            "landing-planning",
        ),
        unsafe_allow_html=True,
    )
    if hasattr(st, "page_link"):
        st.page_link("pages/resource_planning_lead.py", label="Open Resource Planning Lead")

st.divider()

st.subheader("Coverage snapshot")
coverage_col1, coverage_col2, coverage_col3 = st.columns(3)

if table_has_rows():
    coverage = get_summary_coverage()
    coverage_col1.metric("Core daily rows", f"{int(coverage['row_count']):,}")
    coverage_col1.caption(f"{coverage['min_date']} to {coverage['max_date']}")
else:
    coverage_col1.info("Core daily coverage is unavailable.")

if table_has_rows("platinum.grid_operations_hourly"):
    ops_coverage = get_grid_operations_coverage()
    coverage_col2.metric("Ops hourly rows", f"{int(ops_coverage['row_count']):,}")
    coverage_col2.caption(f"{ops_coverage['min_period']} to {ops_coverage['max_period']}")
else:
    coverage_col2.info("Grid operations coverage is unavailable.")

if table_has_rows("platinum.resource_planning_daily"):
    planning_coverage = get_planning_coverage()
    coverage_col3.metric("Planning daily rows", f"{int(planning_coverage['row_count']):,}")
    coverage_col3.caption(
        f"{planning_coverage['min_date']} to {planning_coverage['max_date']}"
    )
else:
    coverage_col3.info("Planning coverage is unavailable.")

st.subheader("Operational Status")
with st.expander("Backfill Status", expanded=False):
    status_df = get_backfill_status()
    if status_df.empty:
        st.info("No backfill jobs have been queued yet.")
    else:
        st.dataframe(status_df, use_container_width=True, hide_index=True)
