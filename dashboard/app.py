"""Read-only Streamlit showcase for the 5minPATH research tool."""

from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st
from sample_data import CLEAN_DAYS, FAILURE_HIERARCHY

st.set_page_config(
    page_title="5minPATH Dashboard",
    page_icon="5",
    layout="wide",
)


def inject_style() -> None:
    st.markdown(
        """
        <style>
        :root {
            --ink: #181510;
            --muted: rgba(24, 21, 16, 0.68);
            --paper: #f5ecd8;
            --card: rgba(255, 252, 243, 0.82);
            --card-strong: rgba(255, 248, 232, 0.94);
            --line: rgba(24, 21, 16, 0.13);
            --amber: #b86225;
            --rust: #7f351c;
            --moss: #53633e;
            --slate: #33424d;
            --gold: #d79a32;
        }

        .stApp {
            background:
                radial-gradient(circle at 12% 9%, rgba(215, 154, 50, 0.28), transparent 28rem),
                radial-gradient(circle at 88% 12%, rgba(83, 99, 62, 0.24), transparent 32rem),
                linear-gradient(135deg, #fbf8ef 0%, var(--paper) 48%, #ead9ba 100%);
            color: var(--ink);
        }

        .block-container {
            max-width: 1220px;
            padding-top: 2.4rem;
            padding-bottom: 4rem;
        }

        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color: var(--ink);
            letter-spacing: -0.045em;
        }

        div[data-testid="stMetric"] {
            background: transparent;
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--line);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 18px 55px rgba(76, 53, 22, 0.08);
        }

        .hero {
            position: relative;
            overflow: hidden;
            border: 1px solid var(--line);
            border-radius: 34px;
            padding: 2.8rem;
            background:
                linear-gradient(120deg, rgba(255, 252, 243, 0.92), rgba(255, 248, 230, 0.72)),
                radial-gradient(circle at 100% 0%, rgba(127, 53, 28, 0.16), transparent 26rem);
            box-shadow: 0 28px 90px rgba(76, 53, 22, 0.14);
        }

        .hero::after {
            content: "";
            position: absolute;
            right: -5rem;
            top: -5rem;
            width: 20rem;
            height: 20rem;
            border-radius: 50%;
            border: 1px solid rgba(24, 21, 16, 0.14);
            box-shadow:
                -3.5rem 5rem 0 rgba(83, 99, 62, 0.10),
                -8rem 8rem 0 rgba(184, 98, 37, 0.08);
        }

        .eyebrow {
            color: var(--amber);
            font-weight: 800;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            font-size: 0.76rem;
            margin-bottom: 0.5rem;
        }

        .hero-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: clamp(4rem, 9vw, 7.2rem);
            line-height: 0.88;
            margin: 0;
            max-width: 760px;
        }

        .hero-subtitle {
            color: var(--muted);
            font-size: 1.22rem;
            line-height: 1.65;
            max-width: 760px;
            margin: 1.25rem 0 0;
        }

        .verdict {
            display: inline-flex;
            gap: 0.6rem;
            align-items: center;
            margin-top: 1.4rem;
            padding: 0.75rem 1rem;
            border: 1px solid rgba(127, 53, 28, 0.22);
            border-radius: 999px;
            background: rgba(127, 53, 28, 0.08);
            color: var(--rust);
            font-weight: 800;
        }

        .section-shell {
            margin-top: 2.3rem;
            padding: 2rem;
            border: 1px solid var(--line);
            border-radius: 28px;
            background: rgba(255, 252, 243, 0.58);
            box-shadow: 0 18px 60px rgba(76, 53, 22, 0.08);
        }

        .section-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 2.35rem;
            line-height: 1.05;
            margin: 0 0 0.6rem;
        }

        .section-copy {
            color: var(--muted);
            font-size: 1.02rem;
            line-height: 1.62;
            max-width: 820px;
            margin-bottom: 1.2rem;
        }

        .metric-card,
        .insight-card,
        .step-card,
        .failure-card {
            border: 1px solid var(--line);
            border-radius: 22px;
            background: var(--card);
            box-shadow: 0 18px 55px rgba(76, 53, 22, 0.08);
        }

        .metric-card {
            min-height: 136px;
            padding: 1.2rem;
        }

        .metric-value {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 2.65rem;
            font-weight: 700;
            line-height: 1;
            letter-spacing: -0.06em;
        }

        .metric-label {
            color: var(--muted);
            font-size: 0.92rem;
            margin-top: 0.65rem;
        }

        .metric-note {
            color: var(--amber);
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            margin-bottom: 0.55rem;
        }

        .insight-card {
            min-height: 178px;
            padding: 1.35rem;
            background: var(--card-strong);
        }

        .insight-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.42rem;
            line-height: 1.1;
            margin-bottom: 0.8rem;
        }

        .insight-copy {
            color: var(--muted);
            line-height: 1.58;
        }

        .pipeline {
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 1rem;
        }

        .step-card {
            padding: 1.1rem;
            min-height: 165px;
        }

        .step-index {
            width: 2.4rem;
            height: 2.4rem;
            display: grid;
            place-items: center;
            border-radius: 999px;
            background: var(--ink);
            color: #fff8e9;
            font-weight: 800;
            margin-bottom: 1rem;
        }

        .step-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.15rem;
            line-height: 1.15;
            margin-bottom: 0.65rem;
        }

        .step-copy {
            color: var(--muted);
            font-size: 0.92rem;
            line-height: 1.45;
        }

        .bar-table {
            display: grid;
            gap: 0.75rem;
        }

        .bar-row {
            display: grid;
            grid-template-columns: 6.5rem 1fr 6rem 9.5rem;
            align-items: center;
            gap: 1rem;
            padding: 0.9rem 1rem;
            border: 1px solid var(--line);
            border-radius: 18px;
            background: rgba(255, 252, 243, 0.72);
        }

        .day-label {
            font-weight: 900;
        }

        .track {
            height: 0.95rem;
            border-radius: 999px;
            background: rgba(24, 21, 16, 0.10);
            overflow: hidden;
        }

        .fill {
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, var(--amber), var(--rust));
        }

        .pct {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.4rem;
            font-weight: 800;
            letter-spacing: -0.04em;
        }

        .tag {
            display: inline-flex;
            justify-content: center;
            padding: 0.35rem 0.7rem;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 850;
            border: 1px solid rgba(24, 21, 16, 0.14);
            background: rgba(255, 255, 255, 0.48);
        }

        .tag-strong {
            color: #2f4b20;
            background: rgba(83, 99, 62, 0.13);
        }

        .tag-middle,
        .tag-lower-middle {
            color: #76501a;
            background: rgba(215, 154, 50, 0.16);
        }

        .tag-weak {
            color: #7f351c;
            background: rgba(127, 53, 28, 0.12);
        }

        .failure-card {
            display: grid;
            grid-template-columns: 3rem 1fr;
            gap: 1rem;
            padding: 1.2rem;
            min-height: 145px;
        }

        .failure-rank {
            width: 2.6rem;
            height: 2.6rem;
            border-radius: 18px;
            background: var(--moss);
            color: #fff8e9;
            display: grid;
            place-items: center;
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.5rem;
            font-weight: 800;
        }

        .failure-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.32rem;
            line-height: 1.1;
            margin-bottom: 0.55rem;
        }

        .failure-copy {
            color: var(--muted);
            line-height: 1.48;
        }

        .callout {
            border: 1px solid rgba(127, 53, 28, 0.22);
            border-left: 7px solid var(--rust);
            border-radius: 24px;
            padding: 1.35rem 1.45rem;
            background: rgba(127, 53, 28, 0.08);
        }

        .callout strong {
            color: var(--rust);
        }

        @media (max-width: 900px) {
            .pipeline {
                grid-template-columns: 1fr;
            }

            .bar-row {
                grid-template-columns: 1fr;
                gap: 0.55rem;
            }

            .hero {
                padding: 1.55rem;
            }

            .section-shell {
                padding: 1.2rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def section_open(eyebrow: str, title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="section-shell">
          <div class="eyebrow">{escape(eyebrow)}</div>
          <div class="section-title">{escape(title)}</div>
          <div class="section-copy">{escape(body)}</div>
        """,
        unsafe_allow_html=True,
    )


def section_close() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def metric_card(note: str, value: str, label: str) -> str:
    return f"""
    <div class="metric-card">
      <div class="metric-note">{escape(note)}</div>
      <div class="metric-value">{escape(value)}</div>
      <div class="metric-label">{escape(label)}</div>
    </div>
    """


def insight_card(title: str, body: str) -> str:
    return f"""
    <div class="insight-card">
      <div class="insight-title">{escape(title)}</div>
      <div class="insight-copy">{escape(body)}</div>
    </div>
    """


def render_pipeline() -> None:
    steps = [
        (
            "Capture",
            "Collects raw Polymarket, oracle, and venue quote state for bounded sessions.",
        ),
        (
            "Normalize",
            "Converts raw feeds into replayable datasets with explicit session artifacts.",
        ),
        (
            "Replay",
            "Rebuilds historical market state and applies the frozen research contract.",
        ),
        (
            "Calibrate",
            "Compares raw replay against calibrated replay to test whether signal improves.",
        ),
        (
            "Shadow",
            "Measures whether modeled edge survives live-forward, execution-side conditions.",
        ),
    ]
    cards = []
    for index, (title, body) in enumerate(steps, start=1):
        cards.append(
            f"""
            <div class="step-card">
              <div class="step-index">{index}</div>
              <div class="step-title">{escape(title)}</div>
              <div class="step-copy">{escape(body)}</div>
            </div>
            """
        )
    st.markdown(f'<div class="pipeline">{"".join(cards)}</div>', unsafe_allow_html=True)


def render_survival_bars(days: pd.DataFrame) -> None:
    max_value = float(days["survival_pct"].max())
    rows = []
    for row in days.to_dict("records"):
        pct = float(row["survival_pct"])
        width = 100.0 if max_value == 0 else max(2.5, pct / max_value * 100.0)
        tag_class = str(row["classification"]).lower().replace(" ", "-")
        rows.append(
            f"""
            <div class="bar-row">
              <div class="day-label">{escape(str(row["day"]))}</div>
              <div class="track"><div class="fill" style="width: {width:.2f}%;"></div></div>
              <div class="pct">{pct:.2f}%</div>
              <div><span class="tag tag-{escape(tag_class)}">
                {escape(str(row["classification"]))}
              </span></div>
            </div>
            """
        )
    st.markdown(f'<div class="bar-table">{"".join(rows)}</div>', unsafe_allow_html=True)


def render_failure_cards(failures: pd.DataFrame) -> None:
    cols = st.columns(3)
    for col, row in zip(cols, failures.to_dict("records"), strict=True):
        with col:
            st.markdown(
                f"""
                <div class="failure-card">
                  <div class="failure-rank">{int(row["rank"])}</div>
                  <div>
                    <div class="failure-title">{escape(str(row["drag"]))}</div>
                    <div class="failure-copy">{escape(str(row["summary"]))}</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


inject_style()

days = pd.DataFrame(CLEAN_DAYS)
failure_hierarchy = pd.DataFrame(FAILURE_HIERARCHY)

mean_survival = days["survival_pct"].mean()
median_survival = days["survival_pct"].median()
max_survival = days["survival_pct"].max()
strong_days = int((days["classification"] == "Strong").sum())

st.markdown(
    """
    <div class="hero">
      <div class="eyebrow">5-minute prediction-market research</div>
      <h1 class="hero-title">5minPATH</h1>
      <p class="hero-subtitle">
        A reusable capture, replay, calibration, and live-forward shadow
        measurement engine for testing whether modeled edge survives real
        market conditions.
      </p>
      <div class="verdict">Validated measurement engine. No deployment recommendation.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.write("")

metric_cols = st.columns(4)
metric_items = [
    ("Outcome", "Built", "Research/data pipeline"),
    ("Replay signal", "Real", "Calibration repeatedly helped"),
    ("Live survival", f"{median_survival:.1f}%", "Median across clean days"),
    ("Deployment", "No", "Current strategy too inconsistent"),
]
for col, item in zip(metric_cols, metric_items, strict=True):
    with col:
        st.markdown(metric_card(*item), unsafe_allow_html=True)

section_open(
    "System",
    "What this tool does",
    "The repo is a research pipeline, not an execution product. It helps a user gather "
    "market data, replay it, test calibration, and measure live-forward survival.",
)
render_pipeline()
section_close()

section_open(
    "Result",
    "The core finding",
    "The project found legitimate replay signal, but the tested strategy did not convert "
    "that signal into consistent live-shadow economics.",
)
insight_cols = st.columns(3)
insights = [
    (
        "What worked",
        "Runtime cleanliness repeated, capture became stable, and calibration improved "
        "replay economics across multiple sessions.",
    ),
    (
        "What did not",
        "Modeled edge survived live conditions inconsistently. Day 7 was strong, but most "
        "clean days were weak-to-middle.",
    ),
    (
        "Why it matters",
        "The tool is valuable because it tells the truth about conversion from replay edge "
        "to live-forward tradability.",
    ),
]
for col, item in zip(insight_cols, insights, strict=True):
    with col:
        st.markdown(insight_card(*item), unsafe_allow_html=True)
section_close()

section_open(
    "Evidence",
    "Six clean-shadow day comparison",
    "The visual center of the project is the survival distribution. Day 7 was real, but it "
    "did not become the norm.",
)
evidence_cols = st.columns(3)
evidence_cards = [
    ("Mean survival", f"{mean_survival:.1f}%", "Pulled upward by Day 7"),
    ("Median survival", f"{median_survival:.1f}%", "Better picture of normal days"),
    ("Best day", f"{max_survival:.1f}%", "Day 7 strong-survival outlier"),
]
for col, item in zip(evidence_cols, evidence_cards, strict=True):
    with col:
        st.markdown(metric_card(*item), unsafe_allow_html=True)

st.write("")
render_survival_bars(days)
st.write("")

display_days = days.rename(
    columns={
        "day": "Day",
        "survival_pct": "Edge survival %",
        "classification": "Classification",
        "main_drag": "Main drag",
        "trusted_venue_rate_pct": "3-trusted-venue rate %",
        "side_match_rate_pct": "Side-match rate %",
    }
)
st.dataframe(
    display_days[
        [
            "Day",
            "Edge survival %",
            "Classification",
            "Main drag",
            "3-trusted-venue rate %",
            "Side-match rate %",
        ]
    ],
    hide_index=True,
    use_container_width=True,
)
section_close()

section_open(
    "Failure anatomy",
    "What blocked deployment effectiveness",
    "The main issue was not top-of-book fill mechanics. The strategy mostly lost edge "
    "before that point: first through availability, then through directional disagreement.",
)
render_failure_cards(failure_hierarchy)
st.write("")
st.markdown(
    """
    <div class="callout">
      <strong>Plain-English conclusion:</strong>
      the market showed real structure, and the system measured it. The first harvesting
      method was too regime-dependent to recommend for deployment.
    </div>
    """,
    unsafe_allow_html=True,
)
section_close()

section_open(
    "Scope",
    "What this dashboard is not",
    "This page is a showcase layer over static evidence. It does not run capture, replay, "
    "calibration, shadow measurement, artifact mutation, or trading execution.",
)
scope_cols = st.columns(3)
scope_items = [
    ("Read-only", "No writes to artifacts or data directories."),
    ("Static evidence", "Designed for fast project comprehension, not recomputation."),
    ("No trading", "No authenticated execution path or deployment claim."),
]
for col, item in zip(scope_cols, scope_items, strict=True):
    with col:
        st.markdown(insight_card(*item), unsafe_allow_html=True)
section_close()
