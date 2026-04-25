"""Read-only Streamlit showcase for the 5minPATH research tool."""

from __future__ import annotations

from html import escape
from textwrap import dedent

import pandas as pd
import streamlit as st
from sample_data import CLEAN_DAYS, FAILURE_HIERARCHY

st.set_page_config(
    page_title="5minPATH Dashboard",
    page_icon="5",
    layout="wide",
)


def html(markup: str) -> None:
    st.html(dedent(markup).strip())


def html_fragment(markup: str) -> str:
    return dedent(markup).strip()


def inject_style() -> None:
    html(
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

        .eyebrow {
            color: var(--amber);
            font-weight: 800;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            font-size: 0.76rem;
            margin-bottom: 0.5rem;
            word-spacing: 0;
            white-space: normal;
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

        .hero-gloss {
            color: var(--muted);
            font-size: 0.98rem;
            line-height: 1.55;
            max-width: 760px;
            margin-top: 0.85rem;
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
            letter-spacing: -0.035em;
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

        .fill-weak {
            background: linear-gradient(90deg, #c76639, #7f351c);
        }

        .fill-middle,
        .fill-lower-middle {
            background: linear-gradient(90deg, #d79a32, #9b6a22);
        }

        .fill-strong {
            background: linear-gradient(90deg, #78804c, #2f4b20);
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

        .distribution-card {
            border: 1px solid var(--line);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            background: rgba(255, 252, 243, 0.66);
            margin-bottom: 1rem;
        }

        .distribution-head {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
            color: var(--muted);
            font-size: 0.9rem;
            margin-bottom: 0.85rem;
        }

        .distribution-track {
            position: relative;
            height: 2.4rem;
            border-radius: 999px;
            background: rgba(24, 21, 16, 0.09);
            border: 1px solid rgba(24, 21, 16, 0.09);
        }

        .distribution-dot {
            position: absolute;
            top: 50%;
            width: 0.95rem;
            height: 0.95rem;
            border-radius: 999px;
            transform: translate(-50%, -50%);
            border: 2px solid rgba(255, 252, 243, 0.96);
            box-shadow: 0 8px 22px rgba(24, 21, 16, 0.18);
        }

        .distribution-dot.weak {
            background: #a94b2d;
        }

        .distribution-dot.middle,
        .distribution-dot.lower-middle {
            background: #d79a32;
        }

        .distribution-dot.strong {
            background: #53633e;
            width: 1.15rem;
            height: 1.15rem;
        }

        .distribution-axis {
            display: flex;
            justify-content: space-between;
            color: var(--muted);
            font-size: 0.78rem;
            margin-top: 0.55rem;
        }

        .table-legend {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.85rem;
            margin: 1rem 0 0.85rem;
        }

        .legend-item {
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 0.85rem 0.95rem;
            background: rgba(255, 252, 243, 0.66);
        }

        .legend-title {
            color: var(--rust);
            font-size: 0.75rem;
            font-weight: 850;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .legend-copy {
            color: var(--muted);
            font-size: 0.88rem;
            line-height: 1.38;
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

            .table-legend {
                grid-template-columns: 1fr;
            }

            .hero {
                padding: 1.55rem;
            }

            .section-shell {
                padding: 1.2rem;
            }

            .eyebrow {
                letter-spacing: 0.055em;
                font-size: 0.68rem;
            }
        }
        </style>
        """
    )


def section_open(eyebrow: str, title: str, body: str) -> None:
    html(
        f"""
        <div class="section-shell">
          <div class="eyebrow">{escape(eyebrow)}</div>
          <div class="section-title">{escape(title)}</div>
          <div class="section-copy">{escape(body)}</div>
        """
    )


def section_close() -> None:
    html("</div>")


def metric_card(note: str, value: str, label: str) -> str:
    return html_fragment(
        f"""
    <div class="metric-card">
      <div class="metric-note">{escape(note)}</div>
      <div class="metric-value">{escape(value)}</div>
      <div class="metric-label">{escape(label)}</div>
    </div>
    """
    )


def insight_card(title: str, body: str) -> str:
    return html_fragment(
        f"""
    <div class="insight-card">
      <div class="insight-title">{escape(title)}</div>
      <div class="insight-copy">{escape(body)}</div>
    </div>
    """
    )


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
            "Measures whether modeled edge survives live forward execution-side conditions.",
        ),
    ]
    cards = []
    for index, (title, body) in enumerate(steps, start=1):
        cards.append(
            html_fragment(
                f"""
            <div class="step-card">
              <div class="step-index">{index}</div>
              <div class="step-title">{escape(title)}</div>
              <div class="step-copy">{escape(body)}</div>
            </div>
            """
            )
        )
    html(f'<div class="pipeline">{"".join(cards)}</div>')


def render_survival_bars(days: pd.DataFrame) -> None:
    max_value = float(days["survival_pct"].max())
    rows = []
    for row in days.to_dict("records"):
        pct = float(row["survival_pct"])
        width = 100.0 if max_value == 0 else max(2.5, pct / max_value * 100.0)
        tag_class = str(row["classification"]).lower().replace(" ", "-")
        rows.append(
            html_fragment(
                f"""
            <div class="bar-row">
              <div class="day-label">{escape(str(row["day"]))}</div>
              <div class="track">
                <div class="fill fill-{escape(tag_class)}" style="width: {width:.2f}%;"></div>
              </div>
              <div class="pct">{pct:.2f}%</div>
              <div><span class="tag tag-{escape(tag_class)}">
                {escape(str(row["classification"]))}
              </span></div>
            </div>
            """
            )
        )
    html(f'<div class="bar-table">{"".join(rows)}</div>')


def render_distribution_strip(days: pd.DataFrame) -> None:
    max_value = float(days["survival_pct"].max())
    dots = []
    for row in days.to_dict("records"):
        pct = float(row["survival_pct"])
        left = 0 if max_value == 0 else pct / max_value * 100.0
        class_name = str(row["classification"]).lower().replace(" ", "-")
        title = f'{row["day"]}: {pct:.2f}% ({row["classification"]})'
        dots.append(
            html_fragment(
                f"""
                <span
                  class="distribution-dot {escape(class_name)}"
                  title="{escape(title)}"
                  style="left: {left:.2f}%;"
                ></span>
                """
            )
        )
    html(
        f"""
        <div class="distribution-card">
          <div class="distribution-head">
            <strong>Survival distribution</strong>
            <span>Six clean days; one strong outlier pulls the mean above the median.</span>
          </div>
          <div class="distribution-track">{"".join(dots)}</div>
          <div class="distribution-axis">
            <span>0%</span>
            <span>{max_value:.1f}%</span>
          </div>
        </div>
        """
    )


def render_failure_cards(failures: pd.DataFrame) -> None:
    cols = st.columns(3)
    for col, row in zip(cols, failures.to_dict("records"), strict=True):
        with col:
            html(
                f"""
                <div class="failure-card">
                  <div class="failure-rank">{int(row["rank"])}</div>
                  <div>
                    <div class="failure-title">{escape(str(row["drag"]))}</div>
                    <div class="failure-copy">{escape(str(row["summary"]))}</div>
                  </div>
                </div>
                """
            )


inject_style()

days = pd.DataFrame(CLEAN_DAYS)
failure_hierarchy = pd.DataFrame(FAILURE_HIERARCHY)

mean_survival = days["survival_pct"].mean()
median_survival = days["survival_pct"].median()
max_survival = days["survival_pct"].max()
strong_days = int((days["classification"] == "Strong").sum())

html(
    """
    <div class="hero">
      <div class="eyebrow">5-MINUTE PREDICTION-MARKET ANALYSIS &amp; TESTING HARNESS</div>
      <h1 class="hero-title">5minPATH</h1>
      <p class="hero-subtitle">
        A research engine for testing whether modeled edge in 5-minute
        prediction markets survives live market conditions.
      </p>
      <div class="verdict">
        Validated measurement engine. Tested strategy not deployment-effective.
      </div>
      <div class="hero-gloss">
        In plain English: the tool worked, the replay signal was real, but this specific
        strategy did not survive live market conditions consistently enough.
      </div>
    </div>
    """
)

st.write("")

metric_cols = st.columns(4)
metric_items = [
    ("Outcome", "Built", "Research/data pipeline"),
    ("Replay signal", "Real", "Calibration repeatedly helped"),
    ("Live survival", f"{median_survival:.1f}%", "Median across clean days"),
    ("Deployment", "Not\u00a0yet", "Not from this tested strategy"),
]
for col, item in zip(metric_cols, metric_items, strict=True):
    with col:
        html(metric_card(*item))

section_open(
    "System",
    "What this tool does",
    "5minPATH is a controlled harness for capture, replay, calibration, and "
    "shadow evaluation. It is not an execution product.",
)
render_pipeline()
section_close()

section_open(
    "Result",
    "The core finding",
    "The project found legitimate replay signal, but the tested strategy did not convert "
    "that signal into consistent live-shadow economics.",
)
insight_cols = st.columns(4)
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
        "Backtests can overstate edge. 5minPATH measures whether modeled edge survives "
        "live forward market conditions.",
    ),
    (
        "Reusable engine",
        "The tested strategy was not deployment-effective, but the capture/replay/shadow "
        "pipeline is reusable for future strategy experiments.",
    ),
]
for col, item in zip(insight_cols, insights, strict=True):
    with col:
        html(insight_card(*item))
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
        html(metric_card(*item))

st.write("")
render_distribution_strip(days)
render_survival_bars(days)
st.write("")
html(
    """
    <div class="table-legend">
      <div class="legend-item">
        <div class="legend-title">Edge survival</div>
        <div class="legend-copy">How much modeled replay edge remained live.</div>
      </div>
      <div class="legend-item">
        <div class="legend-title">3 trusted venues</div>
        <div class="legend-copy">
          Percent of rows reaching the three-trusted-venue threshold required for a composite.
        </div>
      </div>
      <div class="legend-item">
        <div class="legend-title">Side match</div>
        <div class="legend-copy">How often live direction agreed with replay direction.</div>
      </div>
    </div>
    """
)

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
display_days["Edge survival %"] = display_days["Edge survival %"].map("{:.2f}%".format)
display_days["3-trusted-venue rate %"] = display_days[
    "3-trusted-venue rate %"
].map("{:.2f}%".format)
display_days["Side-match rate %"] = display_days["Side-match rate %"].map(
    "{:.1f}%".format
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
    width="stretch",
)
section_close()

section_open(
    "FAILURE ANATOMY",
    "What blocked deployment effectiveness",
    "The main issue was not top-of-book fill mechanics. The strategy mostly lost edge "
    "before that point: first through availability, then through directional disagreement.",
)
render_failure_cards(failure_hierarchy)
st.write("")
html(
    """
    <div class="callout">
      <strong>Plain-English conclusion:</strong>
      the market showed real structure, and the system measured it. The tested strategy
      was too regime-dependent to recommend for deployment.
    </div>
    """
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
        html(insight_card(*item))
section_close()
