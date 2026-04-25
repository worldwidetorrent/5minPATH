"""Read-only Streamlit showcase for the 5minPATH research tool."""

from __future__ import annotations

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
            --ink: #1e1b16;
            --paper: #f7f0df;
            --ochre: #b66a2a;
            --moss: #52633f;
            --line: rgba(30, 27, 22, 0.15);
        }

        .stApp {
            background:
                radial-gradient(circle at 15% 15%, rgba(182, 106, 42, 0.18), transparent 34rem),
                radial-gradient(circle at 85% 10%, rgba(82, 99, 63, 0.16), transparent 30rem),
                linear-gradient(135deg, #fbf7ed 0%, var(--paper) 100%);
            color: var(--ink);
        }

        .block-container {
            max-width: 1180px;
            padding-top: 3rem;
        }

        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color: var(--ink);
            letter-spacing: -0.04em;
        }

        .hero {
            border: 1px solid var(--line);
            border-radius: 28px;
            padding: 2.25rem;
            background: rgba(255, 252, 244, 0.72);
            box-shadow: 0 24px 80px rgba(64, 46, 22, 0.12);
        }

        .eyebrow {
            color: var(--ochre);
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-size: 0.78rem;
            margin-bottom: 0.5rem;
        }

        .status-card {
            border-left: 5px solid var(--moss);
            background: rgba(255, 255, 255, 0.55);
            padding: 1rem 1.15rem;
            border-radius: 18px;
            min-height: 112px;
        }

        .big-number {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 2.4rem;
            font-weight: 700;
            line-height: 1;
        }

        .caption {
            color: rgba(30, 27, 22, 0.68);
            font-size: 0.92rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def section_header(eyebrow: str, title: str, body: str) -> None:
    st.markdown(f'<div class="eyebrow">{eyebrow}</div>', unsafe_allow_html=True)
    st.header(title)
    st.write(body)


inject_style()

days = pd.DataFrame(CLEAN_DAYS)
failure_hierarchy = pd.DataFrame(FAILURE_HIERARCHY)

mean_survival = days["survival_pct"].mean()
median_survival = days["survival_pct"].median()
strong_days = int((days["classification"] == "Strong").sum())

st.markdown(
    """
    <div class="hero">
      <div class="eyebrow">Read-only showcase dashboard</div>
      <h1 style="font-size: 4.7rem; margin: 0;">5minPATH</h1>
      <p style="font-size: 1.25rem; max-width: 820px; margin-top: 0.75rem;">
        A research engine for testing whether modeled edge in 5-minute
        prediction markets survives live market conditions.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.write("")

status_cols = st.columns(4)
status_cards = [
    ("Completed", "Research/data pipeline"),
    ("Validated", "Measurement engine"),
    ("No", "Deployment recommendation"),
    ("Reusable", "Future experiment base"),
]
for col, (number, label) in zip(status_cols, status_cards, strict=True):
    with col:
        st.markdown(
            f"""
            <div class="status-card">
              <div class="big-number">{number}</div>
              <div class="caption">{label}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.divider()

section_header(
    "System",
    "What the tool does",
    "5minPATH is a narrow research and measurement pipeline, not a trading bot.",
)

system_cols = st.columns(5)
system_steps = [
    "Captures raw market, oracle, and quote data",
    "Normalizes data into replayable datasets",
    "Rebuilds historical market state",
    "Applies replay calibration",
    "Measures live-forward shadow survival",
]
for index, (col, step) in enumerate(zip(system_cols, system_steps, strict=True), start=1):
    with col:
        st.metric(label=f"Step {index}", value="", delta=None)
        st.write(step)

st.divider()

section_header(
    "Evidence",
    "Six clean-shadow day comparison",
    "The original strategy produced real but inconsistent live edge survival.",
)

metric_cols = st.columns(3)
metric_cols[0].metric("Mean survival", f"{mean_survival:.1f}%")
metric_cols[1].metric("Median survival", f"{median_survival:.1f}%")
metric_cols[2].metric("Strong days", f"{strong_days} / {len(days)}")

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

chart_data = days.set_index("day")[["survival_pct"]].rename(
    columns={"survival_pct": "Edge survival %"}
)
st.bar_chart(chart_data, height=360)

st.info(
    "Day 7 was the only strong-survival day. Most clean days were weak-to-middle, "
    "so the tested strategy was not consistently deployment-effective."
)

st.divider()

section_header(
    "Failure Anatomy",
    "What blocked deployment effectiveness",
    "The project found that fills were not the primary problem. The main issue was whether "
    "enough reliable live state existed and whether live state agreed directionally with replay.",
)

st.dataframe(
    failure_hierarchy.rename(
        columns={"rank": "Rank", "drag": "Drag", "summary": "Summary"}
    ),
    hide_index=True,
    use_container_width=True,
)

st.divider()

section_header(
    "Conclusion",
    "Valuable tool, no strategy deployment recommendation",
    "The pipeline proved useful for capture, replay, calibration, and shadow measurement. "
    "It also showed that the original strategy captured real signal in replay but did not "
    "convert that signal consistently enough under live-shadow conditions.",
)

st.warning(
    "This dashboard is read-only. It does not run capture, replay, calibration, shadow "
    "measurement, or trading execution."
)
