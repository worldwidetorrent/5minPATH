"""Read-only Streamlit showcase for the 5minPATH research tool."""

from __future__ import annotations

from html import escape
from statistics import mean, median

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
            --ink: #17130e;
            --muted: rgba(23, 19, 14, 0.66);
            --paper: #f4ead3;
            --paper-deep: #e8d5b2;
            --card: rgba(255, 251, 240, 0.82);
            --card-solid: #fff8e8;
            --line: rgba(23, 19, 14, 0.14);
            --amber: #b85f23;
            --rust: #7a301a;
            --moss: #4f613b;
            --olive: #78804c;
            --cream: #fff8e8;
        }

        .stApp {
            background:
                radial-gradient(circle at 11% 9%, rgba(216, 151, 47, 0.31), transparent 28rem),
                radial-gradient(circle at 88% 12%, rgba(79, 97, 59, 0.25), transparent 32rem),
                linear-gradient(135deg, #fcf8ed 0%, var(--paper) 52%, var(--paper-deep) 100%);
            color: var(--ink);
        }

        .block-container {
            max-width: 1240px;
            padding-top: 2.25rem;
            padding-bottom: 4rem;
        }

        header[data-testid="stHeader"] {
            background: transparent;
        }

        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color: var(--ink);
            letter-spacing: -0.045em;
        }

        .hero {
            position: relative;
            overflow: hidden;
            border: 1px solid var(--line);
            border-radius: 34px;
            padding: clamp(1.6rem, 4vw, 3.2rem);
            background:
                linear-gradient(120deg, rgba(255, 251, 240, 0.94), rgba(255, 246, 224, 0.72)),
                radial-gradient(circle at 100% 0%, rgba(122, 48, 26, 0.16), transparent 27rem);
            box-shadow: 0 30px 90px rgba(77, 52, 20, 0.15);
        }

        .hero::after {
            content: "";
            position: absolute;
            right: -6rem;
            top: -6rem;
            width: 21rem;
            height: 21rem;
            border-radius: 50%;
            border: 1px solid rgba(23, 19, 14, 0.15);
            box-shadow:
                -3.5rem 5rem 0 rgba(79, 97, 59, 0.10),
                -8rem 8.2rem 0 rgba(184, 95, 35, 0.08);
        }

        .eyebrow {
            color: var(--amber);
            font-size: 0.75rem;
            font-weight: 850;
            letter-spacing: 0.15em;
            text-transform: uppercase;
        }

        .hero-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: clamp(4.2rem, 10vw, 7.7rem);
            line-height: 0.86;
            letter-spacing: -0.075em;
            margin: 0.35rem 0 0;
            max-width: 760px;
        }

        .hero-copy {
            color: var(--muted);
            font-size: clamp(1rem, 2vw, 1.24rem);
            line-height: 1.65;
            margin: 1.25rem 0 0;
            max-width: 760px;
        }

        .verdict-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.7rem;
            margin-top: 1.45rem;
        }

        .pill {
            display: inline-flex;
            align-items: center;
            min-height: 2.25rem;
            padding: 0.52rem 0.86rem;
            border-radius: 999px;
            border: 1px solid rgba(23, 19, 14, 0.13);
            background: rgba(255, 255, 255, 0.44);
            color: var(--ink);
            font-size: 0.82rem;
            font-weight: 850;
        }

        .pill-warning {
            border-color: rgba(122, 48, 26, 0.22);
            background: rgba(122, 48, 26, 0.09);
            color: var(--rust);
        }

        .section {
            margin-top: 2.2rem;
            border: 1px solid var(--line);
            border-radius: 30px;
            padding: clamp(1.2rem, 2.8vw, 2rem);
            background: rgba(255, 251, 240, 0.62);
            box-shadow: 0 20px 62px rgba(77, 52, 20, 0.09);
        }

        .section-head {
            max-width: 850px;
            margin-bottom: 1.25rem;
        }

        .section-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: clamp(2rem, 4vw, 2.7rem);
            line-height: 1.03;
            letter-spacing: -0.055em;
            margin: 0.28rem 0 0.55rem;
        }

        .section-copy {
            color: var(--muted);
            font-size: 1.01rem;
            line-height: 1.62;
        }

        .grid-4,
        .grid-3,
        .grid-5 {
            display: grid;
            gap: 1rem;
        }

        .grid-4 {
            grid-template-columns: repeat(4, minmax(0, 1fr));
        }

        .grid-3 {
            grid-template-columns: repeat(3, minmax(0, 1fr));
        }

        .grid-5 {
            grid-template-columns: repeat(5, minmax(0, 1fr));
        }

        .card {
            border: 1px solid var(--line);
            border-radius: 23px;
            background: var(--card);
            box-shadow: 0 18px 55px rgba(77, 52, 20, 0.08);
        }

        .metric-card {
            min-height: 142px;
            padding: 1.15rem;
        }

        .metric-note {
            color: var(--amber);
            font-size: 0.74rem;
            font-weight: 850;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-bottom: 0.55rem;
        }

        .metric-value {
            font-family: Georgia, "Times New Roman", serif;
            font-size: clamp(2rem, 4vw, 2.75rem);
            font-weight: 750;
            letter-spacing: -0.07em;
            line-height: 0.96;
        }

        .metric-label {
            color: var(--muted);
            font-size: 0.92rem;
            line-height: 1.42;
            margin-top: 0.7rem;
        }

        .insight-card,
        .step-card,
        .scope-card {
            min-height: 170px;
            padding: 1.2rem;
        }

        .card-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.34rem;
            line-height: 1.1;
            letter-spacing: -0.035em;
            margin-bottom: 0.72rem;
        }

        .card-copy {
            color: var(--muted);
            font-size: 0.94rem;
            line-height: 1.5;
        }

        .step-index {
            width: 2.35rem;
            height: 2.35rem;
            display: grid;
            place-items: center;
            border-radius: 999px;
            background: var(--ink);
            color: var(--cream);
            font-weight: 900;
            margin-bottom: 0.95rem;
        }

        .survival-list {
            display: grid;
            gap: 0.78rem;
            margin-top: 1rem;
        }

        .survival-row {
            display: grid;
            grid-template-columns: 6rem 1fr 6.6rem 9.6rem;
            align-items: center;
            gap: 1rem;
            padding: 0.95rem 1rem;
            border: 1px solid var(--line);
            border-radius: 19px;
            background: rgba(255, 251, 240, 0.76);
        }

        .day-label {
            font-weight: 950;
        }

        .track {
            height: 1rem;
            border-radius: 999px;
            background: rgba(23, 19, 14, 0.10);
            overflow: hidden;
        }

        .fill {
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, var(--amber), var(--rust));
            box-shadow: 0 0 18px rgba(184, 95, 35, 0.22);
        }

        .pct {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.45rem;
            font-weight: 800;
            letter-spacing: -0.045em;
        }

        .tag {
            display: inline-flex;
            justify-content: center;
            padding: 0.34rem 0.68rem;
            border-radius: 999px;
            border: 1px solid rgba(23, 19, 14, 0.13);
            font-size: 0.77rem;
            font-weight: 900;
            white-space: nowrap;
        }

        .tag-strong {
            color: #2d4c20;
            background: rgba(79, 97, 59, 0.15);
        }

        .tag-middle,
        .tag-lower-middle {
            color: #76501a;
            background: rgba(216, 151, 47, 0.17);
        }

        .tag-weak {
            color: var(--rust);
            background: rgba(122, 48, 26, 0.12);
        }

        .evidence-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            overflow: hidden;
            border: 1px solid var(--line);
            border-radius: 20px;
            background: var(--card-solid);
            box-shadow: 0 18px 55px rgba(77, 52, 20, 0.08);
            margin-top: 1rem;
        }

        .evidence-table th,
        .evidence-table td {
            padding: 0.86rem 0.95rem;
            border-bottom: 1px solid rgba(23, 19, 14, 0.10);
            text-align: left;
            vertical-align: top;
        }

        .evidence-table th {
            color: var(--rust);
            background: rgba(122, 48, 26, 0.07);
            font-size: 0.76rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .evidence-table tr:last-child td {
            border-bottom: none;
        }

        .failure-card {
            display: grid;
            grid-template-columns: 2.9rem 1fr;
            gap: 0.95rem;
            min-height: 150px;
            padding: 1.15rem;
        }

        .failure-rank {
            width: 2.6rem;
            height: 2.6rem;
            display: grid;
            place-items: center;
            border-radius: 17px;
            background: var(--moss);
            color: var(--cream);
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.45rem;
            font-weight: 850;
        }

        .callout {
            margin-top: 1rem;
            border: 1px solid rgba(122, 48, 26, 0.22);
            border-left: 7px solid var(--rust);
            border-radius: 24px;
            padding: 1.25rem 1.35rem;
            background: rgba(122, 48, 26, 0.08);
            color: var(--muted);
            line-height: 1.55;
        }

        .callout strong {
            color: var(--rust);
        }

        @media (max-width: 1000px) {
            .grid-4,
            .grid-5 {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            .grid-3 {
                grid-template-columns: 1fr;
            }
        }

        @media (max-width: 700px) {
            .grid-4,
            .grid-5 {
                grid-template-columns: 1fr;
            }

            .survival-row {
                grid-template-columns: 1fr;
                gap: 0.55rem;
            }

            .hero {
                border-radius: 26px;
            }

            .section {
                border-radius: 24px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def metric_card(note: str, value: str, label: str) -> str:
    return f"""
    <div class="card metric-card">
      <div class="metric-note">{escape(note)}</div>
      <div class="metric-value">{escape(value)}</div>
      <div class="metric-label">{escape(label)}</div>
    </div>
    """


def text_card(title: str, body: str, class_name: str = "insight-card") -> str:
    return f"""
    <div class="card {escape(class_name)}">
      <div class="card-title">{escape(title)}</div>
      <div class="card-copy">{escape(body)}</div>
    </div>
    """


def section(title: str, eyebrow: str, body: str, inner_html: str) -> str:
    return f"""
    <section class="section">
      <div class="section-head">
        <div class="eyebrow">{escape(eyebrow)}</div>
        <div class="section-title">{escape(title)}</div>
        <div class="section-copy">{escape(body)}</div>
      </div>
      {inner_html}
    </section>
    """


def grid(items: list[str], class_name: str) -> str:
    return f'<div class="{escape(class_name)}">{"".join(items)}</div>'


def render_pipeline() -> str:
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
            "Measures whether modeled edge survives live-forward execution conditions.",
        ),
    ]
    cards = []
    for index, (title, body) in enumerate(steps, start=1):
        cards.append(
            f"""
            <div class="card step-card">
              <div class="step-index">{index}</div>
              <div class="card-title">{escape(title)}</div>
              <div class="card-copy">{escape(body)}</div>
            </div>
            """
        )
    return grid(cards, "grid-5")


def render_survival_bars(days: list[dict[str, object]]) -> str:
    max_value = max(float(row["survival_pct"]) for row in days)
    rows = []
    for row in days:
        pct = float(row["survival_pct"])
        width = 100.0 if max_value == 0 else max(2.5, pct / max_value * 100.0)
        tag_class = str(row["classification"]).lower().replace(" ", "-")
        rows.append(
            f"""
            <div class="survival-row">
              <div class="day-label">{escape(str(row["day"]))}</div>
              <div class="track"><div class="fill" style="width: {width:.2f}%;"></div></div>
              <div class="pct">{pct:.2f}%</div>
              <div><span class="tag tag-{escape(tag_class)}">
                {escape(str(row["classification"]))}
              </span></div>
            </div>
            """
        )
    return f'<div class="survival-list">{"".join(rows)}</div>'


def render_evidence_table(days: list[dict[str, object]]) -> str:
    rows = []
    for row in days:
        rows.append(
            f"""
            <tr>
              <td><strong>{escape(str(row["day"]))}</strong></td>
              <td>{float(row["survival_pct"]):.2f}%</td>
              <td>{escape(str(row["classification"]))}</td>
              <td>{float(row["trusted_venue_rate_pct"]):.2f}%</td>
              <td>{float(row["side_match_rate_pct"]):.1f}%</td>
              <td>{escape(str(row["main_drag"]))}</td>
            </tr>
            """
        )
    return f"""
    <table class="evidence-table">
      <thead>
        <tr>
          <th>Day</th>
          <th>Survival</th>
          <th>Class</th>
          <th>3 trusted</th>
          <th>Side match</th>
          <th>Main drag</th>
        </tr>
      </thead>
      <tbody>{"".join(rows)}</tbody>
    </table>
    """


def render_failure_cards(failures: list[dict[str, object]]) -> str:
    cards = []
    for row in failures:
        cards.append(
            f"""
            <div class="card failure-card">
              <div class="failure-rank">{int(row["rank"])}</div>
              <div>
                <div class="card-title">{escape(str(row["drag"]))}</div>
                <div class="card-copy">{escape(str(row["summary"]))}</div>
              </div>
            </div>
            """
        )
    return grid(cards, "grid-3")


survival_values = [float(row["survival_pct"]) for row in CLEAN_DAYS]
mean_survival = mean(survival_values)
median_survival = median(survival_values)
max_survival = max(survival_values)
strong_days = sum(row["classification"] == "Strong" for row in CLEAN_DAYS)

inject_style()

hero = """
<div class="hero">
  <div class="eyebrow">5-minute prediction-market research</div>
  <h1 class="hero-title">5minPATH</h1>
  <p class="hero-copy">
    A reusable capture, replay, calibration, and live-forward shadow measurement
    engine for testing whether modeled edge survives real market conditions.
  </p>
  <div class="verdict-row">
    <span class="pill">Validated measurement engine</span>
    <span class="pill pill-warning">No deployment recommendation</span>
    <span class="pill">Read-only showcase</span>
  </div>
</div>
"""

top_metrics = grid(
    [
        metric_card("Outcome", "Built", "Research/data pipeline"),
        metric_card("Replay signal", "Real", "Calibration repeatedly helped"),
        metric_card("Median survival", f"{median_survival:.1f}%", "Across clean-shadow days"),
        metric_card("Strong days", f"{strong_days}/6", "Day 7 was the outlier"),
    ],
    "grid-4",
)

system_section = section(
    "What this tool does",
    "System",
    "The repo is a research pipeline, not an execution product. It helps a user gather "
    "market data, replay it, test calibration, and measure live-forward survival.",
    render_pipeline(),
)

finding_section = section(
    "The core finding",
    "Result",
    "The project found legitimate replay signal, but the tested strategy did not convert "
    "that signal into consistent live-shadow economics.",
    grid(
        [
            text_card(
                "What worked",
                "Runtime cleanliness repeated, capture became stable, and calibration improved "
                "replay economics across multiple sessions.",
            ),
            text_card(
                "What did not",
                "Modeled edge survived live conditions inconsistently. Day 7 was strong, but "
                "most clean days were weak-to-middle.",
            ),
            text_card(
                "Why it matters",
                "The tool is valuable because it tells the truth about conversion from replay "
                "edge to live-forward tradability.",
            ),
        ],
        "grid-3",
    ),
)

evidence_metrics = grid(
    [
        metric_card("Mean survival", f"{mean_survival:.1f}%", "Pulled upward by Day 7"),
        metric_card("Median survival", f"{median_survival:.1f}%", "Better picture of normal days"),
        metric_card("Best day", f"{max_survival:.1f}%", "Day 7 strong-survival outlier"),
    ],
    "grid-3",
)

evidence_section = section(
    "Six clean-shadow day comparison",
    "Evidence",
    "The visual center of the project is the survival distribution. Day 7 was real, "
    "but it did not become the norm.",
    evidence_metrics + render_survival_bars(CLEAN_DAYS) + render_evidence_table(CLEAN_DAYS),
)

failure_section = section(
    "What blocked deployment effectiveness",
    "Failure anatomy",
    "The main issue was not top-of-book fill mechanics. The strategy mostly lost edge "
    "before that point: first through availability, then through directional disagreement.",
    render_failure_cards(FAILURE_HIERARCHY)
    + """
      <div class="callout">
        <strong>Plain-English conclusion:</strong>
        the market showed real structure, and the system measured it. The first harvesting
        method was too regime-dependent to recommend for deployment.
      </div>
      """,
)

scope_section = section(
    "What this dashboard is not",
    "Scope",
    "This page is a showcase layer over static evidence. It does not run capture, replay, "
    "calibration, shadow measurement, artifact mutation, or trading execution.",
    grid(
        [
            text_card("Read-only", "No writes to artifacts or data directories.", "scope-card"),
            text_card(
                "Static evidence",
                "Designed for fast project comprehension, not recomputation.",
                "scope-card",
            ),
            text_card(
                "No trading",
                "No authenticated execution path or deployment claim.",
                "scope-card",
            ),
        ],
        "grid-3",
    ),
)

st.markdown(
    hero + top_metrics + system_section + finding_section + evidence_section + failure_section
    + scope_section,
    unsafe_allow_html=True,
)
