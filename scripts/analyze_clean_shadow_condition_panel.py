#!/usr/bin/env python3
"""Build a compact cross-day condition panel for clean-shadow days."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from analyze_side_mismatch_audit import (
    DaySpec as AuditDaySpec,
    build_rows,
    json_default,
    safe_ratio,
)


WEAK_THRESHOLD = Decimal("0.03")
STRONG_THRESHOLD = Decimal("0.15")


@dataclass(frozen=True)
class DaySpec:
    label: str
    session_id: str
    edge_summary_path: Path
    replay_rows_path: Path
    shadow_decisions_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spec",
        action="append",
        required=True,
        help=(
            "Day label, session id, edge-summary path, replay rows path, and shadow-decisions path "
            "separated by |"
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def parse_spec(value: str) -> DaySpec:
    parts = value.split("|")
    if len(parts) != 5:
        raise ValueError("--spec must be: label|session_id|edge_summary|replay_rows|shadow_decisions")
    label, session_id, edge_summary, replay_rows, shadow_decisions = parts
    return DaySpec(
        label=label,
        session_id=session_id,
        edge_summary_path=Path(edge_summary),
        replay_rows_path=Path(replay_rows),
        shadow_decisions_path=Path(shadow_decisions),
    )


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def classify_survival(value: Decimal) -> str:
    if value >= STRONG_THRESHOLD:
        return "strong"
    if value >= WEAK_THRESHOLD:
        return "middle"
    return "weak"


def hour_of_day(row: dict[str, Any]) -> str:
    return str(row["hour_bucket"])[11:13]


def fair_value_delta_bucket(row: dict[str, Any]) -> str:
    value = row.get("calibrated_fair_value_abs_delta_live_vs_replay")
    if value is None:
        return "missing"
    if value < Decimal("0.02"):
        return "lt_2c"
    if value < Decimal("0.05"):
        return "2c_to_5c"
    if value < Decimal("0.10"):
        return "5c_to_10c"
    return "gte_10c"


def skew_presence(row: dict[str, Any]) -> str:
    return "present" if row.get("skew_bucket") != "absent" else "absent"


def summarize_shadow_decisions(path: Path) -> dict[str, Any]:
    decision_count = 0
    three_trusted = 0
    actionable = 0
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            decision_count += 1
            state = row["executable_state"]
            tradability = row["tradability_check"]
            if int(state.get("exchange_trusted_venue_count", 0)) == 3:
                three_trusted += 1
            if bool(tradability.get("is_actionable")):
                actionable += 1
    return {
        "decision_count": decision_count,
        "three_trusted_venue_row_count": three_trusted,
        "three_trusted_venue_rate": safe_ratio(three_trusted, decision_count),
        "actionable_decision_count": actionable,
        "actionable_rate": safe_ratio(actionable, decision_count),
    }


def sort_dimension_keys(values: dict[str, Any], dimension: str) -> list[str]:
    if dimension == "hour_of_day":
        return sorted(values, key=lambda key: int(key))
    preferred = {
        "calibration_bucket": ["far_down", "lean_down", "near_mid", "lean_up", "far_up", "missing"],
        "skew_presence": ["absent", "present"],
        "fair_value_delta_bucket": ["lt_2c", "2c_to_5c", "5c_to_10c", "gte_10c", "missing"],
    }.get(dimension)
    if preferred is None:
        return sorted(values)
    return [key for key in preferred if key in values] + sorted(
        key for key in values if key not in preferred
    )


def summarize_dimension(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    key_fn: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    buckets: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "total_calibrated_trade_rows": 0,
            "joined_rows": 0,
            "actionable_rows": 0,
            "side_match_rows": 0,
            "side_mismatch_rows": 0,
        }
    )
    for row in rows:
        key = key_fn(row)
        counts = buckets[key]
        counts["total_calibrated_trade_rows"] += 1
        if row.get("shadow_joined"):
            counts["joined_rows"] += 1
        if row.get("shadow_actionable"):
            counts["actionable_rows"] += 1
            if row.get("side_match") is True:
                counts["side_match_rows"] += 1
            elif row.get("side_match") is False:
                counts["side_mismatch_rows"] += 1

    summary: dict[str, Any] = {}
    for key in sort_dimension_keys(buckets, dimension):
        counts = buckets[key]
        summary[key] = {
            **counts,
            "joined_rate": safe_ratio(counts["joined_rows"], counts["total_calibrated_trade_rows"]),
            "actionable_rate_on_calibrated_rows": safe_ratio(
                counts["actionable_rows"], counts["total_calibrated_trade_rows"]
            ),
            "side_match_rate_on_actionable_rows": safe_ratio(
                counts["side_match_rows"], counts["actionable_rows"]
            ),
        }
    return summary


def best_and_worst_hours(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    hourly = summarize_dimension(rows, dimension="hour_of_day", key_fn=hour_of_day)
    eligible = [
        {
            "hour_of_day": hour,
            "actionable_rows": values["actionable_rows"],
            "side_match_rate_on_actionable_rows": values["side_match_rate_on_actionable_rows"],
            "actionable_rate_on_calibrated_rows": values["actionable_rate_on_calibrated_rows"],
        }
        for hour, values in hourly.items()
        if values["actionable_rows"] >= 25 and values["side_match_rate_on_actionable_rows"] is not None
    ]
    best = sorted(
        eligible,
        key=lambda row: (
            Decimal(str(row["side_match_rate_on_actionable_rows"])),
            row["actionable_rows"],
            row["hour_of_day"],
        ),
        reverse=True,
    )[:5]
    worst = sorted(
        eligible,
        key=lambda row: (
            Decimal(str(row["side_match_rate_on_actionable_rows"])),
            -row["actionable_rows"],
            row["hour_of_day"],
        )
    )[:5]
    return {"best_hours": best, "worst_hours": worst}


def cohort_day_metric(days: list[dict[str, Any]], field: str) -> Decimal:
    values = [to_decimal(day[field]) for day in days]
    values = [value for value in values if value is not None]
    return sum(values) / Decimal(len(values))


def summarize_cohort(name: str, days: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "cohort": name,
        "day_labels": [day["day_label"] for day in days],
        "day_count": len(days),
        "mean_edge_survival_ratio": cohort_day_metric(days, "edge_survival_ratio"),
        "mean_three_trusted_venue_rate": cohort_day_metric(days, "three_trusted_venue_rate"),
        "mean_actionable_rate_on_calibrated_rows": cohort_day_metric(
            days, "shadow_actionable_rate_on_calibrated_trade_rows"
        ),
        "mean_side_match_rate_on_actionable_rows": cohort_day_metric(
            days, "side_match_rate_on_shadow_actionable_rows"
        ),
        "mean_availability_loss_per_contract": cohort_day_metric(days, "availability_loss_per_contract"),
        "mean_side_mismatch_loss_per_contract": cohort_day_metric(
            days, "side_mismatch_loss_per_contract"
        ),
        "mean_fill_loss_per_contract": cohort_day_metric(days, "fill_loss_per_contract"),
        "dimensions": {
            "calibration_bucket": summarize_dimension(
                rows,
                dimension="calibration_bucket",
                key_fn=lambda row: str(row.get("calibration_bucket") or "missing"),
            ),
            "hour_of_day": summarize_dimension(rows, dimension="hour_of_day", key_fn=hour_of_day),
            "volatility_regime": summarize_dimension(
                rows,
                dimension="volatility_regime",
                key_fn=lambda row: str(row.get("volatility_regime") or "missing"),
            ),
            "skew_presence": summarize_dimension(
                rows,
                dimension="skew_presence",
                key_fn=skew_presence,
            ),
            "fair_value_delta_bucket": summarize_dimension(
                rows,
                dimension="fair_value_delta_bucket",
                key_fn=fair_value_delta_bucket,
            ),
        },
        "hour_extremes": best_and_worst_hours(rows),
    }


def render_rate_map(
    dimension_summary: dict[str, Any],
    *,
    rate_field: str,
    min_actionable_rows: int = 0,
) -> dict[str, Any]:
    rendered: dict[str, Any] = {}
    for key, values in dimension_summary.items():
        if values["actionable_rows"] < min_actionable_rows:
            continue
        rendered[key] = values[rate_field]
    return rendered


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Clean Shadow Condition Panel",
        "",
        "Scope:",
        "- six clean shadow baseline days only",
        "- calibrated replay trade rows joined to shadow decisions",
        "- live-vs-replay composite delta is approximated with calibrated fair-value delta because replay composite USD price is not persisted",
        "",
        "## Day Table",
        "",
        "| Day | Class | Survival | 3-trusted rate | Actionable on calibrated rows | Side-match on actionable | Availability loss | Side mismatch loss | Fill loss |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for day in summary["days"]:
        lines.append(
            "| "
            f"{day['day_label']} | "
            f"{day['classification']} | "
            f"`{day['edge_survival_ratio']}` | "
            f"`{day['three_trusted_venue_rate']}` | "
            f"`{day['shadow_actionable_rate_on_calibrated_trade_rows']}` | "
            f"`{day['side_match_rate_on_shadow_actionable_rows']}` | "
            f"`{day['availability_loss_per_contract']}` | "
            f"`{day['side_mismatch_loss_per_contract']}` | "
            f"`{day['fill_loss_per_contract']}` |"
        )
    lines.extend(
        [
            "",
            "## Cohort Summary",
            "",
        ]
    )
    for cohort in summary["cohorts"]:
        lines.extend(
            [
                f"### {cohort['cohort'].title()}",
                f"- days: `{cohort['day_labels']}`",
                f"- mean survival: `{cohort['mean_edge_survival_ratio']}`",
                f"- mean 3-trusted rate: `{cohort['mean_three_trusted_venue_rate']}`",
                f"- mean actionable on calibrated rows: `{cohort['mean_actionable_rate_on_calibrated_rows']}`",
                f"- mean side-match on actionable rows: `{cohort['mean_side_match_rate_on_actionable_rows']}`",
                f"- mean availability loss: `{cohort['mean_availability_loss_per_contract']}`",
                f"- mean side mismatch loss: `{cohort['mean_side_mismatch_loss_per_contract']}`",
                f"- mean fill loss: `{cohort['mean_fill_loss_per_contract']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Requested Panel",
            "",
        ]
    )
    for cohort in summary["cohorts"]:
        dims = cohort["dimensions"]
        extremes = cohort["hour_extremes"]
        lines.extend(
            [
                f"### {cohort['cohort'].title()} Condition Cuts",
                (
                    "- calibration bucket side-match rates: "
                    f"`{render_rate_map(dims['calibration_bucket'], rate_field='side_match_rate_on_actionable_rows', min_actionable_rows=25)}`"
                ),
                (
                    "- calibration bucket actionable rates: "
                    f"`{render_rate_map(dims['calibration_bucket'], rate_field='actionable_rate_on_calibrated_rows')}`"
                ),
                (
                    "- volatility regime side-match rates: "
                    f"`{render_rate_map(dims['volatility_regime'], rate_field='side_match_rate_on_actionable_rows', min_actionable_rows=25)}`"
                ),
                (
                    "- event-time skew overlap: "
                    f"`{dims['skew_presence']}`"
                ),
                (
                    "- fair-value delta bucket side-match rates: "
                    f"`{render_rate_map(dims['fair_value_delta_bucket'], rate_field='side_match_rate_on_actionable_rows', min_actionable_rows=25)}`"
                ),
                f"- best hours by side-match: `{extremes['best_hours']}`",
                f"- worst hours by side-match: `{extremes['worst_hours']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            "",
            "- Day 7 is still the only day where high availability and high directional agreement happened together at scale.",
            "- Day 9, Day 10, and Day 11 convert availability into survival only partially: they sit in the middle because one of the two gates stays weak on each day.",
            "- Day 4 and Day 8 are weak for different reasons. Day 4 is an availability-collapse day. Day 8 is a directional-agreement failure day despite abundant 3-trusted state.",
            "- The strongest available explanatory proxy for live-vs-replay disagreement is calibrated fair-value delta. As that delta bucket widens, side-match generally weakens outside the Day 7 strong regime.",
            "- Event-time skew still overlaps the weak and middle cohorts, but fill loss remains negligible, so the practical bottleneck is still trusted-state formation and then side agreement.",
            "",
            "## Decision Read",
            "",
            "- If a future offline rule exists, it should be conditional on the specific availability + directional-agreement signature, not a blanket threshold.",
            "- If that signature does not stabilize with more clean days, the honest conclusion remains that the system is a valid measurement engine with regime-dependent economics.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    specs = [parse_spec(value) for value in args.spec]
    day_summaries: list[dict[str, Any]] = []
    rows_by_cohort: dict[str, list[dict[str, Any]]] = defaultdict(list)
    days_by_cohort: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for spec in specs:
        edge = load_json(spec.edge_summary_path)
        survival = to_decimal(edge["edge_survival_ratio"])
        classification = classify_survival(survival)
        shadow = summarize_shadow_decisions(spec.shadow_decisions_path)
        rows = build_rows(
            AuditDaySpec(
                label=spec.label,
                session_id=spec.session_id,
                replay_rows=spec.replay_rows_path,
                shadow_decisions=spec.shadow_decisions_path,
            )
        )
        day_summary = {
            "day_label": spec.label,
            "session_id": spec.session_id,
            "classification": classification,
            "edge_survival_ratio": survival,
            "three_trusted_venue_rate": to_decimal(shadow["three_trusted_venue_rate"]),
            "shadow_actionable_rate_on_calibrated_trade_rows": to_decimal(
                edge["shadow_actionable_rate_on_calibrated_trade_rows"]
            ),
            "side_match_rate_on_shadow_actionable_rows": to_decimal(
                edge["side_match_rate_on_shadow_actionable_rows"]
            ),
            "availability_loss_per_contract": to_decimal(edge["availability_loss_per_contract"]),
            "side_mismatch_loss_per_contract": to_decimal(edge["side_mismatch_loss_per_contract"]),
            "fill_loss_per_contract": to_decimal(edge["fill_loss_per_contract"]),
        }
        day_summaries.append(day_summary)
        rows_by_cohort[classification].extend(rows)
        days_by_cohort[classification].append(day_summary)

    cohort_order = ["strong", "middle", "weak"]
    summary = {
        "comparison_label": "clean_shadow_condition_panel",
        "composite_delta_proxy": (
            "replay composite USD price is not persisted; calibrated fair-value delta is used as the available proxy"
        ),
        "days": day_summaries,
        "cohorts": [
            summarize_cohort(name, days_by_cohort[name], rows_by_cohort[name])
            for name in cohort_order
            if days_by_cohort[name]
        ],
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(args.output_dir / "summary.json", summary)
    (args.output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
