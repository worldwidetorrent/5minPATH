#!/usr/bin/env python3
"""Build a compact clean-shadow edge comparison across multiple days."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any


WEAK_THRESHOLD = Decimal("0.03")
STRONG_THRESHOLD = Decimal("0.15")


@dataclass(frozen=True)
class DaySpec:
    label: str
    session_id: str
    edge_summary_path: Path
    shadow_summary_path: Path
    shadow_decisions_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spec",
        action="append",
        required=True,
        help=(
            "Day label, session id, edge-summary path, shadow-summary path, and "
            "shadow-decisions path separated by |"
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def parse_spec(value: str) -> DaySpec:
    parts = value.split("|")
    if len(parts) != 5:
        raise ValueError(
            "--spec must be: label|session_id|edge_summary|shadow_summary|shadow_decisions"
        )
    label, session_id, edge_summary, shadow_summary, shadow_decisions = parts
    return DaySpec(
        label=label,
        session_id=session_id,
        edge_summary_path=Path(edge_summary),
        shadow_summary_path=Path(shadow_summary),
        shadow_decisions_path=Path(shadow_decisions),
    )


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported JSON type: {type(value)!r}")


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def safe_ratio(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    return Decimal(numerator) / Decimal(denominator)


def classify_survival(value: Decimal) -> str:
    if value >= STRONG_THRESHOLD:
        return "strong"
    if value >= WEAK_THRESHOLD:
        return "middle"
    return "weak"


def summarize_shadow(path: Path) -> dict[str, Any]:
    decision_count = 0
    actionable_count = 0
    three_trusted_count = 0
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            decision_count += 1
            state = row["executable_state"]
            tradability = row["tradability_check"]
            if int(state.get("exchange_trusted_venue_count", 0)) == 3:
                three_trusted_count += 1
            if bool(tradability.get("is_actionable")):
                actionable_count += 1
    return {
        "decision_count": decision_count,
        "actionable_decision_count": actionable_count,
        "three_trusted_venue_row_count": three_trusted_count,
        "three_trusted_venue_rate": safe_ratio(three_trusted_count, decision_count),
    }


def percentile_inclusive(values: list[Decimal], fraction: Decimal) -> Decimal:
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    position = (Decimal(len(ordered) - 1) * fraction)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - Decimal(lower)
    if upper == lower:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def build_day_summary(spec: DaySpec) -> dict[str, Any]:
    edge = load_json(spec.edge_summary_path)
    shadow = load_json(spec.shadow_summary_path)
    shadow_decisions = summarize_shadow(spec.shadow_decisions_path)
    survival = to_decimal(edge["edge_survival_ratio"])
    return {
        "day_label": spec.label,
        "session_id": spec.session_id,
        "edge_summary_path": str(spec.edge_summary_path),
        "shadow_summary_path": str(spec.shadow_summary_path),
        "shadow_decisions_path": str(spec.shadow_decisions_path),
        "classification": classify_survival(survival),
        "edge_survival_ratio": survival,
        "replay_expected_pnl_total_per_contract": to_decimal(
            edge["replay_expected_pnl_total_per_contract"]
        ),
        "shadow_realized_pnl_total_per_contract": to_decimal(
            edge["shadow_realized_pnl_total_per_contract"]
        ),
        "joined_trade_rate": to_decimal(edge["joined_trade_rate"]),
        "shadow_actionable_rate_on_calibrated_trade_rows": to_decimal(
            edge["shadow_actionable_rate_on_calibrated_trade_rows"]
        ),
        "side_match_rate_on_shadow_actionable_rows": to_decimal(
            edge["side_match_rate_on_shadow_actionable_rows"]
        ),
        "availability_loss_per_contract": to_decimal(edge["availability_loss_per_contract"]),
        "side_mismatch_loss_per_contract": to_decimal(edge["side_mismatch_loss_per_contract"]),
        "fill_loss_per_contract": to_decimal(edge["fill_loss_per_contract"]),
        "decision_count": shadow_decisions["decision_count"],
        "actionable_decision_count": shadow_decisions["actionable_decision_count"],
        "three_trusted_venue_row_count": shadow_decisions["three_trusted_venue_row_count"],
        "three_trusted_venue_rate": shadow_decisions["three_trusted_venue_rate"],
        "shadow_processing_mode": shadow.get("processing_mode"),
        "shadow_backlog_decision_count": shadow.get("backlog_decision_count"),
    }


def build_distribution(days: list[dict[str, Any]]) -> dict[str, Any]:
    survivals = [day["edge_survival_ratio"] for day in days]
    buckets = {"weak": [], "middle": [], "strong": []}
    for day in days:
        buckets[day["classification"]].append(day["day_label"])
    return {
        "day_count": len(days),
        "mean_survival": sum(survivals) / Decimal(len(survivals)),
        "median_survival": Decimal(str(median(survivals))),
        "min_survival": min(survivals),
        "max_survival": max(survivals),
        "p25_survival": percentile_inclusive(survivals, Decimal("0.25")),
        "p75_survival": percentile_inclusive(survivals, Decimal("0.75")),
        "weak_days": buckets["weak"],
        "middle_days": buckets["middle"],
        "strong_days": buckets["strong"],
    }


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Clean Shadow Baseline Edge Comparison Through Day 11",
        "",
        "Scope:",
        "- clean shadow runtime sessions only",
        "- calibrated replay rows joined to live-forward shadow decisions",
        "- one-contract normalized edge-survival method",
        "",
        "Artifacts:",
    ]
    for day in summary["days"]:
        lines.append(f"- {day['day_label']} [`summary.json`](/home/ubuntu/testingproject/{day['edge_summary_path']})")
    lines.extend(
        [
            "",
            "## Headline",
            "",
            f"- mean survival: `{summary['distribution']['mean_survival']}`",
            f"- median survival: `{summary['distribution']['median_survival']}`",
            f"- min survival: `{summary['distribution']['min_survival']}`",
            f"- max survival: `{summary['distribution']['max_survival']}`",
            f"- weak days: `{len(summary['distribution']['weak_days'])}` {summary['distribution']['weak_days']}",
            f"- middle days: `{len(summary['distribution']['middle_days'])}` {summary['distribution']['middle_days']}",
            f"- strong days: `{len(summary['distribution']['strong_days'])}` {summary['distribution']['strong_days']}",
            "",
            "Day 7 remains the only strong-survival day. Day 9, Day 10, and Day 11 form the current middle band. Day 4 and Day 8 remain the weak edge-survival days.",
            "",
            "## Comparison Table",
            "",
            "| Day | Survival | 3-trusted rate | Actionable on calibrated rows | Side-match on actionable | Availability loss | Side mismatch loss | Fill loss |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for day in summary["days"]:
        lines.append(
            "| "
            f"{day['day_label']} | "
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
            "## Interpretation",
            "",
            "- Day 7 remains the outlier strong case because both actionability and side agreement were high.",
            "- Day 4 and Day 8 remain the weak cases for different reasons: Day 4 mostly failed on availability, while Day 8 had high availability but poor side agreement.",
            "- Day 9, Day 10, and Day 11 now form the middle band. Their runtime is clean, calibration remains valuable in replay, but live survival stays modest because availability and side mismatch still absorb most of the modeled edge.",
            "- Day 11 is closer to the weak/middle cluster than to Day 7: `3.78%` survival, `29.62%` 3-trusted-venue rate, and availability loss still dominates.",
            "- Fill loss remains negligible across the clean-shadow set; the bottleneck is still upstream composite availability and then directional agreement.",
            "",
            "## Working Conclusion",
            "",
            "- runtime cleanliness is repeating",
            "- calibration is still economically meaningful in replay",
            "- Day 7 still looks like a rare strong-survival regime, not the current norm",
            "- the current norm is weak-to-middle survival with availability as the first drag",
            "- do not change policy yet; continue the frozen measurement program or stop and write the distribution-level conclusion",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    specs = [parse_spec(value) for value in args.spec]
    days = [build_day_summary(spec) for spec in specs]
    summary = {
        "comparison_label": "clean_shadow_baseline_edge_comparison",
        "classification_thresholds": {
            "weak_lt": WEAK_THRESHOLD,
            "middle_lt": STRONG_THRESHOLD,
            "strong_gte": STRONG_THRESHOLD,
        },
        "days": days,
        "distribution": build_distribution(days),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(args.output_dir / "summary.json", summary)
    (args.output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
