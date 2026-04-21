#!/usr/bin/env python3
"""Build a live-vs-replay delta-bucket panel across clean-shadow days."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from analyze_clean_shadow_condition_panel import classify_survival, fair_value_delta_bucket
from analyze_side_mismatch_audit import (
    DaySpec as AuditDaySpec,
    build_rows,
    json_default,
    safe_ratio,
)


DELTA_BUCKETS = ("lt_2c", "2c_to_5c", "5c_to_10c", "gte_10c", "missing")
LOW_DELTA_BUCKETS = {"lt_2c", "2c_to_5c"}
WIDE_DELTA_BUCKETS = {"5c_to_10c", "gte_10c"}


@dataclass(frozen=True)
class DaySpec:
    label: str
    session_id: str
    replay_rows_path: Path
    shadow_decisions_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spec",
        action="append",
        required=True,
        help="Day label, session id, replay rows path, and shadow decisions path separated by |",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def parse_spec(value: str) -> DaySpec:
    parts = value.split("|")
    if len(parts) != 4:
        raise ValueError("--spec must be: label|session_id|replay_rows|shadow_decisions")
    label, session_id, replay_rows, shadow_decisions = parts
    return DaySpec(
        label=label,
        session_id=session_id,
        replay_rows_path=Path(replay_rows),
        shadow_decisions_path=Path(shadow_decisions),
    )


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def sum_decimal(values: list[Decimal | None]) -> Decimal:
    total = Decimal("0")
    for value in values:
        if value is not None:
            total += value
    return total


def summarize_bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    replay_total = sum_decimal([row.get("calibrated_pnl") for row in rows])
    realized_total = sum_decimal([row.get("shadow_realized_pnl_per_contract") for row in rows])
    actionable_rows = [row for row in rows if row.get("shadow_actionable")]
    side_match_rows = [row for row in actionable_rows if row.get("side_match") is True]
    side_mismatch_rows = [row for row in actionable_rows if row.get("side_match") is False]
    availability_loss = sum_decimal(
        [
            row.get("calibrated_pnl")
            for row in rows
            if not row.get("shadow_actionable")
        ]
    )
    side_mismatch_loss = sum_decimal(
        [
            (row.get("calibrated_pnl") or Decimal("0"))
            - (row.get("shadow_realized_pnl_per_contract") or Decimal("0"))
            for row in side_mismatch_rows
        ]
    )
    return {
        "calibrated_trade_rows": len(rows),
        "joined_rows": sum(1 for row in rows if row.get("shadow_joined")),
        "actionable_rows": len(actionable_rows),
        "side_match_rows": len(side_match_rows),
        "side_mismatch_rows": len(side_mismatch_rows),
        "joined_rate": safe_ratio(
            sum(1 for row in rows if row.get("shadow_joined")),
            len(rows),
        ),
        "actionable_rate_on_calibrated_rows": safe_ratio(len(actionable_rows), len(rows)),
        "side_match_rate_on_actionable_rows": safe_ratio(len(side_match_rows), len(actionable_rows)),
        "replay_expected_pnl_total_per_contract": replay_total,
        "shadow_realized_pnl_total_per_contract": realized_total,
        "edge_survival_ratio": None if replay_total == 0 else realized_total / replay_total,
        "availability_loss_per_contract": availability_loss,
        "side_mismatch_loss_per_contract": side_mismatch_loss,
    }


def add_contribution_shares(
    bucket_summary: dict[str, Any],
    *,
    day_replay_total: Decimal,
    day_realized_total: Decimal,
) -> dict[str, Any]:
    replay_total = bucket_summary["replay_expected_pnl_total_per_contract"]
    realized_total = bucket_summary["shadow_realized_pnl_total_per_contract"]
    bucket_summary["replay_pnl_share_of_day"] = (
        None if day_replay_total == 0 else replay_total / day_replay_total
    )
    bucket_summary["shadow_realized_pnl_share_of_day"] = (
        None if day_realized_total == 0 else realized_total / day_realized_total
    )
    return bucket_summary


def summarize_day(spec: DaySpec) -> dict[str, Any]:
    rows = build_rows(
        AuditDaySpec(
            label=spec.label,
            session_id=spec.session_id,
            replay_rows=spec.replay_rows_path,
            shadow_decisions=spec.shadow_decisions_path,
        )
    )
    day_replay_total = sum_decimal([row.get("calibrated_pnl") for row in rows])
    day_realized_total = sum_decimal([row.get("shadow_realized_pnl_per_contract") for row in rows])
    day_survival = None if day_replay_total == 0 else day_realized_total / day_replay_total

    bucket_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        bucket_rows[fair_value_delta_bucket(row)].append(row)

    bucket_summaries: dict[str, Any] = {}
    for bucket in DELTA_BUCKETS:
        summary = summarize_bucket(bucket_rows.get(bucket, []))
        bucket_summaries[bucket] = add_contribution_shares(
            summary,
            day_replay_total=day_replay_total,
            day_realized_total=day_realized_total,
        )

    low_delta_realized = sum_decimal(
        [bucket_summaries[b]["shadow_realized_pnl_total_per_contract"] for b in LOW_DELTA_BUCKETS]
    )
    wide_delta_realized = sum_decimal(
        [bucket_summaries[b]["shadow_realized_pnl_total_per_contract"] for b in WIDE_DELTA_BUCKETS]
    )
    low_delta_replay = sum_decimal(
        [bucket_summaries[b]["replay_expected_pnl_total_per_contract"] for b in LOW_DELTA_BUCKETS]
    )
    wide_delta_replay = sum_decimal(
        [bucket_summaries[b]["replay_expected_pnl_total_per_contract"] for b in WIDE_DELTA_BUCKETS]
    )

    return {
        "day_label": spec.label,
        "session_id": spec.session_id,
        "classification": classify_survival(day_survival or Decimal("0")),
        "day_replay_expected_pnl_total_per_contract": day_replay_total,
        "day_shadow_realized_pnl_total_per_contract": day_realized_total,
        "day_edge_survival_ratio": day_survival,
        "bucket_summaries": bucket_summaries,
        "low_delta_shadow_realized_share": None if day_realized_total == 0 else low_delta_realized / day_realized_total,
        "wide_delta_shadow_realized_share": None if day_realized_total == 0 else wide_delta_realized / day_realized_total,
        "low_delta_replay_share": None if day_replay_total == 0 else low_delta_replay / day_replay_total,
        "wide_delta_replay_share": None if day_replay_total == 0 else wide_delta_replay / day_replay_total,
    }


def summarize_cohort(days: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = {}
    for bucket in DELTA_BUCKETS:
        replay_total = sum_decimal(
            [day["bucket_summaries"][bucket]["replay_expected_pnl_total_per_contract"] for day in days]
        )
        realized_total = sum_decimal(
            [day["bucket_summaries"][bucket]["shadow_realized_pnl_total_per_contract"] for day in days]
        )
        actionable_rows = sum(day["bucket_summaries"][bucket]["actionable_rows"] for day in days)
        side_match_rows = sum(day["bucket_summaries"][bucket]["side_match_rows"] for day in days)
        calibrated_rows = sum(day["bucket_summaries"][bucket]["calibrated_trade_rows"] for day in days)
        availability_loss = sum_decimal(
            [day["bucket_summaries"][bucket]["availability_loss_per_contract"] for day in days]
        )
        buckets[bucket] = {
            "calibrated_trade_rows": calibrated_rows,
            "actionable_rows": actionable_rows,
            "side_match_rows": side_match_rows,
            "actionable_rate_on_calibrated_rows": safe_ratio(actionable_rows, calibrated_rows),
            "side_match_rate_on_actionable_rows": safe_ratio(side_match_rows, actionable_rows),
            "replay_expected_pnl_total_per_contract": replay_total,
            "shadow_realized_pnl_total_per_contract": realized_total,
            "edge_survival_ratio": None if replay_total == 0 else realized_total / replay_total,
            "availability_loss_per_contract": availability_loss,
        }
    return buckets


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Clean Shadow Delta-Bucket Panel",
        "",
        "Scope:",
        "- clean shadow days only",
        "- rows bucketed by live-vs-replay calibrated fair-value delta",
        "- side-match, survival, and availability measured inside each delta bucket",
        "",
        "## Day-Level Concentration",
        "",
        "| Day | Class | Survival | Low-delta replay share | Low-delta realized share | Wide-delta replay share | Wide-delta realized share |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for day in summary["days"]:
        lines.append(
            "| "
            f"{day['day_label']} | "
            f"{day['classification']} | "
            f"`{day['day_edge_survival_ratio']}` | "
            f"`{day['low_delta_replay_share']}` | "
            f"`{day['low_delta_shadow_realized_share']}` | "
            f"`{day['wide_delta_replay_share']}` | "
            f"`{day['wide_delta_shadow_realized_share']}` |"
        )
    lines.extend(["", "## Per-Day Delta Buckets", ""])
    for day in summary["days"]:
        lines.extend(
            [
                f"### {day['day_label']}",
                "",
                "| Delta bucket | Side-match on actionable | Survival | Availability loss | Replay PnL | Shadow PnL |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for bucket in DELTA_BUCKETS:
            item = day["bucket_summaries"][bucket]
            lines.append(
                "| "
                f"{bucket} | "
                f"`{item['side_match_rate_on_actionable_rows']}` | "
                f"`{item['edge_survival_ratio']}` | "
                f"`{item['availability_loss_per_contract']}` | "
                f"`{item['replay_expected_pnl_total_per_contract']}` | "
                f"`{item['shadow_realized_pnl_total_per_contract']}` |"
            )
        lines.append("")
    lines.extend(["## Cohort Delta Buckets", ""])
    for cohort_name, buckets in summary["cohort_bucket_summaries"].items():
        lines.extend(
            [
                f"### {cohort_name.title()}",
                "",
                "| Delta bucket | Side-match on actionable | Survival | Availability loss | Replay PnL | Shadow PnL |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for bucket in DELTA_BUCKETS:
            item = buckets[bucket]
            lines.append(
                "| "
                f"{bucket} | "
                f"`{item['side_match_rate_on_actionable_rows']}` | "
                f"`{item['edge_survival_ratio']}` | "
                f"`{item['availability_loss_per_contract']}` | "
                f"`{item['replay_expected_pnl_total_per_contract']}` | "
                f"`{item['shadow_realized_pnl_total_per_contract']}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## Interpretation",
            "",
            "- Day 7 should stay concentrated in the low-delta buckets if live and replay state are genuinely aligned on the strong day.",
            "- Day 8 and the weak cohort should widen into the larger-delta buckets if directional disagreement is the main failure mode after actionability.",
            "- Day 9, Day 10, and Day 11 should sit between those extremes: some low-delta contribution survives, but the wider-delta buckets absorb more availability loss and side-match decay.",
            "- If that pattern holds repeatedly, the first serious future offline rule candidate is conditional on state divergence, not a blanket edge threshold.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    specs = [parse_spec(value) for value in args.spec]
    days = [summarize_day(spec) for spec in specs]
    cohort_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for day in days:
        cohort_groups[day["classification"]].append(day)
    summary = {
        "comparison_label": "clean_shadow_delta_bucket_panel",
        "delta_bucket_definition": {
            "lt_2c": "abs(live_calibrated_f - replay_calibrated_f) < 0.02",
            "2c_to_5c": "0.02 <= abs(delta) < 0.05",
            "5c_to_10c": "0.05 <= abs(delta) < 0.10",
            "gte_10c": "abs(delta) >= 0.10",
            "missing": "delta unavailable",
        },
        "days": days,
        "cohort_bucket_summaries": {
            cohort: summarize_cohort(cohort_days)
            for cohort, cohort_days in cohort_groups.items()
        },
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(args.output_dir / "summary.json", summary)
    (args.output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
