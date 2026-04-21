#!/usr/bin/env python3
"""Analyze wide-delta interaction structure across clean shadow days."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from analyze_clean_shadow_condition_panel import classify_survival, fair_value_delta_bucket
from analyze_side_mismatch_audit import DaySpec, build_rows, json_default, safe_ratio


WIDE_DELTA_BUCKETS = {"5c_to_10c", "gte_10c"}


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
    return DaySpec(label, session_id, Path(replay_rows), Path(shadow_decisions))


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def sum_decimal(values: list[Decimal | None]) -> Decimal:
    total = Decimal("0")
    for value in values:
        if value is not None:
            total += value
    return total


def availability_state(row: dict[str, Any]) -> str:
    if not row.get("shadow_joined"):
        return "unjoined"
    if row.get("shadow_actionable"):
        return "actionable"
    trusted = row.get("shadow_trusted_venues")
    if trusted == 3:
        return "non_actionable_3tv"
    return "non_actionable_lt3tv"


def hour_of_day(row: dict[str, Any]) -> str:
    return str(row["hour_bucket"])[11:13]


def delta_bucket(row: dict[str, Any]) -> str:
    return fair_value_delta_bucket(row)


def sort_dimension_keys(values: dict[str, Any], dimension: str) -> list[str]:
    if dimension == "hour_of_day":
        return sorted(values, key=lambda key: int(key))
    preferred = {
        "delta_bucket": ["5c_to_10c", "gte_10c"],
        "calibration_bucket": ["far_down", "lean_down", "near_mid", "lean_up", "far_up", "missing"],
        "availability_state": ["actionable", "non_actionable_3tv", "non_actionable_lt3tv", "unjoined"],
        "volatility_regime": ["stable", "medium", "volatile", "missing"],
    }.get(dimension)
    if preferred is None:
        return sorted(values)
    return [key for key in preferred if key in values] + sorted(key for key in values if key not in preferred)


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    actionable = [row for row in rows if row.get("shadow_actionable")]
    matches = [row for row in actionable if row.get("side_match") is True]
    mismatches = [row for row in actionable if row.get("side_match") is False]
    replay = sum_decimal([row.get("calibrated_pnl") for row in rows])
    shadow = sum_decimal([row.get("shadow_realized_pnl_per_contract") for row in rows])
    availability_loss = sum_decimal([row.get("calibrated_pnl") for row in rows if not row.get("shadow_actionable")])
    side_mismatch_loss = sum_decimal(
        [
            (row.get("calibrated_pnl") or Decimal("0")) - (row.get("shadow_realized_pnl_per_contract") or Decimal("0"))
            for row in mismatches
        ]
    )
    return {
        "wide_delta_row_count": len(rows),
        "shadow_actionable_rows": len(actionable),
        "side_match_rows": len(matches),
        "side_mismatch_rows": len(mismatches),
        "shadow_actionable_rate_on_wide_delta_rows": safe_ratio(len(actionable), len(rows)),
        "side_match_rate_on_actionable_rows": safe_ratio(len(matches), len(actionable)),
        "replay_expected_pnl_total_per_contract": replay,
        "shadow_realized_pnl_total_per_contract": shadow,
        "edge_survival_ratio": None if replay == 0 else shadow / replay,
        "availability_loss_per_contract": availability_loss,
        "side_mismatch_loss_per_contract": side_mismatch_loss,
    }


def summarize_dimension(
    rows: list[dict[str, Any]],
    *,
    dimension: str,
    key_fn: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[key_fn(row)].append(row)
    summary: dict[str, Any] = {}
    for key in sort_dimension_keys(buckets, dimension):
        summary[key] = summarize_rows(buckets[key])
    return summary


def top_hours(summary: dict[str, Any], *, limit: int = 8, min_rows: int = 25) -> list[dict[str, Any]]:
    eligible = [
        {
            "hour_of_day": hour,
            **values,
        }
        for hour, values in summary.items()
        if values["wide_delta_row_count"] >= min_rows and values["side_match_rate_on_actionable_rows"] is not None
    ]
    return sorted(
        eligible,
        key=lambda row: (
            Decimal(str(row["side_match_rate_on_actionable_rows"])),
            row["wide_delta_row_count"],
            row["hour_of_day"],
        ),
        reverse=True,
    )[:limit]


def wide_delta_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if fair_value_delta_bucket(row) in WIDE_DELTA_BUCKETS]


def day_summary(spec: DaySpec) -> dict[str, Any]:
    rows = build_rows(spec)
    overall_replay = sum_decimal([row.get("calibrated_pnl") for row in rows])
    overall_shadow = sum_decimal([row.get("shadow_realized_pnl_per_contract") for row in rows])
    overall_survival = None if overall_replay == 0 else overall_shadow / overall_replay
    wide_rows = wide_delta_rows(rows)
    dimensions = {
        "delta_bucket": summarize_dimension(wide_rows, dimension="delta_bucket", key_fn=delta_bucket),
        "hour_of_day": summarize_dimension(wide_rows, dimension="hour_of_day", key_fn=hour_of_day),
        "calibration_bucket": summarize_dimension(
            wide_rows,
            dimension="calibration_bucket",
            key_fn=lambda row: str(row.get("calibration_bucket") or "missing"),
        ),
        "volatility_regime": summarize_dimension(
            wide_rows,
            dimension="volatility_regime",
            key_fn=lambda row: str(row.get("volatility_regime") or "missing"),
        ),
        "availability_state": summarize_dimension(
            wide_rows,
            dimension="availability_state",
            key_fn=availability_state,
        ),
    }
    return {
        "day_label": spec.label,
        "session_id": spec.session_id,
        "classification": classify_survival(overall_survival or Decimal("0")),
        "overall_edge_survival_ratio": overall_survival,
        "wide_delta_summary": summarize_rows(wide_rows),
        "dimensions": dimensions,
        "best_hours": top_hours(dimensions["hour_of_day"]),
    }


def cohort_summary(label: str, days: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_dimension: dict[str, dict[str, list[dict[str, Any]]]] = {
        "delta_bucket": defaultdict(list),
        "hour_of_day": defaultdict(list),
        "calibration_bucket": defaultdict(list),
        "volatility_regime": defaultdict(list),
        "availability_state": defaultdict(list),
    }
    all_wide_rows: list[dict[str, Any]] = []
    for day in days:
        spec = DaySpec(day["day_label"], day["session_id"], Path(day["replay_rows_path"]), Path(day["shadow_decisions_path"]))
        rows = wide_delta_rows(build_rows(spec))
        all_wide_rows.extend(rows)
    dimensions = {
        "delta_bucket": summarize_dimension(all_wide_rows, dimension="delta_bucket", key_fn=delta_bucket),
        "hour_of_day": summarize_dimension(all_wide_rows, dimension="hour_of_day", key_fn=hour_of_day),
        "calibration_bucket": summarize_dimension(
            all_wide_rows,
            dimension="calibration_bucket",
            key_fn=lambda row: str(row.get("calibration_bucket") or "missing"),
        ),
        "volatility_regime": summarize_dimension(
            all_wide_rows,
            dimension="volatility_regime",
            key_fn=lambda row: str(row.get("volatility_regime") or "missing"),
        ),
        "availability_state": summarize_dimension(
            all_wide_rows,
            dimension="availability_state",
            key_fn=availability_state,
        ),
    }
    return {
        "label": label,
        "days": [day["day_label"] for day in days],
        "wide_delta_summary": summarize_rows(all_wide_rows),
        "dimensions": dimensions,
        "best_hours": top_hours(dimensions["hour_of_day"]),
    }


def render_bucket_line(bucket: str, values: dict[str, Any]) -> str:
    return (
        f"| {bucket} | `{values['wide_delta_row_count']}` | "
        f"`{values['shadow_actionable_rate_on_wide_delta_rows']}` | "
        f"`{values['side_match_rate_on_actionable_rows']}` | "
        f"`{values['edge_survival_ratio']}` | "
        f"`{values['availability_loss_per_contract']}` |"
    )


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Clean Shadow Wide-Delta Interaction Panel",
        "",
        "Scope:",
        "- wide-delta comparable rows only: `5c_to_10c` and `gte_10c`",
        "- clean shadow baseline days only",
        "- focused comparison: Day 8 vs middle cohort Day 9/10/11",
        "",
        "## Day-Level Wide-Delta Summary",
        "",
        "| Day | Class | Wide rows | Actionable rate | Side-match rate | Survival | Availability loss |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for day in summary["days"]:
        values = day["wide_delta_summary"]
        lines.append(
            f"| {day['day_label']} | {day['classification']} | "
            f"`{values['wide_delta_row_count']}` | "
            f"`{values['shadow_actionable_rate_on_wide_delta_rows']}` | "
            f"`{values['side_match_rate_on_actionable_rows']}` | "
            f"`{values['edge_survival_ratio']}` | "
            f"`{values['availability_loss_per_contract']}` |"
        )

    for cohort_name in ("day8_only", "middle_days"):
        cohort = summary["cohorts"][cohort_name]
        lines.extend(
            [
                "",
                f"## {cohort['label']}",
                "",
                "### Delta Bucket",
                "",
                "| Bucket | Rows | Actionable rate | Side-match rate | Survival | Availability loss |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for bucket, values in cohort["dimensions"]["delta_bucket"].items():
            lines.append(render_bucket_line(bucket, values))

        lines.extend(
            [
                "",
                "### Calibration Bucket",
                "",
                "| Bucket | Rows | Actionable rate | Side-match rate | Survival | Availability loss |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for bucket, values in cohort["dimensions"]["calibration_bucket"].items():
            if values["wide_delta_row_count"] >= 25:
                lines.append(render_bucket_line(bucket, values))

        lines.extend(
            [
                "",
                "### Volatility Regime",
                "",
                "| Regime | Rows | Actionable rate | Side-match rate | Survival | Availability loss |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for bucket, values in cohort["dimensions"]["volatility_regime"].items():
            if values["wide_delta_row_count"] >= 25:
                lines.append(render_bucket_line(bucket, values))

        lines.extend(
            [
                "",
                "### Availability State",
                "",
                "| State | Rows | Actionable rate | Side-match rate | Survival | Availability loss |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for bucket, values in cohort["dimensions"]["availability_state"].items():
            lines.append(render_bucket_line(bucket, values))

        lines.extend(
            [
                "",
                "### Best Hours By Side-Match",
                "",
                "| Hour | Rows | Actionable rate | Side-match rate | Survival |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in cohort["best_hours"]:
            lines.append(
                f"| {row['hour_of_day']} | `{row['wide_delta_row_count']}` | "
                f"`{row['shadow_actionable_rate_on_wide_delta_rows']}` | "
                f"`{row['side_match_rate_on_actionable_rows']}` | "
                f"`{row['edge_survival_ratio']}` |"
            )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- This panel is diagnostic only. It tests whether a second condition separates wide-delta rows that still survive from wide-delta rows that fail.",
            "- The highest-value comparison is Day 8 versus the middle cohort Day 9/10/11.",
            "- If no interaction axis separates those cohorts cleanly, then the delta finding is real but insufficient as a standalone refinement path.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    specs = [parse_spec(value) for value in args.spec]
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    days: list[dict[str, Any]] = []
    for spec in specs:
        summary = day_summary(spec)
        summary["replay_rows_path"] = str(spec.replay_rows)
        summary["shadow_decisions_path"] = str(spec.shadow_decisions)
        days.append(summary)

    by_label = {day["day_label"]: day for day in days}
    cohorts = {
        "day8_only": cohort_summary("Day 8 only", [by_label["Day 8"]]),
        "middle_days": cohort_summary("Middle days (Day 9 / Day 10 / Day 11)", [by_label["Day 9"], by_label["Day 10"], by_label["Day 11"]]),
    }

    summary = {
        "comparison_label": "clean_shadow_wide_delta_interactions",
        "days": days,
        "cohorts": cohorts,
    }
    dump_json(output_dir / "summary.json", summary)
    (output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
