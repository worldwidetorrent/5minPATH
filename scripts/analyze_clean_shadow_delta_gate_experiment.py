#!/usr/bin/env python3
"""Run offline delta-gate filters across clean shadow baseline days."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from analyze_clean_shadow_condition_panel import classify_survival
from analyze_side_mismatch_audit import DaySpec, build_rows, json_default, safe_ratio


FILTERS = [
    ("current", None),
    ("exclude_gte_10c", Decimal("0.10")),
    ("exclude_gte_5c", Decimal("0.05")),
]


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


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=json_default) + "\n")


def sum_decimal(values: list[Decimal | None]) -> Decimal:
    total = Decimal("0")
    for value in values:
        if value is not None:
            total += value
    return total


def passes_delta_gate(row: dict[str, Any], threshold: Decimal | None) -> bool:
    if threshold is None:
        return True
    value = row.get("calibrated_fair_value_abs_delta_live_vs_replay")
    if value is None:
        return True
    return value < threshold


def summarize_filter(rows: list[dict[str, Any]], *, name: str, threshold: Decimal | None) -> dict[str, Any]:
    kept = [row for row in rows if passes_delta_gate(row, threshold)]
    joined = [row for row in kept if row.get("shadow_joined")]
    actionable = [row for row in kept if row.get("shadow_actionable")]
    matches = [row for row in actionable if row.get("side_match") is True]
    mismatches = [row for row in actionable if row.get("side_match") is False]
    replay_pnl = sum_decimal([row.get("calibrated_pnl") for row in kept])
    shadow_pnl = sum_decimal([row.get("shadow_realized_pnl_per_contract") for row in kept])
    availability_loss = sum_decimal(
        [row.get("calibrated_pnl") for row in kept if not row.get("shadow_actionable")]
    )
    side_mismatch_loss = sum_decimal(
        [
            (row.get("calibrated_pnl") or Decimal("0"))
            - (row.get("shadow_realized_pnl_per_contract") or Decimal("0"))
            for row in mismatches
        ]
    )
    excluded = [row for row in rows if not passes_delta_gate(row, threshold)]
    excluded_replay_pnl = sum_decimal([row.get("calibrated_pnl") for row in excluded])
    excluded_shadow_pnl = sum_decimal([row.get("shadow_realized_pnl_per_contract") for row in excluded])

    return {
        "experiment": name,
        "max_abs_live_vs_replay_calibrated_fair_value_delta": threshold,
        "kept_calibrated_trade_rows": len(kept),
        "kept_row_rate": safe_ratio(len(kept), len(rows)),
        "joined_trade_rows": len(joined),
        "joined_trade_rate_on_kept_rows": safe_ratio(len(joined), len(kept)),
        "shadow_actionable_rows": len(actionable),
        "shadow_actionable_rate_on_kept_rows": safe_ratio(len(actionable), len(kept)),
        "side_match_rows": len(matches),
        "side_mismatch_rows": len(mismatches),
        "side_match_rate_on_actionable_rows": safe_ratio(len(matches), len(actionable)),
        "replay_expected_pnl_on_kept_rows": replay_pnl,
        "shadow_realized_pnl_on_kept_rows": shadow_pnl,
        "edge_survival_ratio_on_kept_rows": None if replay_pnl == 0 else shadow_pnl / replay_pnl,
        "availability_loss_on_kept_rows": availability_loss,
        "side_mismatch_loss_on_kept_rows": side_mismatch_loss,
        "excluded_rows": len(excluded),
        "excluded_row_rate": safe_ratio(len(excluded), len(rows)),
        "excluded_replay_pnl": excluded_replay_pnl,
        "excluded_shadow_pnl": excluded_shadow_pnl,
    }


def summarize_day(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [summarize_filter(rows, name=name, threshold=threshold) for name, threshold in FILTERS]


def summarize_cohorts(day_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for day in day_summaries:
        grouped[day["classification"]].append(day)

    cohort_summaries: list[dict[str, Any]] = []
    for cohort in ("strong", "middle", "weak"):
        days = grouped.get(cohort, [])
        if not days:
            continue
        experiments: list[dict[str, Any]] = []
        for name, threshold in FILTERS:
            replay = sum_decimal(
                [day_row["replay_expected_pnl_on_kept_rows"] for day in days for day_row in day["experiments"] if day_row["experiment"] == name]
            )
            shadow = sum_decimal(
                [day_row["shadow_realized_pnl_on_kept_rows"] for day in days for day_row in day["experiments"] if day_row["experiment"] == name]
            )
            kept_rows = sum(
                day_row["kept_calibrated_trade_rows"]
                for day in days
                for day_row in day["experiments"]
                if day_row["experiment"] == name
            )
            actionable = sum(
                day_row["shadow_actionable_rows"]
                for day in days
                for day_row in day["experiments"]
                if day_row["experiment"] == name
            )
            matches = sum(
                day_row["side_match_rows"]
                for day in days
                for day_row in day["experiments"]
                if day_row["experiment"] == name
            )
            availability_loss = sum_decimal(
                [day_row["availability_loss_on_kept_rows"] for day in days for day_row in day["experiments"] if day_row["experiment"] == name]
            )
            experiments.append(
                {
                    "experiment": name,
                    "max_abs_live_vs_replay_calibrated_fair_value_delta": threshold,
                    "kept_calibrated_trade_rows": kept_rows,
                    "shadow_actionable_rows": actionable,
                    "shadow_actionable_rate_on_kept_rows": safe_ratio(actionable, kept_rows),
                    "side_match_rows": matches,
                    "side_match_rate_on_actionable_rows": safe_ratio(matches, actionable),
                    "replay_expected_pnl_on_kept_rows": replay,
                    "shadow_realized_pnl_on_kept_rows": shadow,
                    "edge_survival_ratio_on_kept_rows": None if replay == 0 else shadow / replay,
                    "availability_loss_on_kept_rows": availability_loss,
                }
            )
        cohort_summaries.append(
            {
                "cohort": cohort,
                "day_count": len(days),
                "days": [day["day_label"] for day in days],
                "experiments": experiments,
            }
        )
    return cohort_summaries


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Clean Shadow Delta-Gate Experiment",
        "",
        "This is an offline diagnostic only. No live policy, calibrator, venue, or shadow-runtime behavior changed.",
        "",
        "## Scope",
    ]
    for day in summary["days"]:
        lines.append(f"- {day['day_label']} `{day['session_id']}` ({day['classification']})")
    lines.extend(
        [
            "",
            "Gate semantics:",
            "- `current`: keep all rows",
            "- `exclude_gte_10c`: keep rows with comparable delta `< 10c`; rows with missing comparable delta stay in scope",
            "- `exclude_gte_5c`: keep rows with comparable delta `< 5c`; rows with missing comparable delta stay in scope",
            "",
            "## Aggregate Result",
            "",
            "| Experiment | Kept rows | Replay PnL | Shadow PnL | Survival | Actionable rate | Side-match rate | Availability loss | Excluded replay PnL |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary["aggregate_experiments"]:
        lines.append(
            "| "
            f"{row['experiment']} | "
            f"`{row['kept_calibrated_trade_rows']}` | "
            f"`{row['replay_expected_pnl_on_kept_rows']}` | "
            f"`{row['shadow_realized_pnl_on_kept_rows']}` | "
            f"`{row['edge_survival_ratio_on_kept_rows']}` | "
            f"`{row['shadow_actionable_rate_on_kept_rows']}` | "
            f"`{row['side_match_rate_on_actionable_rows']}` | "
            f"`{row['availability_loss_on_kept_rows']}` | "
            f"`{row['excluded_replay_pnl']}` |"
        )
    lines.extend(["", "## Per-Day Results", ""])
    for day in summary["days"]:
        lines.extend(
            [
                f"### {day['day_label']}",
                "",
                "| Experiment | Kept rows | Replay PnL | Shadow PnL | Survival | Actionable rate | Side-match rate | Excluded replay PnL |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in day["experiments"]:
            lines.append(
                "| "
                f"{row['experiment']} | "
                f"`{row['kept_calibrated_trade_rows']}` | "
                f"`{row['replay_expected_pnl_on_kept_rows']}` | "
                f"`{row['shadow_realized_pnl_on_kept_rows']}` | "
                f"`{row['edge_survival_ratio_on_kept_rows']}` | "
                f"`{row['shadow_actionable_rate_on_kept_rows']}` | "
                f"`{row['side_match_rate_on_actionable_rows']}` | "
                f"`{row['excluded_replay_pnl']}` |"
            )
        lines.append("")
    lines.extend(["## Cohort Results", ""])
    for cohort in summary["cohorts"]:
        lines.extend(
            [
                f"### {cohort['cohort'].title()}",
                "",
                f"Days: {', '.join(cohort['days'])}",
                "",
                "| Experiment | Replay PnL | Shadow PnL | Survival | Actionable rate | Side-match rate | Availability loss |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in cohort["experiments"]:
            lines.append(
                "| "
                f"{row['experiment']} | "
                f"`{row['replay_expected_pnl_on_kept_rows']}` | "
                f"`{row['shadow_realized_pnl_on_kept_rows']}` | "
                f"`{row['edge_survival_ratio_on_kept_rows']}` | "
                f"`{row['shadow_actionable_rate_on_kept_rows']}` | "
                f"`{row['side_match_rate_on_actionable_rows']}` | "
                f"`{row['availability_loss_on_kept_rows']}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## Interpretation",
            "",
            "- Judge the gate by whether it rescues weak and middle days without materially damaging Day 7.",
            "- The gate is only meaningful if it improves survival by removing the wide-delta failure state rather than just deleting too much replay PnL.",
            "- Missing comparable-delta rows stay in scope here, so this experiment isolates the explicit wide-delta hypothesis rather than turning missing-delta state into an implicit exclusion rule.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    specs = [parse_spec(value) for value in args.spec]
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    day_summaries: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for spec in specs:
        rows = build_rows(spec)
        all_rows.extend(rows)
        current = summarize_filter(rows, name="current", threshold=None)
        day_summaries.append(
            {
                "day_label": spec.label,
                "session_id": spec.session_id,
                "classification": classify_survival(current["edge_survival_ratio_on_kept_rows"] or Decimal("0")),
                "calibrated_trade_row_count": len(rows),
                "experiments": summarize_day(rows),
            }
        )
        dump_jsonl(output_dir / f"{spec.label.lower().replace(' ', '_')}_filter_rows.jsonl", rows)

    summary = {
        "comparison_label": "clean_shadow_delta_gate_experiment",
        "research_contract": "frozen; diagnostic only",
        "filters": [
            {"experiment": name, "max_abs_live_vs_replay_calibrated_fair_value_delta": threshold}
            for name, threshold in FILTERS
        ],
        "days": day_summaries,
        "aggregate_experiments": summarize_day(all_rows),
        "cohorts": summarize_cohorts(day_summaries),
    }
    dump_json(output_dir / "summary.json", summary)
    (output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
