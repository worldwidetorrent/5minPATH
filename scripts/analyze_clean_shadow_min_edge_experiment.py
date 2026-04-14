#!/usr/bin/env python3
"""Run offline minimum-edge filters across clean shadow baseline days."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from analyze_side_mismatch_audit import DaySpec, build_rows, json_default, safe_ratio


FILTERS = [
    ("current", Decimal("0")),
    ("min_edge_modest_1c", Decimal("0.01")),
    ("min_edge_strict_2p5c", Decimal("0.025")),
    ("min_edge_very_strict_5c", Decimal("0.05")),
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


def passes_min_edge(row: dict[str, Any], threshold: Decimal) -> bool:
    edge = row.get("calibrated_selected_net_edge")
    if edge is None:
        return False
    return edge >= threshold


def summarize_filter(rows: list[dict[str, Any]], *, name: str, threshold: Decimal) -> dict[str, Any]:
    kept = [row for row in rows if passes_min_edge(row, threshold)]
    joined = [row for row in kept if row.get("shadow_joined")]
    actionable = [row for row in kept if row.get("shadow_actionable")]
    matches = [row for row in actionable if row.get("side_match") is True]
    mismatches = [row for row in actionable if row.get("side_match") is False]
    replay_pnl = sum((row.get("calibrated_pnl") or Decimal("0")) for row in kept)
    shadow_pnl = sum((row.get("shadow_realized_pnl_per_contract") or Decimal("0")) for row in kept)

    return {
        "experiment": name,
        "min_selected_net_edge": threshold,
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
    }


def summarize_day(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [summarize_filter(rows, name=name, threshold=threshold) for name, threshold in FILTERS]


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Clean Shadow Minimum-Edge Filter Experiment",
        "",
        "This is an offline diagnostic only. No live policy, calibrator, venue, or shadow-runtime behavior changed.",
        "",
        "## Scope",
    ]
    for day in summary["days"]:
        lines.append(f"- {day['day_label']} `{day['session_id']}`")
    lines.extend(
        [
            "",
            "## Aggregate Result",
            "",
            "| Experiment | Kept rows | Kept rate | Replay PnL | Shadow PnL | Survival | Actionable rate | Side-match rate |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary["aggregate_experiments"]:
        lines.append(
            "| "
            f"{row['experiment']} | "
            f"`{row['kept_calibrated_trade_rows']}` | "
            f"`{row['kept_row_rate']}` | "
            f"`{row['replay_expected_pnl_on_kept_rows']}` | "
            f"`{row['shadow_realized_pnl_on_kept_rows']}` | "
            f"`{row['edge_survival_ratio_on_kept_rows']}` | "
            f"`{row['shadow_actionable_rate_on_kept_rows']}` | "
            f"`{row['side_match_rate_on_actionable_rows']}` |"
        )
    lines.extend(["", "## Per-Day Results", ""])
    for day in summary["days"]:
        lines.extend(
            [
                f"### {day['day_label']}",
                "",
                "| Experiment | Kept rows | Replay PnL | Shadow PnL | Survival | Actionable rate | Side-match rate |",
                "|---|---:|---:|---:|---:|---:|---:|",
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
                f"`{row['side_match_rate_on_actionable_rows']}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## Interpretation",
            "",
            "- The test should be judged by whether it improves weak and middle days without destroying Day 7.",
            "- If the stricter filters only look good by deleting most replay PnL or collapsing Day 7, they are not useful policy candidates.",
            "- If a threshold improves Day 8/Day 9/Day 10 while keeping Day 7 strong, it becomes a candidate for future offline testing, not an immediate live policy change.",
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
        day_summaries.append(
            {
                "day_label": spec.label,
                "session_id": spec.session_id,
                "calibrated_trade_row_count": len(rows),
                "experiments": summarize_day(rows),
            }
        )
        dump_jsonl(output_dir / f"{spec.label.lower().replace(' ', '_')}_filter_rows.jsonl", rows)

    summary = {
        "comparison_label": "clean_shadow_min_edge_filter_experiment",
        "research_contract": "frozen; diagnostic only",
        "filters": [
            {"experiment": name, "min_selected_net_edge": threshold}
            for name, threshold in FILTERS
        ],
        "days": day_summaries,
        "aggregate_experiments": summarize_day(all_rows),
    }
    dump_json(output_dir / "summary.json", summary)
    (output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
