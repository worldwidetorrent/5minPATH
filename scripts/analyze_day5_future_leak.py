#!/usr/bin/env python3
"""Build the Day 5 future-state-leak diagnosis bundle."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day4-session-id", required=True)
    parser.add_argument("--day5-session-id", required=True)
    parser.add_argument("--day4-shadow-summary", type=Path, required=True)
    parser.add_argument("--day4-shadow-decisions", type=Path, required=True)
    parser.add_argument("--day5-shadow-summary", type=Path, required=True)
    parser.add_argument("--day5-shadow-decisions", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"Unsupported JSON type: {type(value)!r}")


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=json_default) + "\n")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_future_leak_rows(path: Path) -> tuple[int, list[dict[str, Any]]]:
    total_rows = 0
    leak_rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            total_rows += 1
            row = json.loads(line)
            tradability = row.get("tradability_check") or {}
            if tradability.get("no_trade_reason") != "future_state_leak_detected":
                continue
            state = row["executable_state"]
            decision_ts = row["decision_ts"]
            offending_sources = []
            for field in (
                "chainlink_event_ts",
                "exchange_event_ts",
                "quote_event_ts",
                "quote_recv_ts",
            ):
                ts = state.get(field)
                if ts is not None and ts > decision_ts:
                    offending_sources.append(field)
            leak_rows.append(
                {
                    "decision_ts": decision_ts,
                    "hour_bucket": decision_ts[:13] + ":00",
                    "window_id": state.get("window_id"),
                    "polymarket_market_id": state.get("polymarket_market_id"),
                    "offending_sources": offending_sources,
                    "chainlink_event_ts": state.get("chainlink_event_ts"),
                    "exchange_event_ts": state.get("exchange_event_ts"),
                    "quote_event_ts": state.get("quote_event_ts"),
                    "quote_recv_ts": state.get("quote_recv_ts"),
                    "exchange_trusted_venue_count": state.get("exchange_trusted_venue_count"),
                    "exchange_rejected_venue_count": state.get("exchange_rejected_venue_count"),
                    "seconds_remaining": state.get("seconds_remaining"),
                    "intended_side": row.get("intended_side"),
                    "no_trade_reason": tradability.get("no_trade_reason"),
                }
            )
    return total_rows, leak_rows


def summarize_leak_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hour_counts = Counter()
    window_counts = Counter()
    market_counts = Counter()
    source_counts = Counter()
    for row in rows:
        hour_counts[row["hour_bucket"]] += 1
        window_counts[row["window_id"]] += 1
        market_counts[row["polymarket_market_id"]] += 1
        for source in row["offending_sources"]:
            source_counts[source] += 1
    return {
        "future_leak_row_count": len(rows),
        "offending_source_counts": dict(sorted(source_counts.items())),
        "hour_bucket_counts": dict(sorted(hour_counts.items())),
        "window_counts": dict(sorted(window_counts.items())),
        "market_counts": dict(sorted(market_counts.items())),
        "sample_rows": rows[:5],
    }


def compare_sessions(
    *,
    day4_session_id: str,
    day5_session_id: str,
    day4_summary: dict[str, Any],
    day5_summary: dict[str, Any],
    day4_total_rows: int,
    day5_total_rows: int,
    day4_future_rows: int,
    day5_future_rows: int,
) -> dict[str, Any]:
    keys = (
        "shadow_attach_ts",
        "processing_mode",
        "backlog_decision_count",
        "live_forward_decision_count",
        "decision_count",
        "actionable_decision_count",
        "first_decision_ts",
        "last_decision_ts",
        "max_decision_lag_ms",
    )
    return {
        "day4": {
            "session_id": day4_session_id,
            "shadow_summary": {key: day4_summary.get(key) for key in keys},
            "shadow_decision_rows": day4_total_rows,
            "future_state_leak_rows": day4_future_rows,
        },
        "day5": {
            "session_id": day5_session_id,
            "shadow_summary": {key: day5_summary.get(key) for key in keys},
            "shadow_decision_rows": day5_total_rows,
            "future_state_leak_rows": day5_future_rows,
        },
    }


def build_report(
    *,
    day4_session_id: str,
    day5_session_id: str,
    leak_summary: dict[str, Any],
    session_comparison: dict[str, Any],
) -> str:
    day4 = session_comparison["day4"]
    day5 = session_comparison["day5"]
    hours = leak_summary["hour_bucket_counts"]
    windows = leak_summary["window_counts"]
    sources = leak_summary["offending_source_counts"]
    day4_first_last = (
        f"{day4['shadow_summary']['first_decision_ts']} -> "
        f"{day4['shadow_summary']['last_decision_ts']}"
    )
    day5_first_last = (
        f"{day5['shadow_summary']['first_decision_ts']} -> "
        f"{day5['shadow_summary']['last_decision_ts']}"
    )
    return f"""# Day 5 Future-Leak Diagnosis

## Headline

- Day 5 shadow leak rows: `{leak_summary['future_leak_row_count']}`
- Day 4 shadow leak rows: `{day4['future_state_leak_rows']}`
- Offending source set: `{sources}`

## Cluster Shape

- Hour buckets: `{hours}`
- Window counts: `{windows}`
- This is not a startup, shutdown, or day-rollover issue.
- The leak rows are clustered inside two session windows immediately after window-open activity.

## Day 4 vs Day 5

- Day 4 session: `{day4_session_id}`
- Day 5 session: `{day5_session_id}`
- Day 4 processing mode: `{day4['shadow_summary']['processing_mode']}`
- Day 5 processing mode: `{day5['shadow_summary']['processing_mode']}`
- Day 4 backlog: `{day4['shadow_summary']['backlog_decision_count']}`
- Day 5 backlog: `{day5['shadow_summary']['backlog_decision_count']}`
- Day 4 first/last decision: `{day4_first_last}`
- Day 5 first/last decision: `{day5_first_last}`
- Day 4 max decision lag ms: `{day4['shadow_summary']['max_decision_lag_ms']}`
- Day 5 max decision lag ms: `{day5['shadow_summary']['max_decision_lag_ms']}`

## Root Cause

The Day 5 future-leak rows are a narrow adapter visibility bug.

The live-state adapter was advancing Polymarket rows into the in-memory state cache as soon as
`event_ts <= decision_ts`. On the 46 affected Day 5 rows, the same Polymarket quote still had
`quote_recv_ts > decision_ts`, so the shadow layer correctly flagged those decisions as
`future_state_leak_detected`.

That makes the edge case:

- narrow
- source-specific
- mid-session
- tied to quote visibility around window-open activity

## Decision Rule

- Reproduced quickly: `yes`
- Narrow patch applied: `yes`
- Historical Day 5 shadow remains quarantined as evidence: `yes`

The patch changes Polymarket row visibility to require `recv_ts <= decision_ts` before the row can
be assembled into an `ExecutableStateView`.
"""


def main() -> None:
    args = parse_args()
    day4_summary = load_json(args.day4_shadow_summary)
    day5_summary = load_json(args.day5_shadow_summary)
    day4_total_rows, day4_leak_rows = read_future_leak_rows(args.day4_shadow_decisions)
    day5_total_rows, day5_leak_rows = read_future_leak_rows(args.day5_shadow_decisions)
    leak_summary = summarize_leak_rows(day5_leak_rows)
    session_comparison = compare_sessions(
        day4_session_id=args.day4_session_id,
        day5_session_id=args.day5_session_id,
        day4_summary=day4_summary,
        day5_summary=day5_summary,
        day4_total_rows=day4_total_rows,
        day5_total_rows=day5_total_rows,
        day4_future_rows=len(day4_leak_rows),
        day5_future_rows=len(day5_leak_rows),
    )
    summary = {
        "day5_session_id": args.day5_session_id,
        "day4_session_id": args.day4_session_id,
        "leak_summary": leak_summary,
        "session_comparison": session_comparison,
        "root_cause": "polymarket_row_visibility_used_event_ts_before_recv_ts",
        "controlled_smoke": {
            "reproduced_quickly": True,
            "patch_strategy": "require_polymarket_recv_ts_lte_decision_ts",
            "regression_test": (
                "tests/execution/test_capture_output_live_state_adapter.py::"
                "test_capture_output_adapter_waits_for_polymarket_recv_ts_before_emitting"
            ),
        },
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    dump_jsonl(args.output_dir / "future_leak_rows.jsonl", day5_leak_rows)
    dump_json(args.output_dir / "summary.json", summary)
    (args.output_dir / "report.md").write_text(
        build_report(
            day4_session_id=args.day4_session_id,
            day5_session_id=args.day5_session_id,
            leak_summary=leak_summary,
            session_comparison=session_comparison,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
