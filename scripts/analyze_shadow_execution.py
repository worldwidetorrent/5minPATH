#!/usr/bin/env python3
"""Build shadow execution diagnostics for one session.

This script reads durable shadow decision artifacts plus calibrated replay rows
and emits three analysis bundles:

1. Shadow-only execution-gap Stage A
2. Replay-vs-shadow execution-gap Stage B
3. Event-time clock skew audit
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--shadow-decisions", type=Path, required=True)
    parser.add_argument("--shadow-summary", type=Path, required=True)
    parser.add_argument("--replay-rows", type=Path, required=True)
    parser.add_argument("--stage-a-output-dir", type=Path, required=True)
    parser.add_argument("--stage-b-output-dir", type=Path, required=True)
    parser.add_argument("--skew-output-dir", type=Path, required=True)
    return parser.parse_args()


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"Unsupported JSON type: {type(value)!r}")


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, default=json_default) + "\n",
        encoding="utf-8",
    )


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=json_default) + "\n")


def pct(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def expected_fill_price(row: dict[str, Any]) -> Decimal | None:
    trade_direction = row["calibrated_trade_direction"]
    if trade_direction == "no_trade":
        return None
    f = Decimal(str(row["calibrated_f"]))
    raw_edge = Decimal(str(row["calibrated_selected_raw_edge"]))
    if trade_direction == "buy_up":
        return f - raw_edge
    if trade_direction == "buy_down":
        return (Decimal("1") - f) - raw_edge
    raise ValueError(f"Unsupported trade direction: {trade_direction}")


def side_from_trade_direction(value: str) -> str | None:
    if value == "buy_up":
        return "up"
    if value == "buy_down":
        return "down"
    return None


def percentile(values: list[Decimal], q: float) -> Decimal | None:
    if not values:
        return None
    idx = min(len(values) - 1, max(0, int(q * (len(values) - 1))))
    return values[idx]


def average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


@dataclass(slots=True)
class ShadowRow:
    decision_ts: datetime
    hour_bucket: str
    key: tuple[str, str, str, str]
    intended_side: str | None
    intended_entry_price: Decimal | None
    top_of_book_price: Decimal | None
    displayed_entry_size_contracts: Decimal | None
    selected_spread_abs: Decimal | None
    quote_age_ms: int | None
    actionable: bool
    trusted_venues: int
    rejected_venues: int
    fair_value_present: bool
    calibrated_fair_value_present: bool
    volatility_regime: str | None
    window_quality_regime: str | None
    no_trade_reason: str | None
    size_coverage_passed: bool
    spread_passed: bool
    freshness_passed: bool
    state_invalid_reason: str | None
    state_diagnostics: list[str]
    raw: dict[str, Any]


def shadow_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    state = row["executable_state"]
    return (
        state["session_id"],
        state["window_id"],
        state["polymarket_market_id"],
        state["snapshot_ts"],
    )


def build_shadow_rows(
    path: Path,
) -> tuple[dict[tuple[str, str, str, str], ShadowRow], list[dict[str, Any]]]:
    shadow_rows: dict[tuple[str, str, str, str], ShadowRow] = {}
    source_rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            source_rows.append(row)
            decision_ts = parse_utc(row["decision_ts"])
            state = row["executable_state"]
            tradability = row["tradability_check"]
            intended_side = row.get("intended_side")
            intended_entry_price = to_decimal(tradability.get("intended_entry_price"))
            top_of_book_price = None
            if intended_side == "up":
                top_of_book_price = to_decimal(state.get("up_ask_price"))
            elif intended_side == "down":
                top_of_book_price = to_decimal(state.get("down_ask_price"))
            diagnostics = state.get("state_diagnostics") or []
            if isinstance(diagnostics, dict):
                diagnostics = [f"{key}={value}" for key, value in sorted(diagnostics.items())]
            shadow_rows[shadow_key(row)] = ShadowRow(
                decision_ts=decision_ts,
                hour_bucket=decision_ts.strftime("%Y-%m-%d %H:00"),
                key=shadow_key(row),
                intended_side=intended_side,
                intended_entry_price=intended_entry_price,
                top_of_book_price=top_of_book_price,
                displayed_entry_size_contracts=to_decimal(
                    tradability.get("displayed_entry_size_contracts")
                ),
                selected_spread_abs=to_decimal(tradability.get("selected_spread_abs")),
                quote_age_ms=tradability.get("quote_age_ms"),
                actionable=bool(tradability.get("is_actionable")),
                trusted_venues=int(state.get("exchange_trusted_venue_count", 0)),
                rejected_venues=int(state.get("exchange_rejected_venue_count", 0)),
                fair_value_present=state.get("fair_value_base") is not None,
                calibrated_fair_value_present=state.get("calibrated_fair_value_base") is not None,
                volatility_regime=state.get("volatility_regime"),
                window_quality_regime=state.get("window_quality_regime"),
                no_trade_reason=tradability.get("no_trade_reason"),
                size_coverage_passed=bool(tradability.get("size_coverage_passed")),
                spread_passed=bool(tradability.get("spread_passed")),
                freshness_passed=bool(tradability.get("freshness_passed")),
                state_invalid_reason=state.get("state_invalid_reason"),
                state_diagnostics=list(diagnostics),
                raw=row,
            )
    return shadow_rows, source_rows


def build_stage_a(
    *,
    session_id: str,
    shadow_rows: dict[tuple[str, str, str, str], ShadowRow],
    output_dir: Path,
) -> dict[str, Any]:
    rows = list(shadow_rows.values())
    actionable_rows = [row for row in rows if row.actionable]
    quote_ages = [
        Decimal(row.quote_age_ms)
        for row in actionable_rows
        if row.quote_age_ms is not None
    ]
    spreads = [
        row.selected_spread_abs
        for row in actionable_rows
        if row.selected_spread_abs is not None
    ]
    entry_slippages = [
        row.intended_entry_price - row.top_of_book_price
        for row in actionable_rows
        if row.intended_entry_price is not None and row.top_of_book_price is not None
    ]
    size_coverage_pass_count = sum(1 for row in actionable_rows if row.size_coverage_passed)
    regime_counts: Counter[str] = Counter(
        row.volatility_regime or "unknown" for row in actionable_rows
    )
    window_regime_counts: Counter[str] = Counter(
        row.window_quality_regime or "unknown" for row in actionable_rows
    )
    trust_counts: Counter[int] = Counter(row.trusted_venues for row in rows)
    actionable_by_trust: Counter[int] = Counter(
        row.trusted_venues for row in actionable_rows
    )
    top_book_pairs = [
        {
            "decision_ts": row.decision_ts,
            "window_id": row.raw["executable_state"]["window_id"],
            "polymarket_market_id": row.raw["executable_state"]["polymarket_market_id"],
            "intended_side": row.intended_side,
            "intended_entry_price": row.intended_entry_price,
            "top_of_book_price": row.top_of_book_price,
            "entry_slippage_vs_top_of_book": (
                None
                if row.intended_entry_price is None or row.top_of_book_price is None
                else row.intended_entry_price - row.top_of_book_price
            ),
            "selected_spread_abs": row.selected_spread_abs,
            "quote_age_ms": row.quote_age_ms,
            "displayed_entry_size_contracts": row.displayed_entry_size_contracts,
            "trusted_venues": row.trusted_venues,
            "volatility_regime": row.volatility_regime,
            "window_quality_regime": row.window_quality_regime,
        }
        for row in actionable_rows
    ]
    summary = {
        "session_id": session_id,
        "decision_count": len(rows),
        "actionable_decision_count": len(actionable_rows),
        "actionable_rate": pct(len(actionable_rows), len(rows)),
        "fair_value_non_null_count": sum(1 for row in rows if row.fair_value_present),
        "calibrated_fair_value_non_null_count": sum(
            1 for row in rows if row.calibrated_fair_value_present
        ),
        "average_quote_age_ms_actionable": average(quote_ages),
        "average_spread_abs_actionable": average(spreads),
        "average_entry_slippage_vs_top_of_book_actionable": average(entry_slippages),
        "size_coverage_pass_rate_actionable": pct(
            size_coverage_pass_count,
            len(actionable_rows),
        ),
        "trusted_venue_distribution": {
            str(key): trust_counts[key] for key in sorted(trust_counts)
        },
        "actionable_by_trusted_venues": {
            str(key): actionable_by_trust[key] for key in sorted(actionable_by_trust)
        },
        "volatility_regime_actionable_counts": dict(sorted(regime_counts.items())),
        "window_quality_regime_actionable_counts": dict(sorted(window_regime_counts.items())),
    }
    report_lines = [
        f"# Shadow Execution Gap Stage A — {session_id}",
        "",
        f"- decisions: `{summary['decision_count']}`",
        f"- actionable decisions: `{summary['actionable_decision_count']}`",
        (
            f"- actionable rate: `{summary['actionable_rate']:.4%}`"
            if summary["actionable_rate"] is not None
            else "- actionable rate: `n/a`"
        ),
        f"- fair value non-null: `{summary['fair_value_non_null_count']}`",
        (
            f"- calibrated fair value non-null: "
            f"`{summary['calibrated_fair_value_non_null_count']}`"
        ),
        (
            "- average quote age on actionable rows: "
            f"`{summary['average_quote_age_ms_actionable']}` ms"
        ),
        f"- average spread on actionable rows: `{summary['average_spread_abs_actionable']}`",
        (
            "- average entry slippage vs top-of-book on actionable rows: "
            f"`{summary['average_entry_slippage_vs_top_of_book_actionable']}`"
        ),
        (
            "- size coverage pass rate on actionable rows: "
            f"`{summary['size_coverage_pass_rate_actionable']:.4%}`"
            if summary["size_coverage_pass_rate_actionable"] is not None
            else "- size coverage pass rate on actionable rows: `n/a`"
        ),
        "",
        "## Regime Breakdown",
        "",
    ]
    for key, count in sorted(regime_counts.items()):
        report_lines.append(f"- volatility `{key}`: `{count}` actionable")
    for key, count in sorted(window_regime_counts.items()):
        report_lines.append(f"- window-quality `{key}`: `{count}` actionable")

    output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(output_dir / "summary.json", summary)
    dump_jsonl(output_dir / "actionable_rows.jsonl", top_book_pairs)
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def build_stage_b(
    *,
    session_id: str,
    shadow_rows: dict[tuple[str, str, str, str], ShadowRow],
    replay_rows_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    joined_rows: list[dict[str, Any]] = []
    calibrated_trade_rows = 0
    joined_trade_rows = 0
    actionable_joined_trade_rows = 0
    actionable_with_three_trusted = 0
    joined_with_three_trusted = 0
    side_match_count = 0
    hour_counts: dict[str, Counter[str]] = defaultdict(Counter)
    regime_counts: dict[str, Counter[str]] = defaultdict(Counter)
    trust_counts: dict[int, Counter[str]] = defaultdict(Counter)
    fill_deltas: list[Decimal] = []
    top_book_slippages: list[Decimal] = []

    with replay_rows_path.open(encoding="utf-8") as handle:
        for line in handle:
            replay = json.loads(line)
            key = (
                replay["session_id"],
                replay["window_id"],
                replay["polymarket_market_id"],
                replay["snapshot_ts"],
            )
            shadow = shadow_rows.get(key)
            calibrated_trade = replay["calibrated_trade_direction"] != "no_trade"
            if calibrated_trade:
                calibrated_trade_rows += 1
            if shadow is None:
                continue
            shadow_trade = shadow.actionable
            if calibrated_trade:
                joined_trade_rows += 1
                if shadow_trade:
                    actionable_joined_trade_rows += 1
                if shadow.trusted_venues == 3:
                    joined_with_three_trusted += 1
                    if shadow_trade:
                        actionable_with_three_trusted += 1

            calibrated_side = side_from_trade_direction(replay["calibrated_trade_direction"])
            expected_fill = expected_fill_price(replay)
            fill_delta = None
            if (
                calibrated_trade
                and shadow_trade
                and calibrated_side == shadow.intended_side
                and shadow.intended_entry_price is not None
                and expected_fill is not None
            ):
                fill_delta = shadow.intended_entry_price - expected_fill
                fill_deltas.append(fill_delta)
            if calibrated_trade and shadow_trade and calibrated_side == shadow.intended_side:
                side_match_count += 1
            if (
                shadow_trade
                and shadow.intended_entry_price is not None
                and shadow.top_of_book_price is not None
            ):
                top_book_slippages.append(
                    shadow.intended_entry_price - shadow.top_of_book_price
                )

            status = "actionable" if shadow_trade else "not_actionable"
            hour_counts[shadow.hour_bucket][status] += int(calibrated_trade)
            regime_counts[shadow.volatility_regime or "unknown"][status] += int(calibrated_trade)
            trust_counts[shadow.trusted_venues][status] += int(calibrated_trade)

            joined_rows.append(
                {
                    "decision_ts": shadow.decision_ts,
                    "snapshot_ts": replay["snapshot_ts"],
                    "window_id": replay["window_id"],
                    "polymarket_market_id": replay["polymarket_market_id"],
                    "hour_bucket": shadow.hour_bucket,
                    "volatility_regime": shadow.volatility_regime,
                    "window_quality_regime": shadow.window_quality_regime,
                    "trusted_venues": shadow.trusted_venues,
                    "calibrated_trade_direction": replay["calibrated_trade_direction"],
                    "calibrated_preferred_side": replay["calibrated_preferred_side"],
                    "calibrated_selected_net_edge": to_decimal(
                        replay.get("calibrated_selected_net_edge")
                    ),
                    "calibrated_expected_fill_price": expected_fill,
                    "shadow_actionable": shadow_trade,
                    "shadow_intended_side": shadow.intended_side,
                    "shadow_intended_entry_price": shadow.intended_entry_price,
                    "shadow_top_of_book_price": shadow.top_of_book_price,
                    "shadow_entry_slippage_vs_top_of_book": (
                        None
                        if shadow.intended_entry_price is None
                        or shadow.top_of_book_price is None
                        else shadow.intended_entry_price - shadow.top_of_book_price
                    ),
                    "shadow_quote_age_ms": shadow.quote_age_ms,
                    "shadow_spread_abs": shadow.selected_spread_abs,
                    "fill_price_delta_shadow_minus_replay": fill_delta,
                    "side_match": (
                        calibrated_trade
                        and shadow_trade
                        and calibrated_side == shadow.intended_side
                    ),
                }
            )

    joined_rows.sort(key=lambda item: item["decision_ts"])
    summary = {
        "session_id": session_id,
        "replay_row_count": sum(1 for _ in replay_rows_path.open(encoding="utf-8")),
        "joined_row_count": len(joined_rows),
        "calibrated_trade_row_count": calibrated_trade_rows,
        "joined_calibrated_trade_row_count": joined_trade_rows,
        "shadow_actionable_on_joined_calibrated_rows": actionable_joined_trade_rows,
        "shadow_actionable_rate_on_joined_calibrated_rows": pct(
            actionable_joined_trade_rows,
            joined_trade_rows,
        ),
        "joined_calibrated_rows_with_3_trusted_venues": joined_with_three_trusted,
        "shadow_actionable_rate_given_3_trusted_venues": pct(
            actionable_with_three_trusted,
            joined_with_three_trusted,
        ),
        "side_match_count": side_match_count,
        "side_match_rate_on_actionable_joined_calibrated_rows": pct(
            side_match_count,
            actionable_joined_trade_rows,
        ),
        "mean_shadow_minus_replay_fill": average(fill_deltas),
        "mean_entry_slippage_vs_top_of_book": average(top_book_slippages),
        "hourly_actionability": _format_group_counts(hour_counts),
        "volatility_regime_actionability": _format_group_counts(regime_counts),
        "trusted_venue_actionability": _format_group_counts(trust_counts),
    }
    report_lines = [
        f"# Shadow Execution Gap Stage B — {session_id}",
        "",
        f"- joined calibrated replay trade rows: `{joined_trade_rows}`",
        f"- shadow actionable on joined calibrated rows: `{actionable_joined_trade_rows}`",
        (
            f"- shadow actionable rate on joined calibrated rows: "
            f"`{summary['shadow_actionable_rate_on_joined_calibrated_rows']:.4%}`"
            if summary["shadow_actionable_rate_on_joined_calibrated_rows"] is not None
            else "- shadow actionable rate on joined calibrated rows: `n/a`"
        ),
        (
            "- shadow actionable rate given 3 trusted venues: "
            f"`{summary['shadow_actionable_rate_given_3_trusted_venues']:.4%}`"
            if summary["shadow_actionable_rate_given_3_trusted_venues"] is not None
            else "- shadow actionable rate given 3 trusted venues: `n/a`"
        ),
        f"- mean shadow minus replay expected fill: `{summary['mean_shadow_minus_replay_fill']}`",
        (
            "- mean shadow entry slippage vs top-of-book: "
            f"`{summary['mean_entry_slippage_vs_top_of_book']}`"
        ),
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(output_dir / "summary.json", summary)
    dump_jsonl(output_dir / "joined_rows.jsonl", joined_rows)
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def build_skew_audit(
    *,
    session_id: str,
    source_rows: list[dict[str, Any]],
    output_dir: Path,
) -> dict[str, Any]:
    skew_rows: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    magnitude_counts: Counter[str] = Counter()
    hourly_counts: Counter[str] = Counter()
    actionable_counts: Counter[str] = Counter()

    for row in source_rows:
        state = row["executable_state"]
        tradability = row["tradability_check"]
        if tradability.get("no_trade_reason") != "future_event_clock_skew" and state.get(
            "state_invalid_reason"
        ) != "future_event_clock_skew":
            continue
        decision_ts = parse_utc(row["decision_ts"])
        diagnostics = state.get("state_diagnostics") or []
        if isinstance(diagnostics, dict):
            diagnostics = [f"{key}={value}" for key, value in sorted(diagnostics.items())]
        source_name = "unknown"
        magnitude = "unknown"
        source_ts = None
        for item in diagnostics:
            if not isinstance(item, str):
                continue
            if item.startswith("future_event_clock_skew:") and ":decision_ts=" not in item:
                parts = item.split(":")
                if len(parts) >= 3:
                    source_name = parts[1]
                    magnitude = parts[2]
            if ":decision_ts=" in item and ":source_ts=" in item:
                try:
                    source_ts = item.split(":source_ts=", 1)[1]
                except IndexError:
                    source_ts = None
        source_counts[source_name] += 1
        magnitude_counts[magnitude] += 1
        hour_bucket = decision_ts.strftime("%Y-%m-%d %H:00")
        hourly_counts[hour_bucket] += 1
        actionable_key = (
            "actionable" if tradability.get("is_actionable") else "not_actionable"
        )
        actionable_counts[actionable_key] += 1
        skew_rows.append(
            {
                "decision_ts": decision_ts,
                "hour_bucket": hour_bucket,
                "window_id": state.get("window_id"),
                "polymarket_market_id": state.get("polymarket_market_id"),
                "source": source_name,
                "magnitude_bucket": magnitude,
                "source_timestamp": source_ts,
                "quote_event_ts": state.get("quote_event_ts"),
                "quote_recv_ts": state.get("quote_recv_ts"),
                "actionable": bool(tradability.get("is_actionable")),
                "trusted_venues": state.get("exchange_trusted_venue_count"),
                "no_trade_reason": tradability.get("no_trade_reason"),
                "state_invalid_reason": state.get("state_invalid_reason"),
                "state_diagnostics": diagnostics,
            }
        )

    skew_rows.sort(key=lambda item: item["decision_ts"])
    summary = {
        "session_id": session_id,
        "future_event_clock_skew_row_count": len(skew_rows),
        "source_breakdown": dict(sorted(source_counts.items())),
        "magnitude_histogram": dict(sorted(magnitude_counts.items())),
        "hourly_clustering": {
            hour: count for hour, count in sorted(hourly_counts.items())
        },
        "actionable_vs_non_actionable": dict(sorted(actionable_counts.items())),
        "actionable_rate_on_skew_rows": pct(
            actionable_counts["actionable"],
            len(skew_rows),
        ),
    }
    report_lines = [
        f"# Event-Time Clock Skew Audit — {session_id}",
        "",
        f"- future_event_clock_skew rows: `{len(skew_rows)}`",
        "",
        "## Source Breakdown",
        "",
    ]
    for key, count in sorted(source_counts.items()):
        report_lines.append(f"- `{key}`: `{count}`")
    report_lines += ["", "## Magnitude Histogram", ""]
    for key, count in sorted(magnitude_counts.items()):
        report_lines.append(f"- `{key}`: `{count}`")
    report_lines += ["", "## Actionability", ""]
    for key, count in sorted(actionable_counts.items()):
        report_lines.append(f"- `{key}`: `{count}`")

    output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(output_dir / "summary.json", summary)
    dump_jsonl(output_dir / "skew_rows.jsonl", skew_rows)
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def _format_group_counts(group_counts: dict[Any, Counter[str]]) -> dict[str, dict[str, Any]]:
    return {
        str(key): {
            "joined_calibrated_trade_rows": counts["actionable"] + counts["not_actionable"],
            "shadow_actionable_rows": counts["actionable"],
            "shadow_actionable_rate": pct(
                counts["actionable"],
                counts["actionable"] + counts["not_actionable"],
            ),
        }
        for key, counts in sorted(group_counts.items(), key=lambda item: str(item[0]))
    }


def main() -> None:
    args = parse_args()
    shadow_rows, source_rows = build_shadow_rows(args.shadow_decisions)
    build_stage_a(
        session_id=args.session_id,
        shadow_rows=shadow_rows,
        output_dir=args.stage_a_output_dir,
    )
    build_stage_b(
        session_id=args.session_id,
        shadow_rows=shadow_rows,
        replay_rows_path=args.replay_rows,
        output_dir=args.stage_b_output_dir,
    )
    build_skew_audit(
        session_id=args.session_id,
        source_rows=source_rows,
        output_dir=args.skew_output_dir,
    )


if __name__ == "__main__":
    main()
