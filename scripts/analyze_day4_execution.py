#!/usr/bin/env python3
"""Build Day 4 shadow execution diagnostics.

This script is intentionally file-driven. It reads durable shadow decision
artifacts plus calibrated replay rows and emits two analysis bundles:

1. Binance outlier audit
2. Replay-vs-shadow execution-gap pass 2
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--shadow-decisions", type=Path, required=True)
    parser.add_argument("--shadow-summary", type=Path, required=True)
    parser.add_argument("--replay-rows", type=Path, required=True)
    parser.add_argument("--binance-outlier-output-dir", type=Path, required=True)
    parser.add_argument("--pass2-output-dir", type=Path, required=True)
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
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=json_default) + "\n")


def pct(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def dec_to_float(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


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


@dataclass(slots=True)
class ShadowRow:
    decision_ts: datetime
    hour_bucket: str
    key: tuple[str, str, str, str]
    intended_side: str | None
    intended_entry_price: Decimal | None
    top_of_book_price: Decimal | None
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
    raw: dict[str, Any]


def shadow_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    state = row["executable_state"]
    return (
        state["session_id"],
        state["window_id"],
        state["polymarket_market_id"],
        state["snapshot_ts"],
    )


def build_shadow_rows(path: Path) -> tuple[dict[tuple[str, str, str, str], ShadowRow], list[dict[str, Any]]]:
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
            shadow_rows[shadow_key(row)] = ShadowRow(
                decision_ts=decision_ts,
                hour_bucket=decision_ts.strftime("%Y-%m-%d %H:00"),
                key=shadow_key(row),
                intended_side=intended_side,
                intended_entry_price=intended_entry_price,
                top_of_book_price=top_of_book_price,
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
                raw=row,
            )
    return shadow_rows, source_rows


def build_binance_outlier_audit(
    *,
    source_rows: list[dict[str, Any]],
    output_dir: Path,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    count_by_hour: Counter[str] = Counter()
    comparison_size_counts: Counter[int] = Counter()
    coinbase_state_counts: Counter[str] = Counter()
    abs_deviations: list[Decimal] = []
    rel_bps_values: list[Decimal] = []
    weak_comparison_count = 0

    for row in source_rows:
        state = row["executable_state"]
        if state.get("exchange_ineligible_reason_by_venue", {}).get("binance") != "outlier_rejected":
            continue
        decision_ts = parse_utc(row["decision_ts"])
        mids = {
            venue: to_decimal(value)
            for venue, value in (state.get("exchange_mid_price_by_venue") or {}).items()
            if value is not None
        }
        comparison_venues = sorted(
            venue
            for venue, valid in (state.get("exchange_quote_valid_for_composite_by_venue") or {}).items()
            if valid
        )
        comparison_mids = [mids[venue] for venue in comparison_venues if venue in mids]
        reference_price = None
        abs_deviation = None
        rel_bps = None
        if comparison_mids:
            reference_price = Decimal(str(median(comparison_mids)))
            if mids.get("binance") is not None:
                abs_deviation = abs(mids["binance"] - reference_price)
                if reference_price != 0:
                    rel_bps = (abs_deviation / reference_price) * Decimal("10000")
        coinbase_reason = (state.get("exchange_ineligible_reason_by_venue") or {}).get("coinbase")
        coinbase_eligible = bool((state.get("exchange_eligible_by_venue") or {}).get("coinbase"))
        coinbase_state = "eligible" if coinbase_eligible else coinbase_reason or "ineligible_unknown"
        if len(comparison_venues) <= 2:
            weak_comparison_count += 1
        if abs_deviation is not None:
            abs_deviations.append(abs_deviation)
        if rel_bps is not None:
            rel_bps_values.append(rel_bps)

        hour_bucket = decision_ts.strftime("%Y-%m-%d %H:00")
        count_by_hour[hour_bucket] += 1
        comparison_size_counts[len(comparison_venues)] += 1
        coinbase_state_counts[coinbase_state] += 1
        rows.append(
            {
                "decision_ts": decision_ts,
                "hour_bucket": hour_bucket,
                "window_id": state.get("window_id"),
                "polymarket_market_id": state.get("polymarket_market_id"),
                "binance_mid": mids.get("binance"),
                "kraken_mid": mids.get("kraken"),
                "coinbase_mid": mids.get("coinbase"),
                "comparison_set_size": len(comparison_venues),
                "comparison_venues": comparison_venues,
                "reference_price_used": reference_price,
                "absolute_deviation_usd": abs_deviation,
                "relative_deviation_bps": rel_bps,
                "coinbase_eligible": coinbase_eligible,
                "coinbase_eligibility_state": coinbase_state,
                "coinbase_ineligible_reason": coinbase_reason,
                "trusted_venue_count": state.get("exchange_trusted_venue_count"),
                "rejected_venue_count": state.get("exchange_rejected_venue_count"),
            }
        )

    rows.sort(key=lambda item: item["decision_ts"])
    abs_deviations_sorted = sorted(abs_deviations)
    rel_bps_sorted = sorted(rel_bps_values)

    def percentile(values: list[Decimal], q: float) -> Decimal | None:
        if not values:
            return None
        idx = min(len(values) - 1, max(0, int(q * (len(values) - 1))))
        return values[idx]

    summary = {
        "session_id": "20260327T093850581Z",
        "shadow_decision_count": len(source_rows),
        "binance_outlier_row_count": len(rows),
        "comparison_set_size_counts": dict(sorted(comparison_size_counts.items())),
        "coinbase_eligibility_state_counts": dict(sorted(coinbase_state_counts.items())),
        "weak_comparison_rate": pct(weak_comparison_count, len(rows)),
        "absolute_deviation_usd": {
            "p50": percentile(abs_deviations_sorted, 0.50),
            "p90": percentile(abs_deviations_sorted, 0.90),
            "max": (abs_deviations_sorted[-1] if abs_deviations_sorted else None),
        },
        "relative_deviation_bps": {
            "p50": percentile(rel_bps_sorted, 0.50),
            "p90": percentile(rel_bps_sorted, 0.90),
            "max": (rel_bps_sorted[-1] if rel_bps_sorted else None),
        },
        "top_hours": [
            {"hour_bucket": hour, "binance_outlier_rows": count}
            for hour, count in count_by_hour.most_common(8)
        ],
    }

    report_lines = [
        "# Day 4 Binance Outlier Audit",
        "",
        f"- session: `20260327T093850581Z`",
        f"- shadow decision rows inspected: `{len(source_rows)}`",
        f"- Binance `outlier_rejected` rows: `{len(rows)}`",
        f"- weak comparison-set rate (`<=2` contributing venues): `{pct(weak_comparison_count, len(rows)):.4%}`" if rows else "- weak comparison-set rate: `n/a`",
        "",
        "## Comparison Set",
        "",
    ]
    for size, count in sorted(comparison_size_counts.items()):
        report_lines.append(f"- size `{size}`: `{count}`")
    report_lines += [
        "",
        "## Coinbase State At The Same Timestamp",
        "",
    ]
    for state, count in sorted(coinbase_state_counts.items()):
        report_lines.append(f"- `{state}`: `{count}`")
    report_lines += [
        "",
        "## Binance Deviation",
        "",
        f"- absolute deviation p50: `{summary['absolute_deviation_usd']['p50']}` USD",
        f"- absolute deviation p90: `{summary['absolute_deviation_usd']['p90']}` USD",
        f"- relative deviation p50: `{summary['relative_deviation_bps']['p50']}` bps",
        f"- relative deviation p90: `{summary['relative_deviation_bps']['p90']}` bps",
        "",
        "## Top Hours",
        "",
    ]
    for entry in summary["top_hours"]:
        report_lines.append(
            f"- `{entry['hour_bucket']} UTC`: `{entry['binance_outlier_rows']}` Binance outlier rows"
        )
    report_lines += [
        "",
        "## Read",
        "",
        (
            "- Binance is usually being judged against a full 3-venue comparison set."
            if pct(weak_comparison_count, len(rows)) is not None
            and pct(weak_comparison_count, len(rows)) < 0.10
            else "- Binance is often being judged against a weak comparison set."
        ),
        "- The main question is whether those deviations are genuinely large enough to justify rejection.",
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    dump_jsonl(output_dir / "binance_outlier_rows.jsonl", rows)
    dump_json(output_dir / "summary.json", summary)
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def build_execution_gap_pass2(
    *,
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
    edge_bucket_counts: dict[str, Counter[str]] = defaultdict(Counter)
    fill_deltas: list[Decimal] = []
    top_book_slippages: list[Decimal] = []

    def edge_bucket(edge: Decimal | None) -> str:
        if edge is None:
            return "missing"
        if edge < Decimal("0.01"):
            return "<0.01"
        if edge < Decimal("0.05"):
            return "0.01-0.05"
        if edge < Decimal("0.10"):
            return "0.05-0.10"
        return "0.10+"

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

            shadow_state = shadow.raw["executable_state"]
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
            if shadow_trade and shadow.intended_entry_price is not None and shadow.top_of_book_price is not None:
                top_book_slippages.append(shadow.intended_entry_price - shadow.top_of_book_price)

            hour_bucket = shadow.hour_bucket
            volatility_regime = shadow.volatility_regime or "unknown"
            trust = shadow.trusted_venues
            cal_edge = to_decimal(replay.get("calibrated_selected_net_edge"))
            bucket = edge_bucket(cal_edge)

            status = "actionable" if shadow_trade else "not_actionable"
            if calibrated_trade:
                hour_counts[hour_bucket][status] += 1
                regime_counts[volatility_regime][status] += 1
                trust_counts[trust][status] += 1
                edge_bucket_counts[bucket][status] += 1

            joined_rows.append(
                {
                    "decision_ts": shadow.decision_ts,
                    "snapshot_ts": replay["snapshot_ts"],
                    "window_id": replay["window_id"],
                    "polymarket_market_id": replay["polymarket_market_id"],
                    "hour_bucket": hour_bucket,
                    "volatility_regime": volatility_regime,
                    "window_quality_regime": shadow.window_quality_regime,
                    "trusted_venues": trust,
                    "calibrated_trade_direction": replay["calibrated_trade_direction"],
                    "calibrated_preferred_side": replay["calibrated_preferred_side"],
                    "calibrated_selected_net_edge": cal_edge,
                    "calibrated_expected_fill_price": expected_fill,
                    "shadow_actionable": shadow_trade,
                    "shadow_intended_side": shadow.intended_side,
                    "shadow_intended_entry_price": shadow.intended_entry_price,
                    "shadow_top_of_book_price": shadow.top_of_book_price,
                    "shadow_entry_slippage_vs_top_of_book": (
                        None
                        if shadow.intended_entry_price is None or shadow.top_of_book_price is None
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
        "session_id": "20260327T093850581Z",
        "replay_row_count": sum(1 for _ in replay_rows_path.open(encoding="utf-8")),
        "joined_row_count": len(joined_rows),
        "calibrated_trade_row_count": calibrated_trade_rows,
        "joined_calibrated_trade_row_count": joined_trade_rows,
        "shadow_actionable_on_joined_calibrated_rows": actionable_joined_trade_rows,
        "shadow_actionable_rate_on_joined_calibrated_rows": pct(actionable_joined_trade_rows, joined_trade_rows),
        "joined_calibrated_rows_with_3_trusted_venues": joined_with_three_trusted,
        "shadow_actionable_rate_given_3_trusted_venues": pct(actionable_with_three_trusted, joined_with_three_trusted),
        "side_match_count": side_match_count,
        "side_match_rate_on_actionable_joined_calibrated_rows": pct(side_match_count, actionable_joined_trade_rows),
        "mean_shadow_minus_replay_fill": (
            str(sum(fill_deltas, Decimal("0")) / len(fill_deltas)) if fill_deltas else None
        ),
        "mean_entry_slippage_vs_top_of_book": (
            str(sum(top_book_slippages, Decimal("0")) / len(top_book_slippages))
            if top_book_slippages
            else None
        ),
        "hourly_actionability": {
            hour: {
                "joined_calibrated_trade_rows": counts["actionable"] + counts["not_actionable"],
                "shadow_actionable_rows": counts["actionable"],
                "shadow_actionable_rate": pct(
                    counts["actionable"],
                    counts["actionable"] + counts["not_actionable"],
                ),
            }
            for hour, counts in sorted(hour_counts.items())
        },
        "volatility_regime_actionability": {
            regime: {
                "joined_calibrated_trade_rows": counts["actionable"] + counts["not_actionable"],
                "shadow_actionable_rows": counts["actionable"],
                "shadow_actionable_rate": pct(
                    counts["actionable"],
                    counts["actionable"] + counts["not_actionable"],
                ),
            }
            for regime, counts in sorted(regime_counts.items())
        },
        "trusted_venue_actionability": {
            str(trust): {
                "joined_calibrated_trade_rows": counts["actionable"] + counts["not_actionable"],
                "shadow_actionable_rows": counts["actionable"],
                "shadow_actionable_rate": pct(
                    counts["actionable"],
                    counts["actionable"] + counts["not_actionable"],
                ),
            }
            for trust, counts in sorted(trust_counts.items())
        },
        "edge_bucket_actionability": {
            bucket: {
                "joined_calibrated_trade_rows": counts["actionable"] + counts["not_actionable"],
                "shadow_actionable_rows": counts["actionable"],
                "shadow_actionable_rate": pct(
                    counts["actionable"],
                    counts["actionable"] + counts["not_actionable"],
                ),
            }
            for bucket, counts in sorted(edge_bucket_counts.items())
        },
    }

    top_hours = sorted(
        summary["hourly_actionability"].items(),
        key=lambda item: (
            item[1]["shadow_actionable_rate"] or 0,
            item[1]["shadow_actionable_rows"],
        ),
        reverse=True,
    )[:8]

    report_lines = [
        "# Day 4 Execution Gap Pass 2",
        "",
        f"- session: `20260327T093850581Z`",
        f"- replay rows: `{summary['replay_row_count']}`",
        f"- joined rows: `{summary['joined_row_count']}`",
        f"- calibrated trade rows: `{summary['calibrated_trade_row_count']}`",
        f"- joined calibrated trade rows: `{summary['joined_calibrated_trade_row_count']}`",
        f"- shadow actionable on joined calibrated trade rows: `{summary['shadow_actionable_on_joined_calibrated_rows']}`",
        (
            f"- shadow actionable rate on joined calibrated rows: "
            f"`{summary['shadow_actionable_rate_on_joined_calibrated_rows']:.4%}`"
            if summary["shadow_actionable_rate_on_joined_calibrated_rows"] is not None
            else "- shadow actionable rate on joined calibrated rows: `n/a`"
        ),
        (
            f"- shadow actionable rate given `3` trusted venues: "
            f"`{summary['shadow_actionable_rate_given_3_trusted_venues']:.4%}`"
            if summary["shadow_actionable_rate_given_3_trusted_venues"] is not None
            else "- shadow actionable rate given `3` trusted venues: `n/a`"
        ),
        "",
        "## Fill Comparison",
        "",
        f"- mean `shadow intended fill - replay expected fill`: `{summary['mean_shadow_minus_replay_fill']}`",
        f"- mean `entry slippage vs top-of-book`: `{summary['mean_entry_slippage_vs_top_of_book']}`",
        "",
        "## Best Hours",
        "",
    ]
    for hour, payload in top_hours:
        report_lines.append(
            f"- `{hour} UTC`: `{payload['shadow_actionable_rows']}` actionable on "
            f"`{payload['joined_calibrated_trade_rows']}` joined calibrated trade rows "
            f"(`{payload['shadow_actionable_rate']:.4%}`)"
        )
    report_lines += [
        "",
        "## Read",
        "",
        "- This tells you whether calibrated replay trades were actually tradable at decision time.",
        "- The main comparison is not raw trade count; it is actionability conditional on calibrated replay wanting the trade.",
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    dump_jsonl(output_dir / "joined_rows.jsonl", joined_rows)
    dump_json(output_dir / "summary.json", summary)
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    shadow_rows, source_rows = build_shadow_rows(args.shadow_decisions)
    build_binance_outlier_audit(
        source_rows=source_rows,
        output_dir=args.binance_outlier_output_dir,
    )
    build_execution_gap_pass2(
        shadow_rows=shadow_rows,
        replay_rows_path=args.replay_rows,
        output_dir=args.pass2_output_dir,
    )


if __name__ == "__main__":
    main()
