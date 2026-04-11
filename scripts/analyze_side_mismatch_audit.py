#!/usr/bin/env python3
"""Audit clean-shadow side mismatch and run offline filter experiments."""

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

VENUES = ("binance", "coinbase", "kraken")


@dataclass(frozen=True)
class DaySpec:
    label: str
    session_id: str
    replay_rows: Path
    shadow_decisions: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day7-replay", type=Path, required=True)
    parser.add_argument("--day7-shadow", type=Path, required=True)
    parser.add_argument("--day8-replay", type=Path, required=True)
    parser.add_argument("--day8-shadow", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported JSON type: {type(value)!r}")


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default) + "\n", encoding="utf-8")


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=json_default) + "\n")


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def key_from_replay(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row["session_id"]),
        str(row["window_id"]),
        str(row["polymarket_market_id"]),
        str(row["snapshot_ts"]),
    )


def key_from_shadow(row: dict[str, Any]) -> tuple[str, str, str, str]:
    state = row["executable_state"]
    return (
        str(state["session_id"]),
        str(state["window_id"]),
        str(state["polymarket_market_id"]),
        str(state["snapshot_ts"]),
    )


def replay_side(row: dict[str, Any]) -> str:
    direction = row["calibrated_trade_direction"]
    if direction == "buy_up":
        return "up"
    if direction == "buy_down":
        return "down"
    raise ValueError(f"unsupported calibrated trade direction: {direction}")


def infer_resolved_up(*, trade_direction: str, replay_pnl: Decimal) -> bool:
    if trade_direction == "buy_up":
        return replay_pnl > 0
    if trade_direction == "buy_down":
        return replay_pnl <= 0
    raise ValueError(f"unsupported trade direction: {trade_direction}")


def shadow_realized_pnl_per_contract(
    *,
    side: str,
    entry_price: Decimal,
    resolved_up: bool,
) -> Decimal:
    if side == "up":
        return (Decimal("1") if resolved_up else Decimal("0")) - entry_price
    if side == "down":
        return (Decimal("0") if resolved_up else Decimal("1")) - entry_price
    raise ValueError(f"unsupported side: {side}")


def load_shadow(path: Path) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            rows[key_from_shadow(row)] = row
    return rows


def load_replay_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row["calibrated_trade_direction"] == "no_trade":
                continue
            rows.append(row)
    return rows


def quantiles(values: list[Decimal]) -> dict[str, Decimal | None]:
    if not values:
        return {"p10": None, "p50": None, "p90": None}
    sorted_values = sorted(values)

    def q(frac: Decimal) -> Decimal:
        scaled_index = Decimal(len(sorted_values) - 1) * frac
        idx = int(scaled_index.to_integral_value(rounding="ROUND_HALF_UP"))
        return sorted_values[idx]

    return {"p10": q(Decimal("0.10")), "p50": q(Decimal("0.50")), "p90": q(Decimal("0.90"))}


def avg(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def edge_bucket(edge: Decimal | None) -> str:
    if edge is None:
        return "missing"
    if edge < Decimal("0.01"):
        return "lt_1c"
    if edge < Decimal("0.025"):
        return "1c_to_2p5c"
    if edge < Decimal("0.05"):
        return "2p5c_to_5c"
    return "gte_5c"


def neutral_distance_bucket(distance: Decimal | None) -> str:
    if distance is None:
        return "missing"
    if distance < Decimal("0.02"):
        return "lt_2c"
    if distance < Decimal("0.05"):
        return "2c_to_5c"
    if distance < Decimal("0.10"):
        return "5c_to_10c"
    return "gte_10c"


def skew_bucket(diagnostics: Any) -> str:
    if isinstance(diagnostics, list):
        for item in diagnostics:
            prefix = "future_event_clock_skew:quote_event_ts:"
            if isinstance(item, str) and item.startswith(prefix) and ":decision_ts=" not in item:
                return item.removeprefix(prefix)
        if "future_event_clock_skew" in diagnostics:
            return "present_unbucketed"
    return "absent"


def venue_context(state: dict[str, Any]) -> dict[str, Any]:
    mids = {
        venue: to_decimal((state.get("exchange_mid_price_by_venue") or {}).get(venue))
        for venue in VENUES
    }
    eligible = state.get("exchange_eligible_by_venue") or {}
    reasons = state.get("exchange_ineligible_reason_by_venue") or {}
    valid_mids = [value for value in mids.values() if value is not None]
    dispersion = (max(valid_mids) - min(valid_mids)) if len(valid_mids) >= 2 else None

    binance_mid = mids.get("binance")
    other_mids = [mids[venue] for venue in ("coinbase", "kraken") if mids.get(venue) is not None]
    binance_abs_dev = None
    binance_rel_dev_bps = None
    binance_near_outlier_threshold = None
    if binance_mid is not None and other_mids:
        ref = median(other_mids)
        binance_abs_dev = abs(binance_mid - ref)
        if ref:
            binance_rel_dev_bps = binance_abs_dev / ref * Decimal("10000")
        # The live quality threshold used in earlier audits was 25 USD / 5 bps.
        binance_near_outlier_threshold = bool(
            binance_abs_dev >= Decimal("20")
            or (binance_rel_dev_bps is not None and binance_rel_dev_bps >= Decimal("4"))
        )

    return {
        "exchange_mid_price_by_venue": mids,
        "exchange_eligible_by_venue": {venue: eligible.get(venue) for venue in VENUES},
        "exchange_ineligible_reason_by_venue": {venue: reasons.get(venue) for venue in VENUES},
        "live_venue_mid_dispersion_abs": dispersion,
        "binance_abs_deviation_vs_coinbase_kraken_median": binance_abs_dev,
        "binance_rel_deviation_bps_vs_coinbase_kraken_median": binance_rel_dev_bps,
        "binance_near_outlier_threshold": binance_near_outlier_threshold,
    }


def build_rows(spec: DaySpec) -> list[dict[str, Any]]:
    shadow_rows = load_shadow(spec.shadow_decisions)
    rows: list[dict[str, Any]] = []
    for replay in load_replay_trades(spec.replay_rows):
        key = key_from_replay(replay)
        shadow = shadow_rows.get(key)
        cal_f = to_decimal(replay.get("calibrated_f"))
        raw_f = to_decimal(replay.get("raw_f"))
        selected_edge = to_decimal(replay.get("calibrated_selected_net_edge"))
        replay_preferred_side = replay_side(replay)
        base_row: dict[str, Any] = {
            "day_label": spec.label,
            "session_id": spec.session_id,
            "snapshot_ts": replay["snapshot_ts"],
            "hour_bucket": parse_utc(replay["snapshot_ts"]).strftime("%Y-%m-%d %H:00"),
            "window_id": replay["window_id"],
            "polymarket_market_id": replay["polymarket_market_id"],
            "calibration_bucket": replay.get("calibration_bucket"),
            "calibrated_f": cal_f,
            "raw_f": raw_f,
            "calibrated_preferred_side": replay_preferred_side,
            "calibrated_selected_net_edge": selected_edge,
            "calibrated_pnl": to_decimal(replay.get("calibrated_pnl")),
            "replay_neutral_distance_abs": None if cal_f is None else abs(cal_f - Decimal("0.5")),
            "raw_neutral_distance_abs": None if raw_f is None else abs(raw_f - Decimal("0.5")),
            "shadow_joined": shadow is not None,
            "shadow_actionable": False,
            "side_match": None,
        }
        if shadow is None:
            rows.append(base_row)
            continue

        state = shadow["executable_state"]
        tradability = shadow["tradability_check"]
        shadow_cal_f = to_decimal(state.get("calibrated_fair_value_base"))
        shadow_raw_f = to_decimal(state.get("fair_value_base"))
        action = bool(tradability.get("is_actionable"))
        side = shadow.get("intended_side")
        side_match = (side == replay_preferred_side) if action else None
        diagnostics = state.get("state_diagnostics") or []
        shadow_entry = to_decimal(tradability.get("intended_entry_price"))
        replay_pnl = to_decimal(replay.get("calibrated_pnl")) or Decimal("0")
        shadow_pnl = None
        if action and side is not None and shadow_entry is not None:
            resolved_up = infer_resolved_up(
                trade_direction=replay["calibrated_trade_direction"],
                replay_pnl=replay_pnl,
            )
            shadow_pnl = shadow_realized_pnl_per_contract(
                side=side,
                entry_price=shadow_entry,
                resolved_up=resolved_up,
            )
        base_row.update(
            {
                "shadow_actionable": action,
                "shadow_intended_side": side,
                "shadow_intended_entry_price": shadow_entry,
                "shadow_realized_pnl_per_contract": shadow_pnl,
                "side_match": side_match,
                "shadow_no_trade_reason": tradability.get("no_trade_reason"),
                "shadow_state_invalid_reason": state.get("state_invalid_reason"),
                "shadow_trusted_venues": state.get("exchange_trusted_venue_count"),
                "shadow_calibrated_fair_value_base": shadow_cal_f,
                "shadow_fair_value_base": shadow_raw_f,
                "shadow_neutral_distance_abs": (
                    None if shadow_cal_f is None else abs(shadow_cal_f - Decimal("0.5"))
                ),
                "calibrated_fair_value_delta_live_minus_replay": (
                    None if shadow_cal_f is None or cal_f is None else shadow_cal_f - cal_f
                ),
                "calibrated_fair_value_abs_delta_live_vs_replay": (
                    None if shadow_cal_f is None or cal_f is None else abs(shadow_cal_f - cal_f)
                ),
                "shadow_selected_net_edge": to_decimal(tradability.get("selected_net_edge")),
                "skew_bucket": skew_bucket(diagnostics),
                "state_diagnostics": diagnostics,
            }
        )
        base_row.update(venue_context(state))
        rows.append(base_row)
    return rows


def split_actionable(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    actionable = [row for row in rows if row["shadow_actionable"]]
    return (
        [row for row in actionable if row["side_match"] is True],
        [row for row in actionable if row["side_match"] is False],
    )


def summarize_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    replay_distances = [
        row["replay_neutral_distance_abs"]
        for row in rows
        if row.get("replay_neutral_distance_abs") is not None
    ]
    selected_edges = [
        row["calibrated_selected_net_edge"]
        for row in rows
        if row.get("calibrated_selected_net_edge") is not None
    ]
    fair_value_deltas = [
        row["calibrated_fair_value_abs_delta_live_vs_replay"]
        for row in rows
        if row.get("calibrated_fair_value_abs_delta_live_vs_replay") is not None
    ]
    venue_dispersions = [
        row["live_venue_mid_dispersion_abs"]
        for row in rows
        if row.get("live_venue_mid_dispersion_abs") is not None
    ]

    def eligibility_pattern(row: dict[str, Any]) -> str:
        eligible = row.get("exchange_eligible_by_venue") or {}
        return ",".join(f"{venue}:{eligible.get(venue)}" for venue in VENUES)

    return {
        "count": len(rows),
        "bucket_counts": dict(Counter(str(row.get("calibration_bucket")) for row in rows)),
        "replay_neutral_distance_abs": quantiles(replay_distances),
        "calibrated_selected_net_edge": quantiles(selected_edges),
        "live_vs_replay_calibrated_f_abs_delta": quantiles(fair_value_deltas),
        "live_venue_mid_dispersion_abs": quantiles(venue_dispersions),
        "edge_bucket_counts": dict(
            Counter(edge_bucket(row.get("calibrated_selected_net_edge")) for row in rows)
        ),
        "neutral_distance_bucket_counts": dict(
            Counter(neutral_distance_bucket(row.get("replay_neutral_distance_abs")) for row in rows)
        ),
        "skew_bucket_counts": dict(Counter(str(row.get("skew_bucket")) for row in rows)),
        "venue_eligibility_pattern_counts": dict(Counter(eligibility_pattern(row) for row in rows)),
        "binance_near_outlier_threshold_count": sum(
            1 for row in rows if row.get("binance_near_outlier_threshold") is True
        ),
    }


def summarize_day(label: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    joined = [row for row in rows if row["shadow_joined"]]
    actionable = [row for row in rows if row["shadow_actionable"]]
    matches, mismatches = split_actionable(rows)
    hour_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        hour = row["hour_bucket"]
        hour_counts[hour]["total_calibrated_trade_rows"] += 1
        if row["shadow_joined"]:
            hour_counts[hour]["joined"] += 1
        if row["shadow_actionable"]:
            hour_counts[hour]["actionable"] += 1
            if row["side_match"]:
                hour_counts[hour]["side_match"] += 1
            else:
                hour_counts[hour]["side_mismatch"] += 1

    hourly = {}
    for hour, counts in sorted(hour_counts.items()):
        hourly[hour] = {
            **dict(counts),
            "joined_rate": safe_ratio(counts["joined"], counts["total_calibrated_trade_rows"]),
            "actionable_rate": safe_ratio(
                counts["actionable"],
                counts["total_calibrated_trade_rows"],
            ),
            "side_match_rate": safe_ratio(counts["side_match"], counts["actionable"]),
        }
    top_mismatch_hours = sorted(
        (
            {
                "hour_bucket": hour,
                "side_mismatch_count": values["side_mismatch"],
                "actionable_count": values["actionable"],
                "side_match_rate": values["side_match_rate"],
            }
            for hour, values in hourly.items()
            if values.get("side_mismatch", 0) > 0
        ),
        key=lambda row: (row["side_mismatch_count"], row["hour_bucket"]),
        reverse=True,
    )[:8]

    return {
        "day_label": label,
        "calibrated_trade_row_count": len(rows),
        "joined_trade_row_count": len(joined),
        "shadow_actionable_row_count": len(actionable),
        "side_match_count": len(matches),
        "side_mismatch_count": len(mismatches),
        "joined_trade_rate": safe_ratio(len(joined), len(rows)),
        "shadow_actionable_rate_on_calibrated_rows": safe_ratio(len(actionable), len(rows)),
        "side_match_rate_on_shadow_actionable_rows": safe_ratio(len(matches), len(actionable)),
        "match_rows": summarize_group(matches),
        "mismatch_rows": summarize_group(mismatches),
        "hourly": hourly,
        "top_mismatch_hours": top_mismatch_hours,
    }


def passes_filter(row: dict[str, Any], *, dead_zone: Decimal, min_edge: Decimal) -> bool:
    distance = row.get("replay_neutral_distance_abs")
    edge = row.get("calibrated_selected_net_edge")
    if distance is None or edge is None:
        return False
    return distance >= dead_zone and edge >= min_edge


def experiment_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    configs = [
        ("current", Decimal("0"), Decimal("0")),
        ("dead_zone_small", Decimal("0.02"), Decimal("0")),
        ("dead_zone_medium", Decimal("0.05"), Decimal("0")),
        ("dead_zone_conservative", Decimal("0.10"), Decimal("0")),
        ("min_edge_modest", Decimal("0"), Decimal("0.01")),
        ("min_edge_strict", Decimal("0"), Decimal("0.025")),
        ("combined_medium", Decimal("0.05"), Decimal("0.01")),
        ("combined_strict", Decimal("0.10"), Decimal("0.025")),
    ]
    results = []
    for label, dead_zone, min_edge in configs:
        kept = [row for row in rows if passes_filter(row, dead_zone=dead_zone, min_edge=min_edge)]
        actionable = [row for row in kept if row["shadow_actionable"]]
        matches = [row for row in actionable if row["side_match"] is True]
        replay_pnl = sum((row.get("calibrated_pnl") or Decimal("0")) for row in kept)
        shadow_pnl = sum(
            (row.get("shadow_realized_pnl_per_contract") or Decimal("0"))
            for row in kept
        )
        results.append(
            {
                "experiment": label,
                "dead_zone_abs_distance": dead_zone,
                "min_selected_net_edge": min_edge,
                "kept_calibrated_trade_rows": len(kept),
                "kept_row_rate": safe_ratio(len(kept), len(rows)),
                "shadow_actionable_rows": len(actionable),
                "shadow_actionable_rate_on_kept_rows": safe_ratio(len(actionable), len(kept)),
                "side_match_rows": len(matches),
                "side_match_rate_on_actionable_rows": safe_ratio(len(matches), len(actionable)),
                "replay_expected_pnl_on_kept_rows": replay_pnl,
                "shadow_realized_pnl_on_kept_rows": shadow_pnl,
                "edge_survival_ratio_on_kept_rows": (
                    None if replay_pnl == 0 else shadow_pnl / replay_pnl
                ),
            }
        )
    return results


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Day 7 vs Day 8 Side-Mismatch Audit",
        "",
        (
            "This is diagnostic only. No policy, calibrator, venue, capture, "
            "or shadow-contract changes were made."
        ),
        "",
        "## Key Limitation",
        (
            "- replay rows do not persist replay composite USD price, so "
            "composite-delta is reported as live-vs-replay calibrated-fair-value delta"
        ),
        "",
        "## Headline",
        "- Day 7 had high side agreement on actionable joined rows",
        "- Day 8 had similar/higher actionability, but materially weaker side agreement",
        (
            "- Day 8 mismatches are not only a near-neutral-boundary phenomenon; "
            "they also appear in high-confidence tails"
        ),
        "",
    ]
    for day in summary["days"]:
        lines.extend(
            [
                f"## {day['day_label']}",
                f"- calibrated trade rows: `{day['calibrated_trade_row_count']}`",
                f"- joined trade rate: `{day['joined_trade_rate']}`",
                (
                    "- shadow actionable rate on calibrated rows: "
                    f"`{day['shadow_actionable_rate_on_calibrated_rows']}`"
                ),
                (
                    "- side-match rate on actionable rows: "
                    f"`{day['side_match_rate_on_shadow_actionable_rows']}`"
                ),
                f"- match bucket counts: `{day['match_rows']['bucket_counts']}`",
                f"- mismatch bucket counts: `{day['mismatch_rows']['bucket_counts']}`",
                (
                    "- match replay-neutral-distance quantiles: "
                    f"`{day['match_rows']['replay_neutral_distance_abs']}`"
                ),
                (
                    "- mismatch replay-neutral-distance quantiles: "
                    f"`{day['mismatch_rows']['replay_neutral_distance_abs']}`"
                ),
                (
                    "- match live-vs-replay fair-value abs-delta quantiles: "
                    f"`{day['match_rows']['live_vs_replay_calibrated_f_abs_delta']}`"
                ),
                (
                    "- mismatch live-vs-replay fair-value abs-delta quantiles: "
                    f"`{day['mismatch_rows']['live_vs_replay_calibrated_f_abs_delta']}`"
                ),
                f"- mismatch skew buckets: `{day['mismatch_rows']['skew_bucket_counts']}`",
                (
                    "- mismatch venue-dispersion quantiles: "
                    f"`{day['mismatch_rows']['live_venue_mid_dispersion_abs']}`"
                ),
                (
                    "- mismatch venue eligibility patterns: "
                    f"`{day['mismatch_rows']['venue_eligibility_pattern_counts']}`"
                ),
                (
                    "- mismatch Binance near-outlier count: "
                    f"`{day['mismatch_rows']['binance_near_outlier_threshold_count']}`"
                ),
                f"- top mismatch hours: `{day['top_mismatch_hours']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Offline Filter Experiments",
            (
                "These are not policy changes. They only ask whether a dead zone "
                "or edge threshold would have improved the Day 7/Day 8 split."
            ),
            "",
        ]
    )
    for label, experiments in summary["offline_experiments"].items():
        lines.append(f"### {label}")
        for row in experiments:
            lines.append(
                "- "
                f"{row['experiment']}: kept `{row['kept_calibrated_trade_rows']}`, "
                f"actionable_rate `{row['shadow_actionable_rate_on_kept_rows']}`, "
                f"side_match_rate `{row['side_match_rate_on_actionable_rows']}`, "
                f"replay_pnl `{row['replay_expected_pnl_on_kept_rows']}`, "
                f"shadow_pnl `{row['shadow_realized_pnl_on_kept_rows']}`, "
                f"survival `{row['edge_survival_ratio_on_kept_rows']}`"
            )
        lines.append("")
    lines.extend(
        [
            "## Bottom Line",
            "- Day 8 weakness is not explained by join rate or raw actionability.",
            "- Day 8 weakness is explained by directional disagreement after actionability.",
            (
                "- Simple boundary/edge filters need to be judged by whether they "
                "improve Day 8 without killing Day 7."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    day_specs = [
        DaySpec("Day 7", "20260401T112554963Z", args.day7_replay, args.day7_shadow),
        DaySpec("Day 8", "20260407T110750965Z", args.day8_replay, args.day8_shadow),
    ]
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    all_rows_by_day = {spec.label: build_rows(spec) for spec in day_specs}
    summary = {
        "comparison_label": "day7_day8_side_mismatch_audit",
        "research_contract": "frozen; diagnostic only",
        "composite_delta_status": (
            "replay composite USD price is not persisted in calibrated replay row artifacts; "
            "live-vs-replay calibrated fair-value delta is used as the available proxy"
        ),
        "days": [summarize_day(label, rows) for label, rows in all_rows_by_day.items()],
        "offline_experiments": {
            label: experiment_summary(rows) for label, rows in all_rows_by_day.items()
        },
    }
    dump_json(output_dir / "summary.json", summary)
    for label, rows in all_rows_by_day.items():
        slug = label.lower().replace(" ", "_")
        dump_jsonl(output_dir / f"{slug}_audit_rows.jsonl", rows)
    (output_dir / "report.md").write_text(render_report(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
