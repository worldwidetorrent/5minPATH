#!/usr/bin/env python3
"""Analyze calibrated-edge survival against shadow execution decisions.

This script normalizes shadow PnL to one contract so it is directly comparable
to calibrated replay rows, which are simulated at one contract per trade.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--replay-rows", type=Path, required=True)
    parser.add_argument("--shadow-decisions", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--label", default=None)
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
    raise TypeError(f"unsupported JSON type: {type(value)!r}")


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


def expected_fill_price(row: dict[str, Any]) -> Decimal | None:
    direction = row["calibrated_trade_direction"]
    if direction == "no_trade":
        return None
    calibrated_f = Decimal(str(row["calibrated_f"]))
    raw_edge = Decimal(str(row["calibrated_selected_raw_edge"]))
    if direction == "buy_up":
        return calibrated_f - raw_edge
    if direction == "buy_down":
        return (Decimal("1") - calibrated_f) - raw_edge
    raise ValueError(f"unsupported trade direction: {direction}")


def infer_resolved_up(*, trade_direction: str, replay_pnl: Decimal) -> bool:
    if trade_direction == "buy_up":
        return replay_pnl > 0
    if trade_direction == "buy_down":
        return replay_pnl <= 0
    raise ValueError(f"unsupported trade direction: {trade_direction}")


def shadow_key_from_replay(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        row["session_id"],
        row["window_id"],
        row["polymarket_market_id"],
        row["snapshot_ts"],
    )


def shadow_key_from_decision(row: dict[str, Any]) -> tuple[str, str, str, str]:
    state = row["executable_state"]
    return (
        state["session_id"],
        state["window_id"],
        state["polymarket_market_id"],
        state["snapshot_ts"],
    )


@dataclass(slots=True)
class ShadowDecisionView:
    decision_ts: datetime
    actionable: bool
    intended_side: str | None
    intended_entry_price: Decimal | None
    no_trade_reason: str | None
    trusted_venues: int
    state_invalid_reason: str | None
    state_diagnostics: list[str]
    raw: dict[str, Any]


def load_shadow_decisions(path: Path) -> dict[tuple[str, str, str, str], ShadowDecisionView]:
    result: dict[tuple[str, str, str, str], ShadowDecisionView] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            diagnostics = row["executable_state"].get("state_diagnostics") or []
            if isinstance(diagnostics, dict):
                diagnostics = [f"{k}={v}" for k, v in sorted(diagnostics.items())]
            result[shadow_key_from_decision(row)] = ShadowDecisionView(
                decision_ts=parse_utc(row["decision_ts"]),
                actionable=bool(row["tradability_check"]["is_actionable"]),
                intended_side=row.get("intended_side"),
                intended_entry_price=to_decimal(row["tradability_check"].get("intended_entry_price")),
                no_trade_reason=row["tradability_check"].get("no_trade_reason"),
                trusted_venues=int(row["executable_state"].get("exchange_trusted_venue_count", 0)),
                state_invalid_reason=row["executable_state"].get("state_invalid_reason"),
                state_diagnostics=list(diagnostics),
                raw=row,
            )
    return result


def load_replay_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row["calibrated_trade_direction"] == "no_trade":
                continue
            rows.append(row)
    return rows


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


def analyze_session(
    *,
    session_id: str,
    replay_rows_path: Path,
    shadow_decisions_path: Path,
    output_dir: Path,
    label: str | None,
) -> dict[str, Any]:
    replay_rows = load_replay_rows(replay_rows_path)
    shadow_rows = load_shadow_decisions(shadow_decisions_path)

    replay_total = Decimal("0")
    joined_trade_count = 0
    shadow_actionable_count = 0
    side_match_count = 0

    replay_total_on_shadow_actionable = Decimal("0")
    replay_total_on_side_matched = Decimal("0")
    shadow_realized_total = Decimal("0")
    shadow_realized_total_side_matched = Decimal("0")
    fill_loss_total = Decimal("0")
    side_mismatch_loss_total = Decimal("0")

    availability_reason_counts: Counter[str] = Counter()
    trusted_venue_counts: Counter[int] = Counter()
    hourly_availability_loss: Counter[str] = Counter()

    output_rows: list[dict[str, Any]] = []

    for replay_row in replay_rows:
        replay_pnl = Decimal(str(replay_row["calibrated_pnl"]))
        replay_total += replay_pnl
        key = shadow_key_from_replay(replay_row)
        shadow = shadow_rows.get(key)
        expected_fill = expected_fill_price(replay_row)
        trade_direction = replay_row["calibrated_trade_direction"]
        preferred_side = "up" if trade_direction == "buy_up" else "down"

        enriched: dict[str, Any] = {
            "session_id": session_id,
            "snapshot_ts": replay_row["snapshot_ts"],
            "window_id": replay_row["window_id"],
            "polymarket_market_id": replay_row["polymarket_market_id"],
            "hour_bucket": parse_utc(replay_row["snapshot_ts"]).strftime("%Y-%m-%d %H:00"),
            "calibrated_trade_direction": trade_direction,
            "calibrated_preferred_side": preferred_side,
            "calibrated_replay_pnl_per_contract": replay_pnl,
            "calibrated_expected_fill_price": expected_fill,
            "shadow_joined": shadow is not None,
        }

        if shadow is None:
            availability_reason_counts["missing_shadow_row"] += 1
            hourly_availability_loss[enriched["hour_bucket"]] += 1
            enriched["gap_class"] = "availability"
            output_rows.append(enriched)
            continue

        joined_trade_count += 1
        trusted_venue_counts[shadow.trusted_venues] += 1
        enriched["shadow_actionable"] = shadow.actionable
        enriched["shadow_trusted_venues"] = shadow.trusted_venues
        enriched["shadow_no_trade_reason"] = shadow.no_trade_reason
        enriched["shadow_state_invalid_reason"] = shadow.state_invalid_reason
        enriched["shadow_intended_side"] = shadow.intended_side
        enriched["shadow_intended_entry_price"] = shadow.intended_entry_price

        if not shadow.actionable:
            availability_reason_counts[shadow.no_trade_reason or "shadow_not_actionable"] += 1
            hourly_availability_loss[enriched["hour_bucket"]] += 1
            enriched["gap_class"] = "availability"
            output_rows.append(enriched)
            continue

        shadow_actionable_count += 1
        replay_total_on_shadow_actionable += replay_pnl
        resolved_up = infer_resolved_up(trade_direction=trade_direction, replay_pnl=replay_pnl)
        if shadow.intended_side is None or shadow.intended_entry_price is None:
            availability_reason_counts["actionable_missing_intended_fill"] += 1
            hourly_availability_loss[enriched["hour_bucket"]] += 1
            enriched["gap_class"] = "availability"
            output_rows.append(enriched)
            continue

        shadow_pnl = shadow_realized_pnl_per_contract(
            side=shadow.intended_side,
            entry_price=shadow.intended_entry_price,
            resolved_up=resolved_up,
        )
        shadow_realized_total += shadow_pnl
        enriched["shadow_realized_pnl_per_contract"] = shadow_pnl

        side_match = shadow.intended_side == preferred_side
        enriched["side_match"] = side_match
        if side_match:
            side_match_count += 1
            replay_total_on_side_matched += replay_pnl
            shadow_realized_total_side_matched += shadow_pnl
            fill_loss = replay_pnl - shadow_pnl
            fill_loss_total += fill_loss
            enriched["gap_class"] = "fill"
            enriched["fill_loss_per_contract"] = fill_loss
        else:
            side_mismatch_loss = replay_pnl - shadow_pnl
            side_mismatch_loss_total += side_mismatch_loss
            enriched["gap_class"] = "side_mismatch"
            enriched["side_mismatch_loss_per_contract"] = side_mismatch_loss

        output_rows.append(enriched)

    total_gap = replay_total - shadow_realized_total
    availability_loss_total = replay_total - replay_total_on_shadow_actionable
    residual_outcome_loss_total = (
        total_gap
        - availability_loss_total
        - fill_loss_total
        - side_mismatch_loss_total
    )

    summary = {
        "session_id": session_id,
        "label": label or session_id,
        "normalization_note": (
            "shadow realized pnl is normalized to one contract to match "
            "calibrated replay rows"
        ),
        "calibrated_trade_row_count": len(replay_rows),
        "joined_calibrated_trade_row_count": joined_trade_count,
        "shadow_actionable_joined_trade_count": shadow_actionable_count,
        "shadow_side_match_count": side_match_count,
        "replay_expected_pnl_total_per_contract": replay_total,
        "shadow_realized_pnl_total_per_contract": shadow_realized_total,
        "edge_survival_ratio": (
            None if replay_total == 0 else shadow_realized_total / replay_total
        ),
        "availability_loss_per_contract": availability_loss_total,
        "fill_loss_per_contract": fill_loss_total,
        "side_mismatch_loss_per_contract": side_mismatch_loss_total,
        "residual_outcome_loss_per_contract": residual_outcome_loss_total,
        "availability_reason_counts": dict(availability_reason_counts),
        "trusted_venue_distribution_on_joined_trade_rows": dict(trusted_venue_counts),
        "joined_trade_rate": pct(joined_trade_count, len(replay_rows)),
        "shadow_actionable_rate_on_calibrated_trade_rows": pct(
            shadow_actionable_count,
            len(replay_rows),
        ),
        "side_match_rate_on_shadow_actionable_rows": pct(side_match_count, shadow_actionable_count),
        "hourly_availability_loss_counts": dict(hourly_availability_loss),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    dump_json(output_dir / "summary.json", summary)
    dump_jsonl(output_dir / "joined_trade_rows.jsonl", output_rows)
    report = _render_report(summary)
    (output_dir / "report.md").write_text(report, encoding="utf-8")
    return summary


def _render_report(summary: dict[str, Any]) -> str:
    return f"""# Edge Survival Report

Session: `{summary['session_id']}`
Label: `{summary['label']}`

## Headline
- calibrated trade rows: `{summary['calibrated_trade_row_count']}`
- joined calibrated trade rows: `{summary['joined_calibrated_trade_row_count']}`
- shadow actionable joined trade rows: `{summary['shadow_actionable_joined_trade_count']}`
- replay expected pnl per contract: `{summary['replay_expected_pnl_total_per_contract']}`
- shadow realized pnl per contract: `{summary['shadow_realized_pnl_total_per_contract']}`
- edge survival ratio: `{summary['edge_survival_ratio']}`

## Gap decomposition
- availability loss per contract: `{summary['availability_loss_per_contract']}`
- fill loss per contract: `{summary['fill_loss_per_contract']}`
- side mismatch loss per contract: `{summary['side_mismatch_loss_per_contract']}`
- residual outcome loss per contract: `{summary['residual_outcome_loss_per_contract']}`

## Supporting rates
- joined trade rate: `{summary['joined_trade_rate']}`
- shadow actionable rate on calibrated trade rows:
  `{summary['shadow_actionable_rate_on_calibrated_trade_rows']}`
- side-match rate on shadow actionable rows:
  `{summary['side_match_rate_on_shadow_actionable_rows']}`

## Availability reasons
{json.dumps(summary['availability_reason_counts'], indent=2)}
"""


def main() -> int:
    args = parse_args()
    analyze_session(
        session_id=args.session_id,
        replay_rows_path=args.replay_rows,
        shadow_decisions_path=args.shadow_decisions,
        output_dir=args.output_dir,
        label=args.label,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
