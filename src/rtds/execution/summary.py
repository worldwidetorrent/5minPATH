"""Structured shadow summary metrics for execution-v0 evidence."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from rtds.execution.models import ShadowOutcome, ShadowSummary, ShadowVsReplaySummary


def build_shadow_summary(ledger) -> ShadowSummary:
    """Build the structured shadow summary from a ledger snapshot."""

    decisions = list(ledger.decisions)
    outcomes = list(ledger.outcomes)
    decision_count = len(decisions)
    actionable_count = sum(int(decision.tradability_check.is_actionable) for decision in decisions)
    no_trade_count = decision_count - actionable_count
    order_state_counts = _count_by_attr(ledger.order_states, attr="order_state")
    no_trade_reason_counts = _count_no_trade_reasons(decisions)
    reject_rate_by_reason = {
        reason: _rate(count, decision_count)
        for reason, count in no_trade_reason_counts.items()
    }
    first_ts = None if not decisions else min(decision.decision_ts for decision in decisions)
    last_ts = None if not decisions else max(decision.decision_ts for decision in decisions)
    replay_expected_total, shadow_realized_total, divergence_total = _outcome_totals(outcomes)
    return ShadowSummary(
        session_id=ledger.session_id,
        policy_mode=ledger.policy_mode,
        decision_count=decision_count,
        actionable_decision_count=actionable_count,
        no_trade_count=no_trade_count,
        written_decision_count=ledger.written_decision_count,
        shadow_attach_ts=ledger.shadow_attach_ts,
        processing_mode=ledger.processing_mode,
        backlog_decision_count=ledger.backlog_decision_count,
        live_forward_decision_count=ledger.live_forward_decision_count,
        order_state_transition_count=len(ledger.order_states),
        order_state_counts=order_state_counts,
        no_trade_reason_counts=no_trade_reason_counts,
        reject_rate_by_reason=reject_rate_by_reason,
        tradability_pass_rate=_rate(actionable_count, decision_count),
        freshness_pass_rate=_rate(
            sum(int(decision.tradability_check.freshness_passed) for decision in decisions),
            decision_count,
        ),
        size_coverage_pass_rate=_rate(
            sum(int(decision.tradability_check.size_coverage_passed) for decision in decisions),
            decision_count,
        ),
        spread_pass_rate=_rate(
            sum(int(decision.tradability_check.spread_passed) for decision in decisions),
            decision_count,
        ),
        replay_expected_pnl=replay_expected_total,
        shadow_realized_pnl=shadow_realized_total,
        pnl_divergence_vs_replay=divergence_total,
        first_decision_ts=first_ts,
        last_decision_ts=last_ts,
        max_decision_lag_ms=ledger.max_decision_lag_ms,
    )


def reconcile_shadow_summary_from_artifacts(
    summary: ShadowSummary,
    *,
    shadow_decisions_path: str | Path,
    shadow_order_states_path: str | Path,
) -> ShadowSummary:
    """Rebuild summary counts from durable JSONL artifacts.

    This is intended for final shutdown reconciliation so the summary reflects
    the append-only files on disk even if in-memory counters lag slightly at
    termination.
    """

    decision_stats = _scan_shadow_decisions(Path(shadow_decisions_path))
    order_state_stats = _scan_shadow_order_states(Path(shadow_order_states_path))
    decision_count = decision_stats["decision_count"]
    actionable_count = decision_stats["actionable_decision_count"]
    no_trade_count = decision_count - actionable_count
    no_trade_reason_counts = decision_stats["no_trade_reason_counts"]
    return replace(
        summary,
        decision_count=decision_count,
        actionable_decision_count=actionable_count,
        no_trade_count=no_trade_count,
        written_decision_count=decision_count,
        order_state_transition_count=order_state_stats["order_state_transition_count"],
        order_state_counts=order_state_stats["order_state_counts"],
        no_trade_reason_counts=no_trade_reason_counts,
        reject_rate_by_reason={
            reason: _rate(count, decision_count)
            for reason, count in no_trade_reason_counts.items()
        },
        tradability_pass_rate=_rate(actionable_count, decision_count),
        freshness_pass_rate=_rate(
            decision_stats["freshness_passed_count"],
            decision_count,
        ),
        size_coverage_pass_rate=_rate(
            decision_stats["size_coverage_passed_count"],
            decision_count,
        ),
        spread_pass_rate=_rate(
            decision_stats["spread_passed_count"],
            decision_count,
        ),
        first_decision_ts=decision_stats["first_decision_ts"],
        last_decision_ts=decision_stats["last_decision_ts"],
    )


def build_shadow_vs_replay_summary(
    *,
    ledger,
    outcomes: tuple[ShadowOutcome, ...] | list[ShadowOutcome] | None = None,
) -> ShadowVsReplaySummary:
    """Build a structured execution-gap comparison from decisions plus outcomes."""

    decisions = list(ledger.decisions)
    resolved_outcomes = list(ledger.outcomes if outcomes is None else outcomes)
    no_trade_reason_counts = _count_no_trade_reasons(decisions)
    return ShadowVsReplaySummary(
        session_id=ledger.session_id,
        policy_mode=ledger.policy_mode,
        decision_count=len(decisions),
        actionable_decision_count=sum(
            int(decision.tradability_check.is_actionable) for decision in decisions
        ),
        reconciled_decision_count=len(resolved_outcomes),
        replay_expected_pnl=sum(
            (outcome.replay_expected_pnl or Decimal("0")) for outcome in resolved_outcomes
        ),
        shadow_realized_pnl=sum(
            (outcome.shadow_realized_pnl or Decimal("0")) for outcome in resolved_outcomes
        ),
        pnl_divergence_vs_replay=sum(
            (outcome.pnl_divergence_vs_replay or Decimal("0")) for outcome in resolved_outcomes
        ),
        reject_rate_by_reason={
            reason: _rate(count, len(decisions))
            for reason, count in no_trade_reason_counts.items()
        },
        tradability_pass_rate=_rate(
            sum(int(decision.tradability_check.is_actionable) for decision in decisions),
            len(decisions),
        ),
        freshness_pass_rate=_rate(
            sum(int(decision.tradability_check.freshness_passed) for decision in decisions),
            len(decisions),
        ),
        size_coverage_pass_rate=_rate(
            sum(int(decision.tradability_check.size_coverage_passed) for decision in decisions),
            len(decisions),
        ),
        spread_pass_rate=_rate(
            sum(int(decision.tradability_check.spread_passed) for decision in decisions),
            len(decisions),
        ),
    )


def _count_by_attr(rows, *, attr: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = getattr(row, attr)
        if hasattr(key, "value"):
            key = key.value
        else:
            key = str(key)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _count_no_trade_reasons(decisions) -> dict[str, int]:
    counts: dict[str, int] = {}
    for decision in decisions:
        reason = decision.tradability_check.no_trade_reason
        if reason is None:
            continue
        counts[reason.value] = counts.get(reason.value, 0) + 1
    return counts


def _rate(numerator: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(numerator) / Decimal(denominator)


def _outcome_totals(
    outcomes: list[ShadowOutcome],
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if not outcomes:
        return None, None, None
    replay_expected_total = sum(
        (outcome.replay_expected_pnl or Decimal("0")) for outcome in outcomes
    )
    shadow_realized_total = sum(
        (outcome.shadow_realized_pnl or Decimal("0")) for outcome in outcomes
    )
    divergence_total = sum(
        (outcome.pnl_divergence_vs_replay or Decimal("0")) for outcome in outcomes
    )
    return replay_expected_total, shadow_realized_total, divergence_total


def _scan_shadow_decisions(path: Path) -> dict[str, object]:
    decision_count = 0
    actionable_count = 0
    freshness_passed_count = 0
    size_coverage_passed_count = 0
    spread_passed_count = 0
    no_trade_reason_counts: dict[str, int] = {}
    first_decision_ts: datetime | None = None
    last_decision_ts: datetime | None = None
    if not path.exists():
        return {
            "decision_count": 0,
            "actionable_decision_count": 0,
            "freshness_passed_count": 0,
            "size_coverage_passed_count": 0,
            "spread_passed_count": 0,
            "no_trade_reason_counts": {},
            "first_decision_ts": None,
            "last_decision_ts": None,
        }
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            decision_count += 1
            decision_ts = _parse_decision_ts(row.get("decision_ts"))
            if decision_ts is not None:
                if first_decision_ts is None or decision_ts < first_decision_ts:
                    first_decision_ts = decision_ts
                if last_decision_ts is None or decision_ts > last_decision_ts:
                    last_decision_ts = decision_ts
            tradability = row.get("tradability_check") or {}
            if tradability.get("is_actionable"):
                actionable_count += 1
            else:
                reason = tradability.get("no_trade_reason")
                if reason:
                    no_trade_reason_counts[str(reason)] = (
                        no_trade_reason_counts.get(str(reason), 0) + 1
                    )
            if tradability.get("freshness_passed"):
                freshness_passed_count += 1
            if tradability.get("size_coverage_passed"):
                size_coverage_passed_count += 1
            if tradability.get("spread_passed"):
                spread_passed_count += 1
    return {
        "decision_count": decision_count,
        "actionable_decision_count": actionable_count,
        "freshness_passed_count": freshness_passed_count,
        "size_coverage_passed_count": size_coverage_passed_count,
        "spread_passed_count": spread_passed_count,
        "no_trade_reason_counts": no_trade_reason_counts,
        "first_decision_ts": first_decision_ts,
        "last_decision_ts": last_decision_ts,
    }


def _scan_shadow_order_states(path: Path) -> dict[str, object]:
    counts: dict[str, int] = {}
    transition_count = 0
    if not path.exists():
        return {"order_state_transition_count": 0, "order_state_counts": {}}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            transition_count += 1
            order_state = row.get("order_state")
            if order_state:
                key = str(order_state)
                counts[key] = counts.get(key, 0) + 1
    return {
        "order_state_transition_count": transition_count,
        "order_state_counts": counts,
    }


def _parse_decision_ts(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    return datetime.fromisoformat(normalized)


__all__ = [
    "build_shadow_summary",
    "build_shadow_vs_replay_summary",
    "reconcile_shadow_summary_from_artifacts",
]
