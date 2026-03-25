"""Minimal in-memory ledger for execution-v0 shadow evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rtds.core.time import ensure_utc
from rtds.execution.enums import OrderState, PolicyMode
from rtds.execution.models import ShadowDecision, ShadowSummary

LEDGER_STATE_SEEN = "decision_seen"
LEDGER_STATE_WRITTEN = "decision_written"


@dataclass(slots=True, frozen=True)
class LedgerEvent:
    """Minimal internal ledger event for one shadow decision."""

    decision_id: str
    session_id: str
    policy_mode: PolicyMode
    order_state: OrderState
    ledger_state: str
    event_ts: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        object.__setattr__(self, "order_state", OrderState(self.order_state))
        object.__setattr__(self, "event_ts", ensure_utc(self.event_ts, field_name="event_ts"))
        normalized_state = str(self.ledger_state).strip().lower()
        if normalized_state not in {LEDGER_STATE_SEEN, LEDGER_STATE_WRITTEN}:
            raise ValueError(f"unsupported ledger_state: {self.ledger_state}")
        object.__setattr__(self, "ledger_state", normalized_state)


class ShadowLedger:
    """Track minimal decision-seen and decision-written transitions."""

    def __init__(self, *, session_id: str, policy_mode: PolicyMode) -> None:
        normalized_session_id = str(session_id).strip()
        if not normalized_session_id:
            raise ValueError("session_id must be non-empty")
        self._session_id = normalized_session_id
        self._policy_mode = PolicyMode(policy_mode)
        self._events: list[LedgerEvent] = []
        self._seen_ids: set[str] = set()
        self._written_ids: set[str] = set()
        self._decisions_by_id: dict[str, ShadowDecision] = {}

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def policy_mode(self) -> PolicyMode:
        return self._policy_mode

    @property
    def events(self) -> tuple[LedgerEvent, ...]:
        return tuple(self._events)

    def record_decision_seen(self, decision: ShadowDecision) -> LedgerEvent:
        """Record that a decision was produced by the decision kernel."""

        return self._record_event(
            decision=decision,
            ledger_state=LEDGER_STATE_SEEN,
        )

    def record_decision_written(self, decision: ShadowDecision) -> LedgerEvent:
        """Record that a decision was durably written to the shadow tree."""

        return self._record_event(
            decision=decision,
            ledger_state=LEDGER_STATE_WRITTEN,
        )

    def build_summary(self) -> ShadowSummary:
        """Build the minimal shadow summary from seen decisions."""

        seen_decisions = [
            event
            for event in self._events
            if event.ledger_state == LEDGER_STATE_SEEN
        ]
        actionable_count = 0
        no_trade_count = 0
        order_state_counts: dict[str, int] = {}
        no_trade_reason_counts: dict[str, int] = {}
        first_ts: datetime | None = None
        last_ts: datetime | None = None
        for event in seen_decisions:
            order_state_counts[event.order_state.value] = (
                order_state_counts.get(event.order_state.value, 0) + 1
            )
            matching_decision = self._decisions_by_id[event.decision_id]
            tradability = matching_decision.tradability_check
            if tradability.is_actionable:
                actionable_count += 1
            else:
                no_trade_count += 1
                if tradability.no_trade_reason is not None:
                    key = tradability.no_trade_reason.value
                    no_trade_reason_counts[key] = no_trade_reason_counts.get(key, 0) + 1
            event_ts = matching_decision.decision_ts
            first_ts = event_ts if first_ts is None or event_ts < first_ts else first_ts
            last_ts = event_ts if last_ts is None or event_ts > last_ts else last_ts
        return ShadowSummary(
            session_id=self._session_id,
            policy_mode=self._policy_mode,
            decision_count=len(seen_decisions),
            actionable_decision_count=actionable_count,
            no_trade_count=no_trade_count,
            order_state_counts=order_state_counts,
            no_trade_reason_counts=no_trade_reason_counts,
            first_decision_ts=first_ts,
            last_decision_ts=last_ts,
        )

    def _record_event(self, *, decision: ShadowDecision, ledger_state: str) -> LedgerEvent:
        if decision.executable_state.session_id != self._session_id:
            raise ValueError("decision session_id does not match ledger session_id")
        if decision.policy_mode != self._policy_mode:
            raise ValueError("decision policy_mode does not match ledger policy_mode")
        if ledger_state == LEDGER_STATE_SEEN:
            if decision.decision_id in self._seen_ids:
                raise ValueError("decision already recorded as seen")
            self._seen_ids.add(decision.decision_id)
            self._decisions_by_id[decision.decision_id] = decision
        if ledger_state == LEDGER_STATE_WRITTEN:
            if decision.decision_id not in self._seen_ids:
                raise ValueError("decision must be seen before written")
            if decision.decision_id in self._written_ids:
                raise ValueError("decision already recorded as written")
            self._written_ids.add(decision.decision_id)
        order_state = (
            OrderState.ELIGIBLE_RECORDED
            if decision.tradability_check.is_actionable
            else OrderState.NO_TRADE_RECORDED
        )
        event = LedgerEvent(
            decision_id=decision.decision_id,
            session_id=self._session_id,
            policy_mode=self._policy_mode,
            order_state=order_state,
            ledger_state=ledger_state,
            event_ts=decision.decision_ts,
        )
        self._events.append(event)
        return event


__all__ = [
    "LEDGER_STATE_SEEN",
    "LEDGER_STATE_WRITTEN",
    "LedgerEvent",
    "ShadowLedger",
]
