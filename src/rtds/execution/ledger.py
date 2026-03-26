"""In-memory execution-v0 ledger for shadow evidence and state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rtds.core.time import ensure_utc
from rtds.execution.enums import OrderState, PolicyMode
from rtds.execution.models import ShadowDecision, ShadowOrderState, ShadowOutcome, ShadowSummary

LEDGER_STATE_SEEN = "decision_seen"
LEDGER_STATE_WRITTEN = "decision_written"
LEDGER_STATE_RECONCILED = "decision_reconciled"


@dataclass(slots=True, frozen=True)
class LedgerEvent:
    """Internal ledger event for one shadow decision transition."""

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
        if normalized_state not in {
            LEDGER_STATE_SEEN,
            LEDGER_STATE_WRITTEN,
            LEDGER_STATE_RECONCILED,
        }:
            raise ValueError(f"unsupported ledger_state: {self.ledger_state}")
        object.__setattr__(self, "ledger_state", normalized_state)


class ShadowLedger:
    """Track shadow decisions, state transitions, and optional reconciled outcomes."""

    def __init__(self, *, session_id: str, policy_mode: PolicyMode) -> None:
        normalized_session_id = str(session_id).strip()
        if not normalized_session_id:
            raise ValueError("session_id must be non-empty")
        self._session_id = normalized_session_id
        self._policy_mode = PolicyMode(policy_mode)
        self._events: list[LedgerEvent] = []
        self._order_states: list[ShadowOrderState] = []
        self._outcomes: list[ShadowOutcome] = []
        self._seen_ids: set[str] = set()
        self._written_ids: set[str] = set()
        self._reconciled_ids: set[str] = set()
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

    @property
    def decisions(self) -> tuple[ShadowDecision, ...]:
        return tuple(
            self._decisions_by_id[decision_id]
            for decision_id in sorted(self._decisions_by_id)
        )

    @property
    def order_states(self) -> tuple[ShadowOrderState, ...]:
        return tuple(self._order_states)

    @property
    def outcomes(self) -> tuple[ShadowOutcome, ...]:
        return tuple(self._outcomes)

    @property
    def seen_decision_count(self) -> int:
        return len(self._seen_ids)

    @property
    def written_decision_count(self) -> int:
        return len(self._written_ids)

    @property
    def reconciled_decision_count(self) -> int:
        return len(self._reconciled_ids)

    def record_decision_seen(self, decision: ShadowDecision) -> ShadowOrderState:
        """Record that a decision was produced by the decision kernel."""

        return self._record_transition(
            decision=decision,
            ledger_state=LEDGER_STATE_SEEN,
        )

    def record_decision_written(self, decision: ShadowDecision) -> ShadowOrderState:
        """Record that a decision was durably written to the shadow tree."""

        return self._record_transition(
            decision=decision,
            ledger_state=LEDGER_STATE_WRITTEN,
        )

    def record_outcome(self, outcome: ShadowOutcome) -> ShadowOrderState:
        """Record one reconciled shadow outcome and its state transition."""

        decision = outcome.decision
        if decision.executable_state.session_id != self._session_id:
            raise ValueError("outcome session_id does not match ledger session_id")
        if decision.policy_mode != self._policy_mode:
            raise ValueError("outcome policy_mode does not match ledger policy_mode")
        if decision.decision_id not in self._seen_ids:
            raise ValueError("decision must be seen before reconciliation")
        if decision.decision_id in self._reconciled_ids:
            raise ValueError("decision already reconciled")
        self._reconciled_ids.add(decision.decision_id)
        self._outcomes.append(outcome)
        return self._record_transition(
            decision=decision,
            ledger_state=LEDGER_STATE_RECONCILED,
            event_ts=outcome.outcome_ts,
            order_state=outcome.order_state,
        )

    def build_summary(self) -> ShadowSummary:
        """Build the structured summary from current ledger contents."""

        from rtds.execution.summary import build_shadow_summary

        return build_shadow_summary(self)

    def _record_transition(
        self,
        *,
        decision: ShadowDecision,
        ledger_state: str,
        event_ts: datetime | None = None,
        order_state: OrderState | None = None,
    ) -> ShadowOrderState:
        if decision.executable_state.session_id != self._session_id:
            raise ValueError("decision session_id does not match ledger session_id")
        if decision.policy_mode != self._policy_mode:
            raise ValueError("decision policy_mode does not match ledger policy_mode")
        if ledger_state == LEDGER_STATE_SEEN:
            if decision.decision_id in self._seen_ids:
                raise ValueError("decision already recorded as seen")
            self._seen_ids.add(decision.decision_id)
            self._decisions_by_id[decision.decision_id] = decision
        elif ledger_state == LEDGER_STATE_WRITTEN:
            if decision.decision_id not in self._seen_ids:
                raise ValueError("decision must be seen before written")
            if decision.decision_id in self._written_ids:
                raise ValueError("decision already recorded as written")
            self._written_ids.add(decision.decision_id)
        elif ledger_state == LEDGER_STATE_RECONCILED:
            if decision.decision_id not in self._seen_ids:
                raise ValueError("decision must be seen before reconciled")
        else:
            raise ValueError(f"unsupported ledger_state: {ledger_state}")
        resolved_order_state = order_state or self._derive_order_state(decision)
        resolved_event_ts = decision.decision_ts if event_ts is None else event_ts
        event = LedgerEvent(
            decision_id=decision.decision_id,
            session_id=self._session_id,
            policy_mode=self._policy_mode,
            order_state=resolved_order_state,
            ledger_state=ledger_state,
            event_ts=resolved_event_ts,
        )
        self._events.append(event)
        order_state_row = ShadowOrderState(
            decision=decision,
            order_state=resolved_order_state,
            updated_ts=resolved_event_ts,
            transition_name=ledger_state,
            transition_index=len(self._order_states),
        )
        self._order_states.append(order_state_row)
        return order_state_row

    @staticmethod
    def _derive_order_state(decision: ShadowDecision) -> OrderState:
        if decision.tradability_check.is_actionable:
            return OrderState.ELIGIBLE_RECORDED
        return OrderState.NO_TRADE_RECORDED


__all__ = [
    "LEDGER_STATE_RECONCILED",
    "LEDGER_STATE_SEEN",
    "LEDGER_STATE_WRITTEN",
    "LedgerEvent",
    "ShadowLedger",
]
