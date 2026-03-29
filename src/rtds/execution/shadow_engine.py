"""Minimal fail-open shadow runtime for execution v0."""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Deque

from rtds.execution.adapters import (
    ExecutionStateAdapter,
    assert_live_state_adapter,
)
from rtds.execution.enums import PolicyMode
from rtds.execution.ledger import ShadowLedger
from rtds.execution.models import PROCESSING_MODE_LIVE_ONLY_FROM_ATTACH_TS
from rtds.execution.policy_adapter import (
    PolicyDecision,
    PolicyEvaluationInput,
    evaluate_policy_decision,
)
from rtds.execution.sizing import SizingPolicy
from rtds.execution.summary import reconcile_shadow_summary_from_artifacts
from rtds.execution.writer import ShadowArtifactWriter

LOGGER = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
DEFAULT_IDLE_SLEEP_SECONDS = 0.25
DEFAULT_RECENT_DECISION_BUFFER_SIZE = 256


@dataclass(slots=True, frozen=True)
class ShadowEngineConfig:
    """Minimal runtime config for the fail-open shadow engine."""

    session_id: str
    policy_name: str
    policy_role: str
    policy_mode: PolicyMode
    sizing_policy: SizingPolicy
    min_net_edge: Decimal
    max_quote_age_ms: int | None = None
    max_spread_abs: Decimal | None = None
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    idle_sleep_seconds: float = DEFAULT_IDLE_SLEEP_SECONDS
    recent_decision_buffer_size: int = DEFAULT_RECENT_DECISION_BUFFER_SIZE
    shadow_root_dir: str = "artifacts/shadow"
    processing_mode: str = PROCESSING_MODE_LIVE_ONLY_FROM_ATTACH_TS
    shadow_attach_ts: datetime | None = None

    def __post_init__(self) -> None:
        if not str(self.session_id).strip():
            raise ValueError("session_id must be non-empty")
        if self.heartbeat_interval_seconds < 0:
            raise ValueError("heartbeat_interval_seconds must be non-negative")
        if self.idle_sleep_seconds < 0:
            raise ValueError("idle_sleep_seconds must be non-negative")
        if self.recent_decision_buffer_size <= 0:
            raise ValueError("recent_decision_buffer_size must be positive")
        normalized_processing_mode = str(self.processing_mode).strip().lower()
        if normalized_processing_mode != PROCESSING_MODE_LIVE_ONLY_FROM_ATTACH_TS:
            raise ValueError(f"unsupported processing_mode: {self.processing_mode}")
        object.__setattr__(self, "processing_mode", normalized_processing_mode)


@dataclass(slots=True)
class ShadowEngineStats:
    """Live runtime stats for heartbeat and shutdown reporting."""

    read_count: int = 0
    decision_count: int = 0
    actionable_count: int = 0
    no_trade_count: int = 0
    error_count: int = 0
    last_state_ts: object | None = None
    last_decision_id: str | None = None
    last_no_trade_reason: str | None = None


@dataclass(slots=True)
class ShadowEngine:
    """Thin orchestration loop for execution-v0 shadow decisions."""

    adapter: ExecutionStateAdapter
    config: ShadowEngineConfig
    writer: ShadowArtifactWriter = field(init=False)
    ledger: ShadowLedger = field(init=False)
    stats: ShadowEngineStats = field(init=False)
    _recent_decision_ids: Deque[str] = field(init=False, repr=False)
    _stop_requested: bool = field(default=False, init=False, repr=False)
    _last_heartbeat_monotonic: float = field(default_factory=time.monotonic, init=False, repr=False)

    def __post_init__(self) -> None:
        attach_ts = (
            datetime.now(UTC)
            if self.config.shadow_attach_ts is None
            else self.config.shadow_attach_ts
        )
        assert_live_state_adapter(self.adapter.descriptor)
        self.writer = ShadowArtifactWriter(
            session_id=self.config.session_id,
            root_dir=self.config.shadow_root_dir,
        )
        self.ledger = ShadowLedger(
            session_id=self.config.session_id,
            policy_mode=self.config.policy_mode,
            shadow_attach_ts=attach_ts,
            processing_mode=self.config.processing_mode,
        )
        self.stats = ShadowEngineStats()
        self._recent_decision_ids = deque(maxlen=self.config.recent_decision_buffer_size)

    def request_stop(self) -> None:
        """Ask the runtime to stop at the next safe loop boundary."""

        self._stop_requested = True

    def run(
        self,
        *,
        max_iterations: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Run the shadow engine until stop is requested or iteration cap is hit."""

        active_logger = logger or LOGGER
        iterations = 0
        try:
            while not self._stop_requested:
                if max_iterations is not None and iterations >= max_iterations:
                    break
                processed = self.process_next_state(logger=active_logger)
                iterations += 1
                self._maybe_log_heartbeat(active_logger)
                if not processed and self.config.idle_sleep_seconds > 0:
                    time.sleep(self.config.idle_sleep_seconds)
        finally:
            self.flush_summary(reconcile_with_disk=True)
            try:
                self.adapter.close()
            except Exception:
                active_logger.exception("shadow adapter close failed")

    def process_next_state(self, *, logger: logging.Logger | None = None) -> bool:
        """Process one state if available, isolating all shadow-side exceptions."""

        active_logger = logger or LOGGER
        self._record_adapter_soft_errors(active_logger)
        try:
            executable_state = self.adapter.read_state()
        except Exception:
            self.stats.error_count += 1
            active_logger.exception("shadow adapter read failed")
            return False
        self._record_adapter_soft_errors(active_logger)
        if executable_state is None:
            return False

        self.stats.read_count += 1
        self.stats.last_state_ts = executable_state.snapshot_ts
        decision_lag_ms = max(
            0,
            int((datetime.now(UTC) - executable_state.snapshot_ts).total_seconds() * 1000),
        )
        if (
            self.config.processing_mode == PROCESSING_MODE_LIVE_ONLY_FROM_ATTACH_TS
            and self.ledger.shadow_attach_ts is not None
            and executable_state.snapshot_ts < self.ledger.shadow_attach_ts
        ):
            self.ledger.record_backlog_decision(
                decision_ts=executable_state.snapshot_ts,
                decision_lag_ms=decision_lag_ms,
            )
            self.flush_summary()
            return True

        try:
            policy_decision = evaluate_policy_decision(
                PolicyEvaluationInput(
                    executable_state=executable_state,
                    policy_mode=self.config.policy_mode,
                    sizing_policy=self.config.sizing_policy,
                    min_net_edge=self.config.min_net_edge,
                    max_quote_age_ms=self.config.max_quote_age_ms,
                    max_spread_abs=self.config.max_spread_abs,
                    policy_name=self.config.policy_name,
                    policy_role=self.config.policy_role,
                )
            )
            self._record_policy_decision(
                policy_decision,
                decision_lag_ms=decision_lag_ms,
            )
            self.flush_summary()
            return True
        except Exception:
            self.stats.error_count += 1
            active_logger.exception(
                "shadow decision processing failed for session=%s state_ts=%s",
                self.config.session_id,
                executable_state.snapshot_ts,
            )
            return False

    def flush_summary(self, *, reconcile_with_disk: bool = False) -> None:
        """Write the current atomic summary snapshot."""

        summary = self.ledger.build_summary()
        if reconcile_with_disk:
            summary = reconcile_shadow_summary_from_artifacts(
                summary,
                shadow_decisions_path=self.writer.paths.shadow_decisions_path,
                shadow_order_states_path=self.writer.paths.shadow_order_states_path,
            )
        self.writer.write_shadow_summary(summary)

    def _record_policy_decision(
        self,
        policy_decision: PolicyDecision,
        *,
        decision_lag_ms: int | None = None,
    ) -> None:
        self.ledger.update_decision_lag(decision_lag_ms)
        seen_state = self.ledger.record_decision_seen(policy_decision.shadow_decision)
        self.writer.append_shadow_order_state(seen_state)
        self.writer.append_shadow_decision(policy_decision.shadow_decision)
        written_state = self.ledger.record_decision_written(policy_decision.shadow_decision)
        self.writer.append_shadow_order_state(written_state)
        self.stats.decision_count += 1
        self.stats.last_decision_id = policy_decision.shadow_decision.decision_id
        self._recent_decision_ids.append(policy_decision.shadow_decision.decision_id)
        if policy_decision.eligible:
            self.stats.actionable_count += 1
            self.stats.last_no_trade_reason = None
        else:
            self.stats.no_trade_count += 1
            self.stats.last_no_trade_reason = (
                None
                if policy_decision.no_trade_reason is None
                else policy_decision.no_trade_reason.value
            )

    def _maybe_log_heartbeat(self, logger: logging.Logger) -> None:
        now_monotonic = time.monotonic()
        if (
            self.config.heartbeat_interval_seconds <= 0
            or now_monotonic - self._last_heartbeat_monotonic
            < self.config.heartbeat_interval_seconds
        ):
            return
        logger.info(
            "shadow heartbeat session=%s reads=%s decisions=%s actionable=%s "
            "no_trade=%s backlog=%s live_forward=%s errors=%s "
            "last_state_ts=%s last_decision_id=%s "
            "last_no_trade_reason=%s buffer_size=%s",
            self.config.session_id,
            self.stats.read_count,
            self.stats.decision_count,
            self.stats.actionable_count,
            self.stats.no_trade_count,
            self.ledger.backlog_decision_count,
            self.ledger.live_forward_decision_count,
            self.stats.error_count,
            self.stats.last_state_ts,
            self.stats.last_decision_id,
            self.stats.last_no_trade_reason,
            len(self._recent_decision_ids),
        )
        self._last_heartbeat_monotonic = now_monotonic

    def _record_adapter_soft_errors(self, logger: logging.Logger) -> None:
        consume = getattr(self.adapter, "consume_soft_error_count", None)
        if consume is None:
            return
        try:
            soft_errors = int(consume())
        except Exception:
            self.stats.error_count += 1
            logger.exception("shadow adapter soft-error inspection failed")
            return
        if soft_errors <= 0:
            return
        self.stats.error_count += soft_errors
        logger.warning(
            "shadow adapter soft errors session=%s count=%s",
            self.config.session_id,
            soft_errors,
        )


__all__ = [
    "DEFAULT_HEARTBEAT_INTERVAL_SECONDS",
    "DEFAULT_IDLE_SLEEP_SECONDS",
    "DEFAULT_RECENT_DECISION_BUFFER_SIZE",
    "ShadowEngine",
    "ShadowEngineConfig",
    "ShadowEngineStats",
]
