"""Production-safe live-state adapter over session-scoped normalized capture outputs.

Required v0 normalized inputs are intentionally frozen to the same three datasets
the replay loader already treats as primary session-scoped market data:
- ``chainlink_ticks``
- ``exchange_quotes``
- ``polymarket_quotes``

Metadata is a secondary best-effort source only. It may fill token identifiers or
stable market context if those fields are absent from the primary quote rows, but it
must never become a second truth source for price, tradability, or timing.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rtds.execution.adapters import (
    ADAPTER_ROLE_LIVE_STATE,
    AdapterDescriptor,
    ExecutionStateAdapter,
)
from rtds.execution.file_tail import JsonlFileTail
from rtds.execution.models import ExecutableStateView
from rtds.execution.state_assembler import (
    CaptureOutputDerivedStateView,
    CaptureOutputLiveStateCache,
    CaptureOutputStateAssembler,
)
from rtds.features.volatility import DEFAULT_VOLATILITY_POLICY, VolatilityPolicy
from rtds.replay.calibrated_baseline import (
    load_frozen_calibration_runtime,
)
from rtds.replay.slices import DEFAULT_REPLAY_SLICE_POLICY, ReplaySlicePolicy

DEFAULT_NORMALIZED_ROOT = Path("data/normalized")
DEFAULT_ARTIFACTS_ROOT = Path("artifacts/collect")
REQUIRED_NORMALIZED_DATASETS = (
    "chainlink_ticks",
    "exchange_quotes",
    "polymarket_quotes",
)
OPTIONAL_SECONDARY_DATASETS = ("market_metadata_events",)
EMISSION_CADENCE_SAMPLE_COMPLETE_POLYMARKET = "sample_complete_polymarket"


@dataclass(slots=True, frozen=True)
class CaptureOutputLiveStateConfig:
    """Config for the capture-output live-state adapter."""

    session_id: str
    normalized_root: Path = DEFAULT_NORMALIZED_ROOT
    artifacts_root: Path = DEFAULT_ARTIFACTS_ROOT
    calibration_config_path: Path | None = None
    calibration_summary_path: Path | None = None
    replay_slice_policy: ReplaySlicePolicy = DEFAULT_REPLAY_SLICE_POLICY
    volatility_policy: VolatilityPolicy = DEFAULT_VOLATILITY_POLICY

    def __post_init__(self) -> None:
        if not str(self.session_id).strip():
            raise ValueError("session_id must be non-empty")
        object.__setattr__(self, "normalized_root", Path(self.normalized_root))
        object.__setattr__(self, "artifacts_root", Path(self.artifacts_root))
        if (self.calibration_config_path is None) != (self.calibration_summary_path is None):
            raise ValueError(
                "calibration_config_path and calibration_summary_path "
                "must both be set or both be omitted"
            )
        if self.calibration_config_path is not None:
            object.__setattr__(
                self,
                "calibration_config_path",
                Path(self.calibration_config_path),
            )
            object.__setattr__(
                self,
                "calibration_summary_path",
                Path(self.calibration_summary_path),
            )


class CaptureOutputLiveStateAdapter(ExecutionStateAdapter):
    """Tail session-scoped normalized capture outputs into live execution state rows.

    Production v0 input surfaces are frozen to the same normalized datasets replay
    already loads as primary truth:
    - ``chainlink_ticks``
    - ``exchange_quotes``
    - ``polymarket_quotes``

    ``market_metadata_events`` remains a secondary optional input only.

    Emission cadence is frozen to one state row per newly-complete Polymarket quote
    sample, with the freshest available Chainlink and exchange state merged in. Rows
    that cannot support tradability checks are skipped rather than emitted as partial
    executable states.
    """

    descriptor = AdapterDescriptor(
        adapter_name="capture-output-live-state",
        adapter_role=ADAPTER_ROLE_LIVE_STATE,
        production_safe=True,
    )

    def __init__(self, config: CaptureOutputLiveStateConfig) -> None:
        self.config = config
        self._closed = False
        calibration_runtime = None
        if (
            config.calibration_config_path is not None
            and config.calibration_summary_path is not None
        ):
            calibration_runtime = load_frozen_calibration_runtime(
                config_path=config.calibration_config_path,
                summary_path=config.calibration_summary_path,
            )
        self._assembler = CaptureOutputStateAssembler(
            session_id=config.session_id,
            calibration_runtime=calibration_runtime,
            replay_slice_policy=config.replay_slice_policy,
            volatility_policy=config.volatility_policy,
        )
        self._last_emitted_state_fingerprint: str | None = None
        self._last_emitted_identity: tuple[str, object, str] | None = None
        self._pending_samples: deque[dict[str, Any]] = deque()
        self._sample_tailer = JsonlFileTail(
            pattern=(
                f"{config.artifacts_root}/date=*/session={config.session_id}/sample_diagnostics.jsonl"
            )
        )
        self._chainlink_tailer = JsonlFileTail(
            pattern=(
                f"{config.normalized_root}/chainlink_ticks/date=*/session={config.session_id}/*.jsonl"
            )
        )
        self._exchange_tailer = JsonlFileTail(
            pattern=(
                f"{config.normalized_root}/exchange_quotes/date=*/session={config.session_id}/*.jsonl"
            )
        )
        self._polymarket_tailer = JsonlFileTail(
            pattern=(
                f"{config.normalized_root}/polymarket_quotes/date=*/session={config.session_id}/*.jsonl"
            )
        )
        self._metadata_tailer = JsonlFileTail(
            pattern=(
                f"{config.normalized_root}/market_metadata_events/date=*/session={config.session_id}/*.jsonl"
            )
        )

    def read_state(self) -> ExecutableStateView | None:
        if self._closed:
            return None
        self._refresh_tails()
        while self._pending_samples:
            sample_row = self._pending_samples.popleft()
            if not _is_polymarket_completion_trigger(sample_row):
                continue
            state = self._assembler.build_state(sample_row)
            if state is None:
                continue
            if not _supports_tradability(state):
                continue
            identity = (
                state.window_id,
                state.snapshot_ts,
                state.polymarket_market_id,
            )
            if (
                state.state_fingerprint == self._last_emitted_state_fingerprint
                or identity == self._last_emitted_identity
            ):
                continue
            self._last_emitted_state_fingerprint = state.state_fingerprint
            self._last_emitted_identity = identity
            return state
        return None

    def close(self) -> None:
        self._closed = True

    @property
    def state_cache(self) -> CaptureOutputLiveStateCache:
        return self._assembler.state_cache

    def _refresh_tails(self) -> None:
        for row in self._chainlink_tailer.read_new_rows():
            self._assembler.ingest_chainlink_row(row)
        for row in self._exchange_tailer.read_new_rows():
            self._assembler.ingest_exchange_row(row)
        for row in self._polymarket_tailer.read_new_rows():
            self._assembler.ingest_polymarket_row(row)
        for row in self._metadata_tailer.read_new_rows():
            self._assembler.ingest_metadata_row(row)
        for row in self._sample_tailer.read_new_rows():
            self._pending_samples.append(dict(row))


def _is_polymarket_completion_trigger(sample_row: dict[str, Any]) -> bool:
    if sample_row.get("selected_market_id") is None or sample_row.get("selected_window_id") is None:
        return False
    poly_result = sample_row.get("source_results", {}).get("polymarket_quotes", {})
    return str(poly_result.get("status", "")).strip().lower() == "success"


def _supports_tradability(state: ExecutableStateView) -> bool:
    required_values = (
        state.quote_event_ts,
        state.quote_recv_ts,
        state.up_bid_price,
        state.up_ask_price,
        state.down_bid_price,
        state.down_ask_price,
        state.up_bid_size_contracts,
        state.up_ask_size_contracts,
        state.down_bid_size_contracts,
        state.down_ask_size_contracts,
    )
    return all(value is not None for value in required_values)


__all__ = [
    "CaptureOutputLiveStateAdapter",
    "CaptureOutputLiveStateConfig",
    "CaptureOutputDerivedStateView",
    "CaptureOutputLiveStateCache",
    "CaptureOutputStateAssembler",
    "EMISSION_CADENCE_SAMPLE_COMPLETE_POLYMARKET",
    "OPTIONAL_SECONDARY_DATASETS",
    "REQUIRED_NORMALIZED_DATASETS",
]
