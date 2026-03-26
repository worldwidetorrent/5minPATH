"""Production-safe live-state adapter over session-scoped normalized capture outputs."""

from __future__ import annotations

import glob
import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from rtds.core.ids import parse_window_id
from rtds.core.time import ensure_utc, parse_utc
from rtds.execution.adapters import (
    ADAPTER_ROLE_LIVE_STATE,
    AdapterDescriptor,
    ExecutionStateAdapter,
)
from rtds.execution.models import ExecutableStateView
from rtds.features.composite_nowcast import CompositeNowcast, compute_composite_nowcast
from rtds.features.fair_value_base import compute_fair_value_base
from rtds.features.volatility import (
    DEFAULT_VOLATILITY_POLICY,
    VolatilityPolicy,
    compute_volatility_from_nowcasts,
)
from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.replay.calibrated_baseline import (
    FrozenCalibrationRuntime,
    apply_frozen_stage1_calibration,
    load_frozen_calibration_runtime,
)
from rtds.replay.slices import DEFAULT_REPLAY_SLICE_POLICY, ReplaySlicePolicy
from rtds.schemas.normalized import ExchangeQuote, PolymarketQuote

DEFAULT_NORMALIZED_ROOT = Path("data/normalized")
DEFAULT_ARTIFACTS_ROOT = Path("artifacts/collect")


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


@dataclass(slots=True)
class _JsonlTailer:
    pattern: str
    _offsets: dict[Path, int] = field(default_factory=dict)

    def read_new_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for raw_path in sorted(glob.glob(self.pattern)):
            path = Path(raw_path)
            if not path.is_file():
                continue
            offset = self._offsets.get(path, 0)
            with path.open("r", encoding="utf-8") as handle:
                handle.seek(offset)
                while True:
                    start_pos = handle.tell()
                    line = handle.readline()
                    if not line:
                        break
                    if not line.endswith("\n"):
                        handle.seek(start_pos)
                        break
                    payload = line.strip()
                    if not payload:
                        continue
                    rows.append(json.loads(payload))
                self._offsets[path] = handle.tell()
        return rows


@dataclass(slots=True)
class CaptureOutputStateAssembler:
    """Assemble one execution state row from tailed normalized capture outputs."""

    session_id: str
    calibration_runtime: FrozenCalibrationRuntime | None = None
    replay_slice_policy: ReplaySlicePolicy = DEFAULT_REPLAY_SLICE_POLICY
    volatility_policy: VolatilityPolicy = DEFAULT_VOLATILITY_POLICY
    latest_chainlink_tick: ChainlinkTick | None = None
    latest_exchange_by_venue: dict[str, ExchangeQuote] = field(default_factory=dict)
    latest_polymarket_by_market: dict[str, PolymarketQuote] = field(default_factory=dict)
    latest_metadata_by_market: dict[str, dict[str, Any]] = field(default_factory=dict)
    open_anchor_by_window: dict[str, Decimal] = field(default_factory=dict)
    nowcast_history: deque[CompositeNowcast] = field(default_factory=lambda: deque(maxlen=256))

    def ingest_chainlink_row(self, payload: dict[str, Any]) -> None:
        self.latest_chainlink_tick = ChainlinkTick(
            event_id=str(payload["event_id"]),
            event_ts=parse_utc(str(payload["event_ts"])),
            price=payload["price"],
            recv_ts=(
                None
                if payload.get("recv_ts") is None
                else parse_utc(str(payload.get("recv_ts")))
            ),
            oracle_feed_id=str(payload.get("oracle_feed_id", "chainlink:stream:BTC-USD")),
            round_id=None if payload.get("round_id") is None else str(payload.get("round_id")),
            oracle_source=str(payload.get("oracle_source", "")),
            bid_price=payload.get("bid_price"),
            ask_price=payload.get("ask_price"),
        )

    def ingest_exchange_row(self, payload: dict[str, Any]) -> None:
        quote = ExchangeQuote(
            **{
                **payload,
                "event_ts": parse_utc(str(payload["event_ts"])),
                "recv_ts": parse_utc(str(payload["recv_ts"])),
                "proc_ts": parse_utc(str(payload["proc_ts"])),
                "created_ts": parse_utc(str(payload["created_ts"])),
            }
        )
        self.latest_exchange_by_venue[quote.venue_id] = quote

    def ingest_polymarket_row(self, payload: dict[str, Any]) -> None:
        quote = PolymarketQuote(
            **{
                **payload,
                "event_ts": parse_utc(str(payload["event_ts"])),
                "recv_ts": parse_utc(str(payload["recv_ts"])),
                "proc_ts": parse_utc(str(payload["proc_ts"])),
                "created_ts": parse_utc(str(payload["created_ts"])),
            }
        )
        self.latest_polymarket_by_market[quote.market_id] = quote

    def ingest_metadata_row(self, payload: dict[str, Any]) -> None:
        market_id = str(payload["market_id"])
        self.latest_metadata_by_market[market_id] = dict(payload)

    def build_state(self, sample_row: dict[str, Any]) -> ExecutableStateView | None:
        selected_market_id = sample_row.get("selected_market_id")
        selected_window_id = sample_row.get("selected_window_id")
        if selected_market_id is None or selected_window_id is None:
            return None
        sample_ts = parse_utc(str(sample_row["sample_started_at"]))
        polymarket_quote = self.latest_polymarket_by_market.get(str(selected_market_id))
        metadata = self.latest_metadata_by_market.get(str(selected_market_id), {})
        window_start_ts = parse_window_id(str(selected_window_id))[1]
        window_end_ts = window_start_ts + timedelta(minutes=5)
        seconds_remaining = _seconds_remaining(
            sample_row,
            sample_ts=sample_ts,
            window_end_ts=window_end_ts,
        )

        if (
            str(selected_window_id) not in self.open_anchor_by_window
            and self.latest_chainlink_tick is not None
        ):
            self.open_anchor_by_window[str(selected_window_id)] = self.latest_chainlink_tick.price
        open_anchor = self.open_anchor_by_window.get(str(selected_window_id))

        composite_nowcast = self._build_composite_nowcast(sample_ts)
        if composite_nowcast is not None:
            self.nowcast_history.append(composite_nowcast)
        volatility = self._build_volatility(sample_ts)
        fair_value = compute_fair_value_base(
            chainlink_open_anchor_price=open_anchor,
            composite_now_price=(
                None if composite_nowcast is None else composite_nowcast.composite_now_price
            ),
            seconds_remaining=seconds_remaining,
            sigma_eff=None if volatility is None else volatility.sigma_eff,
        )
        calibration_bucket = None
        calibration_support_flag = None
        calibrated_fair_value = fair_value.fair_value_base
        if self.calibration_runtime is not None and fair_value.fair_value_base is not None:
            applied = apply_frozen_stage1_calibration(
                fair_value.fair_value_base,
                runtime=self.calibration_runtime,
            )
            calibration_bucket = applied.bucket_name
            calibration_support_flag = applied.support_flag
            calibrated_fair_value = applied.calibrated_f

        quote_event_ts = None if polymarket_quote is None else polymarket_quote.event_ts
        quote_recv_ts = None if polymarket_quote is None else polymarket_quote.recv_ts
        quote_age_ms = None
        if quote_recv_ts is not None:
            quote_age_ms = max(
                0,
                int((sample_ts - quote_recv_ts).total_seconds() * 1000),
            )

        return ExecutableStateView(
            session_id=self.session_id,
            state_source_kind=ADAPTER_ROLE_LIVE_STATE,
            snapshot_ts=sample_ts,
            window_id=str(selected_window_id),
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            seconds_remaining=seconds_remaining,
            polymarket_market_id=str(selected_market_id),
            polymarket_slug=(
                None
                if sample_row.get("selected_market_slug") is None
                else str(sample_row.get("selected_market_slug"))
            ),
            clob_token_id_up=_optional_str(metadata.get("token_yes_id")),
            clob_token_id_down=_optional_str(metadata.get("token_no_id")),
            window_quality_regime=_window_quality_regime(sample_row),
            chainlink_confidence_state=_chainlink_confidence_state(sample_row),
            volatility_regime=_volatility_regime(
                None if volatility is None else volatility.sigma_eff,
                policy=self.replay_slice_policy,
            ),
            fair_value_base=fair_value.fair_value_base,
            calibrated_fair_value_base=calibrated_fair_value,
            calibration_bucket=calibration_bucket,
            calibration_support_flag=calibration_support_flag,
            quote_source="polymarket",
            quote_event_ts=quote_event_ts,
            quote_recv_ts=quote_recv_ts,
            quote_age_ms=quote_age_ms,
            up_bid_price=None if polymarket_quote is None else polymarket_quote.up_bid,
            up_ask_price=None if polymarket_quote is None else polymarket_quote.up_ask,
            down_bid_price=None if polymarket_quote is None else polymarket_quote.down_bid,
            down_ask_price=None if polymarket_quote is None else polymarket_quote.down_ask,
            up_bid_size_contracts=(
                None if polymarket_quote is None else polymarket_quote.up_bid_size_contracts
            ),
            up_ask_size_contracts=(
                None if polymarket_quote is None else polymarket_quote.up_ask_size_contracts
            ),
            down_bid_size_contracts=(
                None if polymarket_quote is None else polymarket_quote.down_bid_size_contracts
            ),
            down_ask_size_contracts=(
                None if polymarket_quote is None else polymarket_quote.down_ask_size_contracts
            ),
            up_spread_abs=(
                None if polymarket_quote is None else polymarket_quote.market_spread_up_abs
            ),
            down_spread_abs=(
                None if polymarket_quote is None else polymarket_quote.market_spread_down_abs
            ),
            market_actionable_flag=sample_row.get("sample_status") != "failed",
        )

    def _build_composite_nowcast(self, sample_ts: datetime) -> CompositeNowcast | None:
        quotes = list(self.latest_exchange_by_venue.values())
        if not quotes:
            return None
        try:
            return compute_composite_nowcast(quotes, as_of_ts=sample_ts)
        except Exception:
            return None

    def _build_volatility(self, sample_ts: datetime):
        nowcasts = [
            nowcast
            for nowcast in self.nowcast_history
            if nowcast.composite_now_price is not None
        ]
        if not nowcasts:
            return None
        try:
            return compute_volatility_from_nowcasts(
                nowcasts,
                as_of_ts=sample_ts,
                policy=self.volatility_policy,
            )
        except Exception:
            return None


class CaptureOutputLiveStateAdapter(ExecutionStateAdapter):
    """Tail session-scoped normalized capture outputs into live execution state rows."""

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
        self._pending_samples: deque[dict[str, Any]] = deque()
        self._sample_tailer = _JsonlTailer(
            pattern=(
                f"{config.artifacts_root}/date=*/session={config.session_id}/sample_diagnostics.jsonl"
            )
        )
        self._chainlink_tailer = _JsonlTailer(
            pattern=(
                f"{config.normalized_root}/chainlink_ticks/date=*/session={config.session_id}/*.jsonl"
            )
        )
        self._exchange_tailer = _JsonlTailer(
            pattern=(
                f"{config.normalized_root}/exchange_quotes/date=*/session={config.session_id}/*.jsonl"
            )
        )
        self._polymarket_tailer = _JsonlTailer(
            pattern=(
                f"{config.normalized_root}/polymarket_quotes/date=*/session={config.session_id}/*.jsonl"
            )
        )
        self._metadata_tailer = _JsonlTailer(
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
            state = self._assembler.build_state(sample_row)
            if state is not None:
                return state
        return None

    def close(self) -> None:
        self._closed = True

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


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _seconds_remaining(
    sample_row: dict[str, Any],
    *,
    sample_ts: datetime,
    window_end_ts: datetime,
) -> int:
    poly_details = (
        sample_row.get("source_results", {})
        .get("polymarket_quotes", {})
        .get("details", {})
    )
    if poly_details.get("seconds_remaining") is not None:
        return max(0, int(Decimal(str(poly_details["seconds_remaining"]))))
    return max(0, int((ensure_utc(window_end_ts) - ensure_utc(sample_ts)).total_seconds()))


def _window_quality_regime(sample_row: dict[str, Any]) -> str:
    sample_status = str(sample_row.get("sample_status", "unknown")).strip().lower()
    degraded_sources = tuple(sample_row.get("degraded_sources", []))
    if sample_status == "failed":
        return "unusable"
    if not degraded_sources:
        return "good"
    return "degraded_light"


def _chainlink_confidence_state(sample_row: dict[str, Any]) -> str:
    chainlink_result = sample_row.get("source_results", {}).get("chainlink", {})
    if str(chainlink_result.get("status", "")).lower() != "success":
        return "low"
    details = chainlink_result.get("details", {})
    if bool(details.get("fallback_used")):
        return "medium"
    return "high"


def _volatility_regime(
    sigma_eff: Decimal | None,
    *,
    policy: ReplaySlicePolicy,
) -> str:
    if sigma_eff is None:
        return "unknown"
    if sigma_eff < policy.low_vol_threshold:
        return "low_vol"
    if sigma_eff < policy.high_vol_threshold:
        return "mid_vol"
    return "high_vol"


__all__ = [
    "CaptureOutputLiveStateAdapter",
    "CaptureOutputLiveStateConfig",
    "CaptureOutputStateAssembler",
]
