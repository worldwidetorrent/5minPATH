"""Assemble execution live-state views from normalized capture inputs.

This module is intentionally a live execution-state assembler, not a replay
snapshot builder. It consumes the latest normalized capture surfaces already
written by capture and emits one deterministic ``ExecutableStateView`` for the
current decision timestamp.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from rtds.core.ids import parse_window_id
from rtds.core.time import age_ms, parse_utc, seconds_remaining, window_end
from rtds.execution.adapters import ADAPTER_ROLE_LIVE_STATE
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
)
from rtds.replay.slices import DEFAULT_REPLAY_SLICE_POLICY, ReplaySlicePolicy
from rtds.schemas.normalized import ExchangeQuote, PolymarketQuote


@dataclass(slots=True, frozen=True)
class CaptureOutputDerivedStateView:
    """Minimal latest-known derived state for one decision timestamp."""

    decision_ts: datetime
    current_oracle_tick: ChainlinkTick | None
    latest_exchange_mid_by_venue: dict[str, Decimal]
    latest_polymarket_quote: PolymarketQuote | None
    quote_age_ms: int | None


@dataclass(slots=True)
class CaptureOutputLiveStateCache:
    """Incrementally updated latest-known state surface for execution."""

    latest_chainlink_tick: ChainlinkTick | None = None
    latest_exchange_by_venue: dict[str, ExchangeQuote] = field(default_factory=dict)
    latest_exchange_mid_by_venue: dict[str, Decimal] = field(default_factory=dict)
    latest_polymarket_by_market: dict[str, PolymarketQuote] = field(default_factory=dict)
    latest_metadata_by_market: dict[str, dict[str, Any]] = field(default_factory=dict)

    def update_chainlink_tick(self, tick: ChainlinkTick) -> None:
        self.latest_chainlink_tick = tick

    def update_exchange_quote(self, quote: ExchangeQuote) -> None:
        self.latest_exchange_by_venue[quote.venue_id] = quote
        self.latest_exchange_mid_by_venue[quote.venue_id] = quote.mid_price

    def update_polymarket_quote(self, quote: PolymarketQuote) -> None:
        self.latest_polymarket_by_market[quote.market_id] = quote

    def update_metadata_row(self, market_id: str, payload: dict[str, Any]) -> None:
        self.latest_metadata_by_market[market_id] = dict(payload)

    def derived_for_market(
        self,
        market_id: str,
        *,
        decision_ts: datetime,
    ) -> CaptureOutputDerivedStateView:
        quote = self.latest_polymarket_by_market.get(str(market_id))
        quote_age_ms = None
        if quote is not None and quote.recv_ts is not None:
            if decision_ts >= quote.recv_ts:
                quote_age_ms = int(age_ms(decision_ts, quote.recv_ts))
            else:
                quote_age_ms = 0
        return CaptureOutputDerivedStateView(
            decision_ts=decision_ts,
            current_oracle_tick=self.latest_chainlink_tick,
            latest_exchange_mid_by_venue=dict(self.latest_exchange_mid_by_venue),
            latest_polymarket_quote=quote,
            quote_age_ms=quote_age_ms,
        )


@dataclass(slots=True)
class CaptureOutputStateAssembler:
    """Assemble one execution state row from tailed normalized capture outputs.

    The executable-state truth surface is frozen to three primary normalized datasets:
    Chainlink ticks, exchange quotes, and Polymarket quotes. Metadata is consulted only
    as a fallback for stable identifiers when the primary Polymarket quote row does not
    already provide them.
    """

    session_id: str
    calibration_runtime: FrozenCalibrationRuntime | None = None
    replay_slice_policy: ReplaySlicePolicy = DEFAULT_REPLAY_SLICE_POLICY
    volatility_policy: VolatilityPolicy = DEFAULT_VOLATILITY_POLICY
    state_cache: CaptureOutputLiveStateCache = field(default_factory=CaptureOutputLiveStateCache)
    open_anchor_by_window: dict[str, Decimal] = field(default_factory=dict)
    nowcast_history: deque[CompositeNowcast] = field(default_factory=lambda: deque(maxlen=256))

    def ingest_chainlink_row(self, payload: dict[str, Any]) -> None:
        self.state_cache.update_chainlink_tick(
            ChainlinkTick(
                event_id=str(payload["event_id"]),
                event_ts=parse_utc(str(payload["event_ts"])),
                price=payload["price"],
                recv_ts=(
                    None
                    if payload.get("recv_ts") is None
                    else parse_utc(str(payload.get("recv_ts")))
                ),
                oracle_feed_id=str(payload.get("oracle_feed_id", "chainlink:stream:BTC-USD")),
                round_id=(
                    None if payload.get("round_id") is None else str(payload.get("round_id"))
                ),
                oracle_source=str(payload.get("oracle_source", "")),
                bid_price=payload.get("bid_price"),
                ask_price=payload.get("ask_price"),
            )
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
        self.state_cache.update_exchange_quote(quote)

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
        self.state_cache.update_polymarket_quote(quote)

    def ingest_metadata_row(self, payload: dict[str, Any]) -> None:
        market_id = str(payload["market_id"])
        self.state_cache.update_metadata_row(market_id, payload)

    def build_state(self, sample_row: dict[str, Any]) -> ExecutableStateView | None:
        selected_market_id = sample_row.get("selected_market_id")
        selected_window_id = sample_row.get("selected_window_id")
        if selected_market_id is None or selected_window_id is None:
            return None

        sample_ts = parse_utc(str(sample_row["sample_started_at"]))
        selected_market_id_str = str(selected_market_id)
        selected_window_id_str = str(selected_window_id)
        _, window_start_ts = parse_window_id(selected_window_id_str)
        window_end_ts = window_end(window_start_ts)
        remaining_seconds = _seconds_remaining(
            sample_row,
            sample_ts=sample_ts,
            window_end_ts=window_end_ts,
        )
        derived_state = self.state_cache.derived_for_market(
            selected_market_id_str,
            decision_ts=sample_ts,
        )
        polymarket_quote = derived_state.latest_polymarket_quote
        metadata = self.state_cache.latest_metadata_by_market.get(selected_market_id_str, {})

        if (
            selected_window_id_str not in self.open_anchor_by_window
            and derived_state.current_oracle_tick is not None
        ):
            self.open_anchor_by_window[selected_window_id_str] = (
                derived_state.current_oracle_tick.price
            )
        open_anchor = self.open_anchor_by_window.get(selected_window_id_str)

        composite_nowcast = self._build_composite_nowcast(sample_ts)
        if composite_nowcast is not None:
            self.nowcast_history.append(composite_nowcast)
        volatility = self._build_volatility(sample_ts)
        fair_value = compute_fair_value_base(
            chainlink_open_anchor_price=open_anchor,
            composite_now_price=(
                None if composite_nowcast is None else composite_nowcast.composite_now_price
            ),
            seconds_remaining=remaining_seconds,
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

        return ExecutableStateView(
            session_id=self.session_id,
            state_source_kind=ADAPTER_ROLE_LIVE_STATE,
            snapshot_ts=sample_ts,
            window_id=selected_window_id_str,
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            seconds_remaining=remaining_seconds,
            polymarket_market_id=selected_market_id_str,
            polymarket_slug=_optional_str(sample_row.get("selected_market_slug")),
            clob_token_id_up=_optional_str(
                None if polymarket_quote is None else polymarket_quote.token_yes_id
            )
            or _optional_str(metadata.get("token_yes_id")),
            clob_token_id_down=_optional_str(
                None if polymarket_quote is None else polymarket_quote.token_no_id
            )
            or _optional_str(metadata.get("token_no_id")),
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
            quote_age_ms=derived_state.quote_age_ms,
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
        quotes = list(self.state_cache.latest_exchange_by_venue.values())
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
    return int(seconds_remaining(window_end_ts, sample_ts))


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
    "CaptureOutputDerivedStateView",
    "CaptureOutputLiveStateCache",
    "CaptureOutputStateAssembler",
]
