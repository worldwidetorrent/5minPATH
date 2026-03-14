"""Strict Polymarket listing to canonical window mapping."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime

from rtds.collectors.polymarket.metadata import (
    NORMALIZER_VERSION as METADATA_NORMALIZER_VERSION,
)
from rtds.collectors.polymarket.metadata import MarketMetadataCandidate
from rtds.core.enums import AssetCode, ConfidenceLevel
from rtds.core.time import ensure_utc, utc_now, window_end
from rtds.mapping.window_ids import WindowBounds

MAPPING_VERSION = "0.1.0"
SCHEMA_VERSION = "0.1.0"
UPDOWN_PATTERNS = (
    re.compile(r"\bup\s*(?:or|/)\s*down\b", re.IGNORECASE),
    re.compile(r"\bhigher\s+or\s+lower\b", re.IGNORECASE),
    re.compile(r"\babove\s+or\s+below\b", re.IGNORECASE),
)


@dataclass(slots=True, frozen=True)
class CandidateAssessment:
    """Result of evaluating one candidate listing against hard rules."""

    market_id: str
    window_id: str | None
    accepted: bool
    reason: str


@dataclass(slots=True, frozen=True)
class WindowMarketMappingRecord:
    """Window-reference style record linking canonical and venue-side identity."""

    window_id: str
    asset_id: str
    window_start_ts: datetime
    window_end_ts: datetime
    polymarket_market_id: str | None
    polymarket_event_id: str | None
    polymarket_slug: str | None
    clob_token_id_up: str | None
    clob_token_id_down: str | None
    listing_discovered_ts: datetime | None
    market_active_flag: bool | None
    market_closed_flag: bool | None
    mapping_status: str
    mapping_confidence: str
    mapping_method: str
    notes: str | None
    schema_version: str
    normalizer_version: str
    mapping_version: str
    created_ts: datetime
    updated_ts: datetime

    def to_dict(self) -> dict[str, object]:
        """Materialize the record as a plain row dict."""

        return asdict(self)


@dataclass(slots=True, frozen=True)
class MappingBatch:
    """Mapping output plus candidate-level diagnostics."""

    records: list[WindowMarketMappingRecord]
    assessments: list[CandidateAssessment]


def _candidate_text(candidate: MarketMetadataCandidate) -> str:
    parts = [
        candidate.market_title or "",
        candidate.market_question or "",
        candidate.market_slug or "",
        candidate.category or "",
        candidate.subcategory or "",
    ]
    return " ".join(parts)


def _matches_updown_structure(candidate: MarketMetadataCandidate) -> bool:
    text = _candidate_text(candidate)
    return any(pattern.search(text) for pattern in UPDOWN_PATTERNS)


def _has_valid_token_ids(candidate: MarketMetadataCandidate) -> bool:
    if candidate.token_yes_id is None or candidate.token_no_id is None:
        return False
    return candidate.token_yes_id != candidate.token_no_id


def _aligned_window_id(
    candidate: MarketMetadataCandidate,
    *,
    schedule_by_window_id: dict[str, WindowBounds],
) -> str | None:
    if candidate.market_open_ts is None or candidate.market_close_ts is None:
        return None
    start_ts = ensure_utc(candidate.market_open_ts, field_name="market_open_ts")
    end_ts = ensure_utc(candidate.market_close_ts, field_name="market_close_ts")
    matching_window = schedule_by_window_id.get(
        f"{AssetCode.BTC.value.lower()}-5m-{start_ts.strftime('%Y%m%dT%H%M%SZ')}"
    )
    if matching_window is None:
        return None
    if matching_window.window_start_ts != start_ts:
        return None
    if matching_window.window_end_ts != end_ts:
        return None
    if window_end(start_ts) != end_ts:
        return None
    return matching_window.window_id


def assess_candidate(
    candidate: MarketMetadataCandidate,
    *,
    schedule_by_window_id: dict[str, WindowBounds],
) -> CandidateAssessment:
    """Evaluate a candidate against hard accept/reject rules."""

    if candidate.asset_id != AssetCode.BTC.value:
        return CandidateAssessment(
            market_id=candidate.market_id,
            window_id=None,
            accepted=False,
            reason="asset_mismatch",
        )

    if not _matches_updown_structure(candidate):
        return CandidateAssessment(
            market_id=candidate.market_id,
            window_id=None,
            accepted=False,
            reason="structure_mismatch",
        )

    if not _has_valid_token_ids(candidate):
        return CandidateAssessment(
            market_id=candidate.market_id,
            window_id=None,
            accepted=False,
            reason="token_ids_missing",
        )

    if candidate.market_open_ts is None or candidate.market_close_ts is None:
        return CandidateAssessment(
            market_id=candidate.market_id,
            window_id=None,
            accepted=False,
            reason="listing_times_missing",
        )

    duration_seconds = int(
        (candidate.market_close_ts - candidate.market_open_ts).total_seconds()
    )
    if duration_seconds != 300:
        return CandidateAssessment(
            market_id=candidate.market_id,
            window_id=None,
            accepted=False,
            reason="tenor_mismatch",
        )

    window_id = _aligned_window_id(candidate, schedule_by_window_id=schedule_by_window_id)
    if window_id is None:
        return CandidateAssessment(
            market_id=candidate.market_id,
            window_id=None,
            accepted=False,
            reason="window_misaligned",
        )

    return CandidateAssessment(
        market_id=candidate.market_id,
        window_id=window_id,
        accepted=True,
        reason="accepted",
    )


def map_candidates_to_windows(
    windows: list[WindowBounds],
    candidates: list[MarketMetadataCandidate],
    *,
    mapping_method: str = "strict_metadata_rules",
    created_ts: datetime | None = None,
) -> MappingBatch:
    """Bind accepted Polymarket candidates to canonical windows."""

    normalized_created_ts = (
        ensure_utc(created_ts, field_name="created_ts")
        if created_ts is not None
        else max((window.window_start_ts for window in windows), default=utc_now())
    )
    normalized_created_ts = ensure_utc(normalized_created_ts, field_name="created_ts")
    schedule_by_window_id = {window.window_id: window for window in windows}
    accepted_by_window_id: dict[str, list[MarketMetadataCandidate]] = {}
    assessments: list[CandidateAssessment] = []

    for candidate in candidates:
        assessment = assess_candidate(
            candidate,
            schedule_by_window_id=schedule_by_window_id,
        )
        assessments.append(assessment)
        if assessment.accepted and assessment.window_id is not None:
            accepted_by_window_id.setdefault(assessment.window_id, []).append(candidate)

    records = [
        _build_mapping_record(
            window=window,
            candidates=accepted_by_window_id.get(window.window_id, []),
            mapping_method=mapping_method,
            created_ts=normalized_created_ts,
        )
        for window in windows
    ]
    return MappingBatch(records=records, assessments=assessments)


def _build_mapping_record(
    *,
    window: WindowBounds,
    candidates: list[MarketMetadataCandidate],
    mapping_method: str,
    created_ts: datetime,
) -> WindowMarketMappingRecord:
    if not candidates:
        return WindowMarketMappingRecord(
            window_id=window.window_id,
            asset_id=AssetCode.BTC.value,
            window_start_ts=window.window_start_ts,
            window_end_ts=window.window_end_ts,
            polymarket_market_id=None,
            polymarket_event_id=None,
            polymarket_slug=None,
            clob_token_id_up=None,
            clob_token_id_down=None,
            listing_discovered_ts=None,
            market_active_flag=None,
            market_closed_flag=None,
            mapping_status="market_missing",
            mapping_confidence=ConfidenceLevel.NONE.value,
            mapping_method=mapping_method,
            notes=None,
            schema_version=SCHEMA_VERSION,
            normalizer_version=METADATA_NORMALIZER_VERSION,
            mapping_version=MAPPING_VERSION,
            created_ts=created_ts,
            updated_ts=created_ts,
        )

    if len(candidates) > 1:
        market_ids = ", ".join(sorted(candidate.market_id for candidate in candidates))
        return WindowMarketMappingRecord(
            window_id=window.window_id,
            asset_id=AssetCode.BTC.value,
            window_start_ts=window.window_start_ts,
            window_end_ts=window.window_end_ts,
            polymarket_market_id=None,
            polymarket_event_id=None,
            polymarket_slug=None,
            clob_token_id_up=None,
            clob_token_id_down=None,
            listing_discovered_ts=None,
            market_active_flag=None,
            market_closed_flag=None,
            mapping_status="market_ambiguous",
            mapping_confidence=ConfidenceLevel.NONE.value,
            mapping_method=mapping_method,
            notes=f"conflicting_markets={market_ids}",
            schema_version=SCHEMA_VERSION,
            normalizer_version=METADATA_NORMALIZER_VERSION,
            mapping_version=MAPPING_VERSION,
            created_ts=created_ts,
            updated_ts=created_ts,
        )

    candidate = candidates[0]
    return WindowMarketMappingRecord(
        window_id=window.window_id,
        asset_id=AssetCode.BTC.value,
        window_start_ts=window.window_start_ts,
        window_end_ts=window.window_end_ts,
        polymarket_market_id=candidate.market_id,
        polymarket_event_id=candidate.event_id,
        polymarket_slug=candidate.market_slug,
        clob_token_id_up=candidate.token_yes_id,
        clob_token_id_down=candidate.token_no_id,
        listing_discovered_ts=candidate.recv_ts,
        market_active_flag=candidate.active_flag,
        market_closed_flag=candidate.closed_flag,
        mapping_status="mapped",
        mapping_confidence=ConfidenceLevel.HIGH.value,
        mapping_method=mapping_method,
        notes=None,
        schema_version=SCHEMA_VERSION,
        normalizer_version=METADATA_NORMALIZER_VERSION,
        mapping_version=MAPPING_VERSION,
        created_ts=created_ts,
        updated_ts=created_ts,
    )


__all__ = [
    "CandidateAssessment",
    "MAPPING_VERSION",
    "MappingBatch",
    "SCHEMA_VERSION",
    "WindowMarketMappingRecord",
    "assess_candidate",
    "map_candidates_to_windows",
]
