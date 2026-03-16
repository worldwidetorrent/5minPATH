"""Capture-session admission summary for pilot-length replay decisions."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from rtds.collectors.phase1_capture import Phase1CaptureResult
from rtds.collectors.polymarket.metadata import MarketMetadataCandidate
from rtds.core.enums import AssetCode, ConfidenceLevel
from rtds.core.ids import build_window_id
from rtds.core.time import parse_utc
from rtds.mapping.anchor_assignment import assign_window_references
from rtds.mapping.market_mapper import RECURRING_5M_SLUG_PATTERN, WindowMarketMappingRecord
from rtds.mapping.window_ids import daily_window_schedule, owning_window_id
from rtds.replay.loader import _row_to_chainlink_tick, _row_to_metadata_candidate
from rtds.storage.writer import write_json_file

EXCHANGE_VENUES = ("binance", "coinbase", "kraken")
ADMISSIBLE_MAX_DEGRADED_RATIO = 0.10
CONDITIONALLY_ADMISSIBLE_MAX_DEGRADED_RATIO = 0.25
ADMISSIBLE_MIN_SNAPSHOT_ELIGIBLE_RATIO = 0.90
CONDITIONALLY_ADMISSIBLE_MIN_SNAPSHOT_ELIGIBLE_RATIO = 0.60
ADMISSIBLE_MAX_OUTSIDE_GRACE_DEGRADED_SAMPLES = 1
CONDITIONALLY_ADMISSIBLE_MAX_OUTSIDE_GRACE_DEGRADED_SAMPLES = 3


@dataclass(slots=True, frozen=True)
class MetadataStripDiagnostics:
    """Selector-adjacent metadata breadth and ambiguity diagnostics."""

    target_family_candidate_row_count: int
    target_family_unique_market_count: int
    target_family_window_count: int
    ambiguous_window_count: int
    ambiguous_but_resolved_window_count: int


@dataclass(slots=True, frozen=True)
class SelectedWindowBindings:
    """Final per-window selected bindings resolved from sample diagnostics."""

    records: list[WindowMarketMappingRecord]
    observed_window_ids: list[str]
    unresolved_window_ids: list[str]


def write_capture_admission_summary(result: Phase1CaptureResult) -> Path:
    """Persist one capture-session admission summary next to the session summary."""

    summary = build_capture_admission_summary(result)
    output_path = result.summary_path.with_name("admission_summary.json")
    write_json_file(output_path, summary)
    return output_path


def build_capture_admission_summary(result: Phase1CaptureResult) -> dict[str, object]:
    """Build replay-admission diagnostics for one finished capture session."""

    sample_rows = _read_jsonl_rows(result.session_diagnostics.sample_diagnostics_path)
    sample_count = len(sample_rows)
    metadata_rows = _load_metadata_candidates(_collector_path(result, "polymarket_metadata"))
    chainlink_ticks = _load_chainlink_ticks(_collector_path(result, "chainlink"))

    selected_bindings = _resolve_selected_window_bindings(
        sample_rows,
        metadata_rows=metadata_rows,
        capture_date=result.capture_date,
    )
    metadata_strip = _summarize_metadata_strip(
        metadata_rows,
        resolved_window_ids=set(record.window_id for record in selected_bindings.records),
    )
    reference_by_window_id = {
        reference.window_id: reference
        for reference in assign_window_references(selected_bindings.records, chainlink_ticks)
    }

    family_compliance_flags = [
        _sample_is_family_compliant(sample_row) for sample_row in sample_rows
    ]
    family_compliance_count = sum(family_compliance_flags)
    off_family_switch_count = sum(
        1
        for sample_row, is_compliant in zip(sample_rows, family_compliance_flags, strict=True)
        if sample_row.get("selected_market_id") is not None and not is_compliant
    )

    sample_status_counts = Counter(str(row.get("sample_status", "unknown")) for row in sample_rows)
    degraded_inside_grace = 0
    degraded_outside_grace = 0
    metadata_refresh_attempts = 0
    selector_rebind_count = 0
    family_validation_counts: Counter[str] = Counter()
    exchange_present_count: Counter[str] = Counter()
    exchange_missing_count: Counter[str] = Counter()
    exchange_max_consecutive_missing: Counter[str] = Counter()
    exchange_current_missing: Counter[str] = Counter()
    chainlink_present_count = 0
    chainlink_oracle_source_count: Counter[str] = Counter()
    polymarket_present_count = 0
    samples_with_all_exchange_venues = 0
    snapshot_eligible_sample_count = 0

    anchor_confidence_breakdown: Counter[str] = Counter()
    anchor_confidence_by_window_id: dict[str, str] = {}
    for window_id in selected_bindings.observed_window_ids:
        reference = reference_by_window_id.get(window_id)
        confidence = (
            reference.chainlink_open_anchor_confidence
            if reference is not None
            else ConfidenceLevel.NONE.value
        )
        anchor_confidence_breakdown[confidence] += 1
        anchor_confidence_by_window_id[window_id] = confidence

    for sample_row, is_family_compliant in zip(sample_rows, family_compliance_flags, strict=True):
        family_validation_counts[str(sample_row.get("family_validation_status", "unknown"))] += 1
        source_results = sample_row.get("source_results", {})
        if not isinstance(source_results, dict):
            source_results = {}

        chainlink_result = _source_result(source_results, "chainlink")
        exchange_result = _source_result(source_results, "exchange")
        polymarket_result = _source_result(source_results, "polymarket_quotes")
        polymarket_details = _details(polymarket_result)

        if chainlink_result.get("normalized_row_count", 0) > 0:
            chainlink_present_count += 1
        chainlink_details = _details(chainlink_result)
        oracle_source = chainlink_details.get("oracle_source")
        if isinstance(oracle_source, str) and oracle_source:
            chainlink_oracle_source_count[oracle_source] += 1

        exchange_details = _details(exchange_result)
        venue_statuses = exchange_details.get("venue_statuses", {})
        if not isinstance(venue_statuses, dict):
            venue_statuses = {}
        if all(str(venue_statuses.get(venue)) == "success" for venue in EXCHANGE_VENUES):
            samples_with_all_exchange_venues += 1
        for venue in EXCHANGE_VENUES:
            if str(venue_statuses.get(venue)) == "success":
                exchange_present_count[venue] += 1
                exchange_current_missing[venue] = 0
            else:
                exchange_missing_count[venue] += 1
                exchange_current_missing[venue] += 1
                exchange_max_consecutive_missing[venue] = max(
                    exchange_max_consecutive_missing[venue],
                    exchange_current_missing[venue],
                )

        if polymarket_result.get("normalized_row_count", 0) > 0:
            polymarket_present_count += 1

        if polymarket_details.get("metadata_refresh_attempted"):
            metadata_refresh_attempts += 1
        if polymarket_details.get("metadata_refresh_changed_binding"):
            selector_rebind_count += 1

        if sample_row.get("sample_status") == "degraded":
            if polymarket_details.get("within_rollover_grace_window"):
                degraded_inside_grace += 1
            else:
                degraded_outside_grace += 1

        selected_window_id = sample_row.get("selected_window_id")
        anchor_confidence = (
            anchor_confidence_by_window_id.get(str(selected_window_id))
            if selected_window_id is not None
            else None
        )
        if (
            is_family_compliant
            and chainlink_result.get("normalized_row_count", 0) > 0
            and polymarket_result.get("normalized_row_count", 0) > 0
            and all(str(venue_statuses.get(venue)) == "success" for venue in EXCHANGE_VENUES)
            and anchor_confidence in {"high", "medium", "low"}
        ):
            snapshot_eligible_sample_count += 1

    degraded_sample_count = int(sample_status_counts.get("degraded", 0))
    failed_sample_count = int(sample_status_counts.get("failed", 0))
    healthy_sample_count = int(sample_status_counts.get("healthy", 0))
    degraded_ratio = (degraded_sample_count / sample_count) if sample_count else 0.0
    snapshot_eligible_ratio = (
        snapshot_eligible_sample_count / sample_count if sample_count else 0.0
    )

    verdict, verdict_reasons = _classify_admission_verdict(
        termination_reason=result.session_diagnostics.termination_reason,
        off_family_switch_count=off_family_switch_count,
        failed_sample_count=failed_sample_count,
        degraded_ratio=degraded_ratio,
        degraded_outside_grace=degraded_outside_grace,
        snapshot_eligible_ratio=snapshot_eligible_ratio,
        sample_count=sample_count,
    )

    return {
        "session_id": result.session_id,
        "capture_date": result.capture_date.isoformat(),
        "verdict": verdict,
        "verdict_reasons": verdict_reasons,
        "verdict_policy": {
            "admissible_max_degraded_ratio": ADMISSIBLE_MAX_DEGRADED_RATIO,
            "conditionally_admissible_max_degraded_ratio": (
                CONDITIONALLY_ADMISSIBLE_MAX_DEGRADED_RATIO
            ),
            "admissible_min_snapshot_eligible_ratio": (
                ADMISSIBLE_MIN_SNAPSHOT_ELIGIBLE_RATIO
            ),
            "conditionally_admissible_min_snapshot_eligible_ratio": (
                CONDITIONALLY_ADMISSIBLE_MIN_SNAPSHOT_ELIGIBLE_RATIO
            ),
            "admissible_max_outside_grace_degraded_samples": (
                ADMISSIBLE_MAX_OUTSIDE_GRACE_DEGRADED_SAMPLES
            ),
            "conditionally_admissible_max_outside_grace_degraded_samples": (
                CONDITIONALLY_ADMISSIBLE_MAX_OUTSIDE_GRACE_DEGRADED_SAMPLES
            ),
        },
        "sample_counts": {
            "total_samples": sample_count,
            "healthy_samples": healthy_sample_count,
            "degraded_samples": degraded_sample_count,
            "failed_samples": failed_sample_count,
        },
        "family_validation": {
            "family_validation_counts": dict(sorted(family_validation_counts.items())),
            "selected_family_compliance_count": family_compliance_count,
            "selected_family_noncompliance_count": sample_count - family_compliance_count,
            "off_family_switch_count": off_family_switch_count,
        },
        "selector_diagnostics": {
            "metadata_refresh_attempts": metadata_refresh_attempts,
            "selector_rebind_count": selector_rebind_count,
            "selector_candidate_count": result.selector_diagnostics.candidate_count,
            "selector_admitted_count": result.selector_diagnostics.admitted_count,
            "selector_rejected_count_by_reason": dict(
                sorted(result.selector_diagnostics.rejected_count_by_reason.items())
            ),
            "target_family_metadata_candidate_row_count": (
                metadata_strip.target_family_candidate_row_count
            ),
            "target_family_metadata_unique_market_count": (
                metadata_strip.target_family_unique_market_count
            ),
            "target_family_metadata_window_count": metadata_strip.target_family_window_count,
            "selector_ambiguity_window_count": metadata_strip.ambiguous_window_count,
            "selector_ambiguity_resolved_window_count": (
                metadata_strip.ambiguous_but_resolved_window_count
            ),
        },
        "polymarket_continuity": {
            "failure_count_by_class": dict(
                sorted(result.session_diagnostics.polymarket_failure_count_by_class.items())
            ),
            "degraded_samples_inside_rollover_grace_window": degraded_inside_grace,
            "degraded_samples_outside_rollover_grace_window": degraded_outside_grace,
            "max_consecutive_missing_samples": int(
                result.session_diagnostics.max_consecutive_missing_by_source.get(
                    "polymarket_quotes",
                    0,
                )
            ),
            "rollover_grace_sample_count": (
                result.session_diagnostics.polymarket_rollover_grace_sample_count
            ),
            "samples_with_quote_rows": polymarket_present_count,
        },
        "chainlink_continuity": {
            "samples_with_ticks": chainlink_present_count,
            "samples_missing_ticks": sample_count - chainlink_present_count,
            "oracle_source_count": dict(sorted(chainlink_oracle_source_count.items())),
            "max_consecutive_missing_samples": int(
                result.session_diagnostics.max_consecutive_missing_by_source.get("chainlink", 0)
            ),
        },
        "exchange_continuity": {
            "samples_with_all_venues": samples_with_all_exchange_venues,
            "samples_missing_any_venue": sample_count - samples_with_all_exchange_venues,
            "venue_present_count": {
                venue: int(exchange_present_count.get(venue, 0)) for venue in EXCHANGE_VENUES
            },
            "venue_missing_count": {
                venue: int(exchange_missing_count.get(venue, 0)) for venue in EXCHANGE_VENUES
            },
            "venue_max_consecutive_missing": {
                venue: int(exchange_max_consecutive_missing.get(venue, 0))
                for venue in EXCHANGE_VENUES
            },
        },
        "mapping_and_anchor": {
            "observed_window_count": len(selected_bindings.observed_window_ids),
            "mapped_window_count": len(selected_bindings.records),
            "selected_binding_unresolved_window_count": len(
                selected_bindings.unresolved_window_ids
            ),
            "anchor_assignment_confidence_breakdown": dict(
                sorted(anchor_confidence_breakdown.items())
            ),
        },
        "snapshot_eligibility": {
            "snapshot_eligible_sample_count": snapshot_eligible_sample_count,
            "snapshot_eligible_sample_ratio": snapshot_eligible_ratio,
            "method": (
                "capture-side proxy: family-compliant sample with chainlink, all exchange "
                "venues, polymarket quote row, and non-none anchor confidence for the "
                "selected window"
            ),
        },
        "termination_reason": result.session_diagnostics.termination_reason,
    }


def _classify_admission_verdict(
    *,
    termination_reason: str,
    off_family_switch_count: int,
    failed_sample_count: int,
    degraded_ratio: float,
    degraded_outside_grace: int,
    snapshot_eligible_ratio: float,
    sample_count: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if termination_reason != "completed":
        reasons.append(f"session terminated early: {termination_reason}")
    if off_family_switch_count > 0:
        reasons.append(f"detected {off_family_switch_count} off-family sample(s)")
    if failed_sample_count > 0:
        reasons.append(f"encountered {failed_sample_count} failed sample(s)")
    if sample_count == 0:
        reasons.append("capture produced zero samples")

    if (
        not reasons
        and degraded_ratio <= ADMISSIBLE_MAX_DEGRADED_RATIO
        and degraded_outside_grace <= ADMISSIBLE_MAX_OUTSIDE_GRACE_DEGRADED_SAMPLES
        and snapshot_eligible_ratio >= ADMISSIBLE_MIN_SNAPSHOT_ELIGIBLE_RATIO
    ):
        return "admissible", [
            "family continuity intact and degradation stayed within admission thresholds"
        ]

    if (
        not reasons
        and degraded_ratio <= CONDITIONALLY_ADMISSIBLE_MAX_DEGRADED_RATIO
        and degraded_outside_grace <= CONDITIONALLY_ADMISSIBLE_MAX_OUTSIDE_GRACE_DEGRADED_SAMPLES
        and snapshot_eligible_ratio >= CONDITIONALLY_ADMISSIBLE_MIN_SNAPSHOT_ELIGIBLE_RATIO
    ):
        return "conditionally_admissible", [
            (
                "family continuity held, but degradation/snapshot coverage stayed in "
                "exploratory-only range"
            )
        ]

    if not reasons:
        reasons.append(
            "degradation or snapshot coverage fell below conditional admission thresholds"
        )
    return "not_admissible", reasons


def _sample_is_family_compliant(
    sample_row: dict[str, Any],
) -> bool:
    selected_market_id = sample_row.get("selected_market_id")
    selected_market_slug = sample_row.get("selected_market_slug")
    selected_window_id = sample_row.get("selected_window_id")
    sample_started_at = sample_row.get("sample_started_at")
    if (
        sample_row.get("family_validation_status") != "selected"
        or selected_market_id is None
        or selected_market_slug is None
        or selected_window_id is None
        or sample_started_at is None
    ):
        return False
    if RECURRING_5M_SLUG_PATTERN.fullmatch(str(selected_market_slug).strip().lower()) is None:
        return False
    return str(owning_window_id(parse_utc(str(sample_started_at)))) == str(selected_window_id)


def _resolve_selected_window_bindings(
    sample_rows: list[dict[str, Any]],
    *,
    metadata_rows: list[MarketMetadataCandidate],
    capture_date: Any,
) -> SelectedWindowBindings:
    observed_window_ids = sorted(
        {
            str(row["selected_window_id"])
            for row in sample_rows
            if row.get("selected_window_id") is not None
        }
    )
    schedule_by_window_id = _schedule_by_window_id(
        observed_window_ids=observed_window_ids,
        capture_date=capture_date,
    )
    metadata_by_market_id = _latest_metadata_by_market_id(metadata_rows)

    states_by_window_id: dict[str, dict[tuple[str, str | None], dict[str, Any]]] = {}
    for sample_row in sample_rows:
        selected_window_id = sample_row.get("selected_window_id")
        selected_market_id = sample_row.get("selected_market_id")
        if selected_window_id is None or selected_market_id is None:
            continue
        window_id = str(selected_window_id)
        states_by_window_id.setdefault(window_id, {})[
            (str(selected_market_id), _optional_str(sample_row.get("selected_market_slug")))
        ] = sample_row

    records: list[WindowMarketMappingRecord] = []
    unresolved_window_ids: list[str] = []
    created_ts = datetime.now(UTC)
    for window_id in observed_window_ids:
        window = schedule_by_window_id.get(window_id)
        states = states_by_window_id.get(window_id, {})
        if window is None or len(states) != 1:
            unresolved_window_ids.append(window_id)
            continue
        state = next(iter(states.values()))
        selected_market_id = str(state["selected_market_id"])
        metadata = metadata_by_market_id.get(selected_market_id)
        records.append(
            WindowMarketMappingRecord(
                window_id=window.window_id,
                asset_id=AssetCode.BTC.value,
                window_start_ts=window.window_start_ts,
                window_end_ts=window.window_end_ts,
                polymarket_market_id=selected_market_id,
                polymarket_event_id=metadata.event_id if metadata is not None else None,
                polymarket_slug=_optional_str(state.get("selected_market_slug"))
                or (metadata.market_slug if metadata is not None else None),
                clob_token_id_up=metadata.token_yes_id if metadata is not None else None,
                clob_token_id_down=metadata.token_no_id if metadata is not None else None,
                listing_discovered_ts=metadata.recv_ts if metadata is not None else None,
                market_active_flag=metadata.active_flag if metadata is not None else None,
                market_closed_flag=metadata.closed_flag if metadata is not None else None,
                mapping_status="mapped",
                mapping_confidence=ConfidenceLevel.HIGH.value,
                mapping_method="selected_sample_binding",
                notes=None,
                schema_version="0.1.0",
                normalizer_version="0.1.0",
                mapping_version="0.1.0",
                created_ts=created_ts,
                updated_ts=created_ts,
            )
        )
    return SelectedWindowBindings(
        records=records,
        observed_window_ids=observed_window_ids,
        unresolved_window_ids=unresolved_window_ids,
    )


def _schedule_by_window_id(
    *,
    observed_window_ids: list[str],
    capture_date: Any,
) -> dict[str, Any]:
    schedule_dates = {date.fromisoformat(str(capture_date))}
    for window_id in observed_window_ids:
        window_start_ts = _window_start_from_window_id(window_id)
        if window_start_ts is not None:
            schedule_dates.add(window_start_ts.date())

    schedule_by_window_id: dict[str, Any] = {}
    for schedule_date in sorted(schedule_dates):
        schedule_by_window_id.update(
            {window.window_id: window for window in daily_window_schedule(schedule_date)}
        )
    return schedule_by_window_id


def _window_start_from_window_id(window_id: str) -> datetime | None:
    if not window_id.startswith("btc-5m-"):
        return None
    suffix = window_id.removeprefix("btc-5m-")
    try:
        return datetime.strptime(suffix, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _summarize_metadata_strip(
    metadata_rows: list[MarketMetadataCandidate],
    *,
    resolved_window_ids: set[str],
) -> MetadataStripDiagnostics:
    market_ids_by_window_id: dict[str, set[str]] = {}
    target_family_candidate_row_count = 0
    target_family_unique_market_ids: set[str] = set()
    for candidate in metadata_rows:
        window_id = _target_family_window_id(candidate)
        if window_id is None:
            continue
        target_family_candidate_row_count += 1
        target_family_unique_market_ids.add(candidate.market_id)
        market_ids_by_window_id.setdefault(window_id, set()).add(candidate.market_id)

    ambiguous_window_ids = {
        window_id
        for window_id, market_ids in market_ids_by_window_id.items()
        if len(market_ids) > 1
    }
    return MetadataStripDiagnostics(
        target_family_candidate_row_count=target_family_candidate_row_count,
        target_family_unique_market_count=len(target_family_unique_market_ids),
        target_family_window_count=len(market_ids_by_window_id),
        ambiguous_window_count=len(ambiguous_window_ids),
        ambiguous_but_resolved_window_count=len(ambiguous_window_ids & resolved_window_ids),
    )


def _latest_metadata_by_market_id(
    metadata_rows: list[MarketMetadataCandidate],
) -> dict[str, MarketMetadataCandidate]:
    latest_by_market_id: dict[str, MarketMetadataCandidate] = {}
    for candidate in metadata_rows:
        existing = latest_by_market_id.get(candidate.market_id)
        if existing is None or candidate.recv_ts >= existing.recv_ts:
            latest_by_market_id[candidate.market_id] = candidate
    return latest_by_market_id


def _target_family_window_id(candidate: MarketMetadataCandidate) -> str | None:
    slug = (candidate.market_slug or "").strip().lower()
    if (
        candidate.asset_id != AssetCode.BTC.value
        or RECURRING_5M_SLUG_PATTERN.fullmatch(slug) is None
        or candidate.token_yes_id is None
        or candidate.token_no_id is None
        or candidate.token_yes_id == candidate.token_no_id
    ):
        return None
    match = RECURRING_5M_SLUG_PATTERN.fullmatch(slug)
    if match is None:
        return None
    return str(
        build_window_id(
            AssetCode.BTC,
            datetime.fromtimestamp(int(match.group("window_start_epoch")), tz=UTC),
        )
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _collector_path(result: Phase1CaptureResult, collector_name: str) -> Path:
    for collector in result.collectors:
        if collector.collector_name == collector_name:
            return collector.normalized_path
    raise KeyError(f"missing collector artifact: {collector_name}")


def _source_result(source_results: dict[str, Any], source_name: str) -> dict[str, Any]:
    payload = source_results.get(source_name, {})
    return payload if isinstance(payload, dict) else {}


def _details(source_result: dict[str, Any]) -> dict[str, Any]:
    payload = source_result.get("details", {})
    return payload if isinstance(payload, dict) else {}


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        payload = json.loads(candidate)
        if not isinstance(payload, dict):
            raise ValueError(f"expected JSON object row in {path}")
        rows.append(payload)
    return rows


def _load_metadata_candidates(path: Path) -> list[Any]:
    return [_row_to_metadata_candidate(row) for row in _read_jsonl_rows(path)]


def _load_chainlink_ticks(path: Path) -> list[Any]:
    return [_row_to_chainlink_tick(row) for row in _read_jsonl_rows(path)]
