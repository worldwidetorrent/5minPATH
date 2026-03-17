from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from rtds.collectors.admission_summary import (
    _finalize_window_summary,
    build_capture_admission_summary,
    resolve_selected_window_bindings,
)
from rtds.collectors.phase1_capture import (
    CollectorArtifactSet,
    MetadataSelectionDiagnostics,
    Phase1CaptureResult,
    SampleDiagnostics,
    SessionDiagnostics,
    SourceCaptureResult,
)
from rtds.collectors.polymarket.metadata import MarketMetadataCandidate
from rtds.collectors.window_quality import load_window_quality_classifier_policy
from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.storage.writer import write_json_file, write_jsonl_rows


def test_build_capture_admission_summary_reports_conditional_admission(tmp_path: Path) -> None:
    result = _capture_result(tmp_path)

    summary = build_capture_admission_summary(result)

    assert summary["verdict"] == "conditionally_admissible"
    assert summary["sample_counts"] == {
        "total_samples": 4,
        "healthy_samples": 3,
        "degraded_samples": 1,
        "failed_samples": 0,
    }
    assert summary["family_validation"]["selected_family_compliance_count"] == 4
    assert summary["family_validation"]["off_family_switch_count"] == 0
    assert summary["polymarket_continuity"]["degraded_samples_inside_rollover_grace_window"] == 1
    assert summary["polymarket_continuity"]["degraded_samples_outside_rollover_grace_window"] == 0
    assert summary["mapping_and_anchor"]["mapped_window_count"] == 3
    assert summary["mapping_and_anchor"]["selected_binding_unresolved_window_count"] == 0
    assert summary["mapping_and_anchor"]["anchor_assignment_confidence_breakdown"] == {
        "high": 3
    }
    assert summary["snapshot_eligibility"]["snapshot_eligible_sample_count"] == 3
    assert summary["polymarket_continuity"]["empty_book_count_by_window"] == {
        "btc-5m-20260315T133000Z": 1
    }
    assert summary["polymarket_continuity"]["window_verdict_counts"] == {
        "degraded_heavy": 1,
        "good": 2,
    }


def test_build_capture_admission_summary_flags_off_family_switches(tmp_path: Path) -> None:
    result = _capture_result(tmp_path, off_family_last_sample=True)

    summary = build_capture_admission_summary(result)

    assert summary["verdict"] == "not_admissible"
    assert summary["family_validation"]["off_family_switch_count"] == 1


def test_build_capture_admission_summary_keeps_refresh_duplicates_out_of_off_family(
    tmp_path: Path,
) -> None:
    result = _capture_result(tmp_path, metadata_conflict_same_window=True)

    summary = build_capture_admission_summary(result)

    assert summary["verdict"] == "conditionally_admissible"
    assert summary["family_validation"]["off_family_switch_count"] == 0
    assert summary["selector_diagnostics"]["selector_ambiguity_window_count"] == 1
    assert summary["selector_diagnostics"]["selector_ambiguity_resolved_window_count"] == 1
    assert summary["mapping_and_anchor"]["mapped_window_count"] == 3


def test_build_capture_admission_summary_maps_windows_across_utc_midnight(
    tmp_path: Path,
) -> None:
    capture_date = date(2026, 3, 15)
    metadata_path = tmp_path / "data" / "normalized" / "market_metadata_events" / "part-00000.jsonl"
    chainlink_path = tmp_path / "data" / "normalized" / "chainlink_ticks" / "part-00000.jsonl"
    sample_diagnostics_path = tmp_path / "artifacts" / "collect" / "sample_diagnostics.jsonl"
    summary_path = tmp_path / "artifacts" / "collect" / "summary.json"

    markets = [
        _metadata_candidate(
            market_id="0x" + "1" * 64,
            slug="btc-updown-5m-1773618900",
            start_ts=datetime(2026, 3, 15, 23, 55, tzinfo=UTC),
        ),
        _metadata_candidate(
            market_id="0x" + "2" * 64,
            slug="btc-updown-5m-1773619200",
            start_ts=datetime(2026, 3, 16, 0, 0, tzinfo=UTC),
        ),
        _metadata_candidate(
            market_id="0x" + "3" * 64,
            slug="btc-updown-5m-1773619500",
            start_ts=datetime(2026, 3, 16, 0, 5, tzinfo=UTC),
        ),
        _metadata_candidate(
            market_id="0x" + "4" * 64,
            slug="btc-updown-5m-1773619800",
            start_ts=datetime(2026, 3, 16, 0, 10, tzinfo=UTC),
        ),
    ]
    write_jsonl_rows(metadata_path, markets)
    write_jsonl_rows(
        chainlink_path,
        [
            ChainlinkTick(
                event_id="cl:2355",
                event_ts=datetime(2026, 3, 15, 23, 55, 1, tzinfo=UTC),
                price=Decimal("84000.00"),
                recv_ts=datetime(2026, 3, 15, 23, 55, 1, tzinfo=UTC),
            ),
            ChainlinkTick(
                event_id="cl:0000",
                event_ts=datetime(2026, 3, 16, 0, 0, 1, tzinfo=UTC),
                price=Decimal("84010.00"),
                recv_ts=datetime(2026, 3, 16, 0, 0, 1, tzinfo=UTC),
            ),
            ChainlinkTick(
                event_id="cl:0005",
                event_ts=datetime(2026, 3, 16, 0, 5, 1, tzinfo=UTC),
                price=Decimal("84020.00"),
                recv_ts=datetime(2026, 3, 16, 0, 5, 1, tzinfo=UTC),
            ),
            ChainlinkTick(
                event_id="cl:0010",
                event_ts=datetime(2026, 3, 16, 0, 10, 1, tzinfo=UTC),
                price=Decimal("84030.00"),
                recv_ts=datetime(2026, 3, 16, 0, 10, 1, tzinfo=UTC),
            ),
        ],
    )
    write_jsonl_rows(
        sample_diagnostics_path,
        [
            _sample(
                index=1,
                started_at=datetime(2026, 3, 15, 23, 55, 30, tzinfo=UTC),
                market_id=markets[0].market_id,
                slug=markets[0].market_slug,
                window_id="btc-5m-20260315T235500Z",
            ),
            _sample(
                index=2,
                started_at=datetime(2026, 3, 16, 0, 0, 30, tzinfo=UTC),
                market_id=markets[1].market_id,
                slug=markets[1].market_slug,
                window_id="btc-5m-20260316T000000Z",
            ),
            _sample(
                index=3,
                started_at=datetime(2026, 3, 16, 0, 5, 30, tzinfo=UTC),
                market_id=markets[2].market_id,
                slug=markets[2].market_slug,
                window_id="btc-5m-20260316T000500Z",
            ),
            _sample(
                index=4,
                started_at=datetime(2026, 3, 16, 0, 10, 30, tzinfo=UTC),
                market_id=markets[3].market_id,
                slug=markets[3].market_slug,
                window_id="btc-5m-20260316T001000Z",
            ),
        ],
    )
    write_json_file(summary_path, {"session_id": "test-session"})

    result = Phase1CaptureResult(
        session_id="test-session",
        capture_date=capture_date,
        selected_market_id=markets[0].market_id,
        selected_market_slug=markets[0].market_slug,
        selected_market_question=markets[0].market_question,
        selected_window_id="btc-5m-20260315T235500Z",
        selector_diagnostics=MetadataSelectionDiagnostics(
            selected_market_id=markets[0].market_id,
            selected_market_slug=markets[0].market_slug,
            selected_window_id="btc-5m-20260315T235500Z",
            candidate_count=4,
            admitted_count=4,
            rejected_count_by_reason={},
        ),
        duration_seconds=600.0,
        poll_interval_seconds=1.0,
        sample_count=4,
        session_diagnostics=SessionDiagnostics(
            degraded_sample_count=0,
            failed_sample_count=0,
            empty_book_count=0,
            retry_count_by_source={},
            retry_exhaustion_count_by_source={},
            source_failure_count_by_source={},
            max_consecutive_missing_by_source={"chainlink": 0, "polymarket_quotes": 0},
            polymarket_failure_count_by_class={},
            polymarket_selector_refresh_count=0,
            polymarket_selector_rebind_count=0,
            polymarket_rollover_grace_sample_count=0,
            termination_reason="completed",
            sample_diagnostics_path=sample_diagnostics_path,
        ),
        summary_path=summary_path,
        collectors=(
            CollectorArtifactSet(
                collector_name="polymarket_metadata",
                raw_path=tmp_path / "data" / "raw" / "polymarket_metadata" / "part-00000.jsonl",
                normalized_path=metadata_path,
                raw_row_count=4,
                normalized_row_count=4,
            ),
            CollectorArtifactSet(
                collector_name="chainlink",
                raw_path=tmp_path / "data" / "raw" / "chainlink" / "part-00000.jsonl",
                normalized_path=chainlink_path,
                raw_row_count=4,
                normalized_row_count=4,
            ),
            CollectorArtifactSet(
                collector_name="exchange",
                raw_path=tmp_path / "data" / "raw" / "exchange" / "part-00000.jsonl",
                normalized_path=tmp_path
                / "data"
                / "normalized"
                / "exchange_quotes"
                / "part-00000.jsonl",
                raw_row_count=12,
                normalized_row_count=12,
            ),
            CollectorArtifactSet(
                collector_name="polymarket_quotes",
                raw_path=tmp_path / "data" / "raw" / "polymarket_quotes" / "part-00000.jsonl",
                normalized_path=tmp_path
                / "data"
                / "normalized"
                / "polymarket_quotes"
                / "part-00000.jsonl",
                raw_row_count=4,
                normalized_row_count=4,
            ),
        ),
    )

    summary = build_capture_admission_summary(result)

    assert summary["mapping_and_anchor"]["mapped_window_count"] == 4
    assert summary["mapping_and_anchor"]["selected_binding_unresolved_window_count"] == 0
    assert summary["mapping_and_anchor"]["anchor_assignment_confidence_breakdown"] == {
        "high": 4
    }


def test_build_capture_admission_summary_preserves_public_stream_boundary_validation_baseline(
    tmp_path: Path,
) -> None:
    capture_date = date(2026, 3, 15)
    metadata_path = tmp_path / "data" / "normalized" / "market_metadata_events" / "part-00000.jsonl"
    chainlink_path = tmp_path / "data" / "normalized" / "chainlink_ticks" / "part-00000.jsonl"
    sample_diagnostics_path = tmp_path / "artifacts" / "collect" / "sample_diagnostics.jsonl"
    summary_path = tmp_path / "artifacts" / "collect" / "summary.json"

    markets = [
        _metadata_candidate(
            market_id="0x" + "a" * 64,
            slug="btc-updown-5m-1773618900",
            start_ts=datetime(2026, 3, 15, 23, 55, tzinfo=UTC),
        ),
        _metadata_candidate(
            market_id="0x" + "b" * 64,
            slug="btc-updown-5m-1773619200",
            start_ts=datetime(2026, 3, 16, 0, 0, tzinfo=UTC),
        ),
        _metadata_candidate(
            market_id="0x" + "c" * 64,
            slug="btc-updown-5m-1773619500",
            start_ts=datetime(2026, 3, 16, 0, 5, tzinfo=UTC),
        ),
        _metadata_candidate(
            market_id="0x" + "d" * 64,
            slug="btc-updown-5m-1773619800",
            start_ts=datetime(2026, 3, 16, 0, 10, tzinfo=UTC),
        ),
    ]
    write_jsonl_rows(metadata_path, markets)
    write_jsonl_rows(
        chainlink_path,
        [
            ChainlinkTick(
                event_id="cl:235509",
                event_ts=datetime(2026, 3, 15, 23, 55, 11, tzinfo=UTC),
                price=Decimal("84000.00"),
                recv_ts=datetime(2026, 3, 15, 23, 55, 11, tzinfo=UTC),
                oracle_source="chainlink_stream_public_delayed",
            ),
            ChainlinkTick(
                event_id="cl:000001",
                event_ts=datetime(2026, 3, 16, 0, 0, 1, tzinfo=UTC),
                price=Decimal("84010.00"),
                recv_ts=datetime(2026, 3, 16, 0, 0, 1, tzinfo=UTC),
                oracle_source="chainlink_stream_public_delayed",
            ),
            ChainlinkTick(
                event_id="cl:000502",
                event_ts=datetime(2026, 3, 16, 0, 5, 2, tzinfo=UTC),
                price=Decimal("84020.00"),
                recv_ts=datetime(2026, 3, 16, 0, 5, 2, tzinfo=UTC),
                oracle_source="chainlink_stream_public_delayed",
            ),
            ChainlinkTick(
                event_id="cl:001009",
                event_ts=datetime(2026, 3, 16, 0, 10, 9, tzinfo=UTC),
                price=Decimal("84030.00"),
                recv_ts=datetime(2026, 3, 16, 0, 10, 9, tzinfo=UTC),
                oracle_source="chainlink_stream_public_delayed",
            ),
        ],
    )
    write_jsonl_rows(
        sample_diagnostics_path,
        [
            _sample(
                index=1,
                started_at=datetime(2026, 3, 15, 23, 58, 0, tzinfo=UTC),
                market_id=markets[0].market_id,
                slug=markets[0].market_slug,
                window_id="btc-5m-20260315T235500Z",
                chainlink_oracle_source="chainlink_stream_public_delayed",
            ),
            _sample(
                index=2,
                started_at=datetime(2026, 3, 16, 0, 0, 30, tzinfo=UTC),
                market_id=markets[1].market_id,
                slug=markets[1].market_slug,
                window_id="btc-5m-20260316T000000Z",
                chainlink_oracle_source="chainlink_stream_public_delayed",
            ),
            _sample(
                index=3,
                started_at=datetime(2026, 3, 16, 0, 5, 30, tzinfo=UTC),
                market_id=markets[2].market_id,
                slug=markets[2].market_slug,
                window_id="btc-5m-20260316T000500Z",
                chainlink_oracle_source="chainlink_stream_public_delayed",
            ),
            _sample(
                index=4,
                started_at=datetime(2026, 3, 16, 0, 10, 30, tzinfo=UTC),
                market_id=markets[3].market_id,
                slug=markets[3].market_slug,
                window_id="btc-5m-20260316T001000Z",
                chainlink_oracle_source="chainlink_stream_public_delayed",
            ),
        ],
    )
    write_json_file(summary_path, {"session_id": "test-session"})

    result = Phase1CaptureResult(
        session_id="test-session",
        capture_date=capture_date,
        selected_market_id=markets[0].market_id,
        selected_market_slug=markets[0].market_slug,
        selected_market_question=markets[0].market_question,
        selected_window_id="btc-5m-20260315T235500Z",
        selector_diagnostics=MetadataSelectionDiagnostics(
            selected_market_id=markets[0].market_id,
            selected_market_slug=markets[0].market_slug,
            selected_window_id="btc-5m-20260315T235500Z",
            candidate_count=4,
            admitted_count=4,
            rejected_count_by_reason={},
        ),
        duration_seconds=720.0,
        poll_interval_seconds=1.0,
        sample_count=4,
        session_diagnostics=SessionDiagnostics(
            degraded_sample_count=0,
            failed_sample_count=0,
            empty_book_count=0,
            retry_count_by_source={},
            retry_exhaustion_count_by_source={},
            source_failure_count_by_source={},
            max_consecutive_missing_by_source={"chainlink": 0, "polymarket_quotes": 0},
            polymarket_failure_count_by_class={},
            polymarket_selector_refresh_count=0,
            polymarket_selector_rebind_count=0,
            polymarket_rollover_grace_sample_count=0,
            termination_reason="completed",
            sample_diagnostics_path=sample_diagnostics_path,
        ),
        summary_path=summary_path,
        collectors=(
            CollectorArtifactSet(
                collector_name="polymarket_metadata",
                raw_path=tmp_path / "data" / "raw" / "polymarket_metadata" / "part-00000.jsonl",
                normalized_path=metadata_path,
                raw_row_count=4,
                normalized_row_count=4,
            ),
            CollectorArtifactSet(
                collector_name="chainlink",
                raw_path=tmp_path / "data" / "raw" / "chainlink" / "part-00000.jsonl",
                normalized_path=chainlink_path,
                raw_row_count=4,
                normalized_row_count=4,
            ),
            CollectorArtifactSet(
                collector_name="exchange",
                raw_path=tmp_path / "data" / "raw" / "exchange" / "part-00000.jsonl",
                normalized_path=tmp_path
                / "data"
                / "normalized"
                / "exchange_quotes"
                / "part-00000.jsonl",
                raw_row_count=12,
                normalized_row_count=12,
            ),
            CollectorArtifactSet(
                collector_name="polymarket_quotes",
                raw_path=tmp_path / "data" / "raw" / "polymarket_quotes" / "part-00000.jsonl",
                normalized_path=tmp_path
                / "data"
                / "normalized"
                / "polymarket_quotes"
                / "part-00000.jsonl",
                raw_row_count=4,
                normalized_row_count=4,
            ),
        ),
    )

    summary = build_capture_admission_summary(result)

    assert summary["verdict"] == "conditionally_admissible"
    assert summary["family_validation"]["off_family_switch_count"] == 0
    assert summary["mapping_and_anchor"]["mapped_window_count"] == 4
    assert summary["mapping_and_anchor"]["selected_binding_unresolved_window_count"] == 0
    assert summary["mapping_and_anchor"]["anchor_assignment_confidence_breakdown"] == {
        "high": 1,
        "low": 1,
        "medium": 1,
        "none": 1,
    }
    assert summary["chainlink_continuity"]["oracle_source_count"] == {
        "chainlink_stream_public_delayed": 4
    }
    assert summary["snapshot_eligibility"]["snapshot_eligible_sample_count"] == 3
    assert summary["snapshot_eligibility"]["snapshot_eligible_sample_ratio"] == 0.75
    assert summary["polymarket_continuity"]["window_verdict_counts"] == {"good": 4}
    assert summary["polymarket_continuity"]["window_quote_coverage"][0]["window_verdict"] == "good"


def test_finalize_window_summary_splits_degraded_windows_into_sub_buckets() -> None:
    classifier_policy = load_window_quality_classifier_policy()
    light = _finalize_window_summary(
        _window_summary_stub(
            total_samples=10,
            samples_with_quote_rows=9,
            valid_empty_book_samples=1,
            degraded_samples_outside_rollover_grace_window=0,
            max_consecutive_valid_empty_book=1,
            snapshot_eligible_samples=9,
        ),
        unusable_min_quote_coverage_ratio=0.2,
        classifier_policy=classifier_policy,
    )
    medium = _finalize_window_summary(
        _window_summary_stub(
            total_samples=10,
            samples_with_quote_rows=9,
            valid_empty_book_samples=1,
            degraded_samples_outside_rollover_grace_window=2,
            max_consecutive_valid_empty_book=4,
            snapshot_eligible_samples=8,
        ),
        unusable_min_quote_coverage_ratio=0.2,
        classifier_policy=classifier_policy,
    )
    heavy = _finalize_window_summary(
        _window_summary_stub(
            total_samples=10,
            samples_with_quote_rows=7,
            valid_empty_book_samples=3,
            degraded_samples_outside_rollover_grace_window=5,
            max_consecutive_valid_empty_book=10,
            snapshot_eligible_samples=6,
        ),
        unusable_min_quote_coverage_ratio=0.2,
        classifier_policy=classifier_policy,
    )

    assert light["window_verdict"] == "degraded_light"
    assert medium["window_verdict"] == "degraded_medium"
    assert heavy["window_verdict"] == "degraded_heavy"
    assert light["snapshot_eligible_ratio"] == 0.9
    assert medium["quote_coverage_ratio"] == 0.9


def test_capture_admission_emits_versioned_window_quality_classifier(tmp_path: Path) -> None:
    summary = build_capture_admission_summary(_capture_result(tmp_path))

    classifier = summary["polymarket_continuity"]["window_quality_classifier"]

    assert classifier["classifier_version"] == "window_quality_v1"
    assert classifier["config_path"] == "configs/replay/window_quality_classifier_v1.json"
    assert classifier["label_order"] == [
        "good",
        "degraded_light",
        "degraded_medium",
        "degraded_heavy",
        "unusable",
    ]


def test_resolve_selected_window_bindings_uses_final_sample_state_per_window(
    tmp_path: Path,
) -> None:
    result = _capture_result(tmp_path)
    metadata_path = next(
        collector.normalized_path
        for collector in result.collectors
        if collector.collector_name == "polymarket_metadata"
    )

    bindings = resolve_selected_window_bindings(
        capture_date=result.capture_date,
        sample_diagnostics_path=result.session_diagnostics.sample_diagnostics_path,
        metadata_path=metadata_path,
    )

    assert len(bindings.records) == 3
    assert bindings.unresolved_window_ids == []
    assert [record.window_id for record in bindings.records] == [
        "btc-5m-20260315T133000Z",
        "btc-5m-20260315T133500Z",
        "btc-5m-20260315T134000Z",
    ]


def _capture_result(
    tmp_path: Path,
    *,
    off_family_last_sample: bool = False,
    metadata_conflict_same_window: bool = False,
) -> Phase1CaptureResult:
    capture_date = date(2026, 3, 15)
    metadata_path = tmp_path / "data" / "normalized" / "market_metadata_events" / "part-00000.jsonl"
    chainlink_path = tmp_path / "data" / "normalized" / "chainlink_ticks" / "part-00000.jsonl"
    sample_diagnostics_path = tmp_path / "artifacts" / "collect" / "sample_diagnostics.jsonl"
    summary_path = tmp_path / "artifacts" / "collect" / "summary.json"

    market_one = _metadata_candidate(
        market_id="0x" + "1" * 64,
        slug="btc-updown-5m-1773581400",
        start_ts=datetime(2026, 3, 15, 13, 30, tzinfo=UTC),
    )
    market_two = _metadata_candidate(
        market_id="0x" + "2" * 64,
        slug="btc-updown-5m-1773581700",
        start_ts=datetime(2026, 3, 15, 13, 35, tzinfo=UTC),
    )
    market_three = _metadata_candidate(
        market_id="0x" + "3" * 64,
        slug="btc-updown-5m-1773582000",
        start_ts=datetime(2026, 3, 15, 13, 40, tzinfo=UTC),
    )
    metadata_rows = [market_one, market_two, market_three]
    if metadata_conflict_same_window:
        metadata_rows.extend(
            [
                _metadata_candidate(
                    market_id="0x" + "9" * 64,
                    slug=market_two.market_slug,
                    start_ts=datetime(2026, 3, 15, 13, 35, tzinfo=UTC),
                ),
                market_two,
            ]
        )
    write_jsonl_rows(metadata_path, metadata_rows)
    write_jsonl_rows(
        chainlink_path,
        [
            ChainlinkTick(
                event_id="cl:1",
                event_ts=datetime(2026, 3, 15, 13, 30, tzinfo=UTC),
                price=Decimal("84000.00"),
                recv_ts=datetime(2026, 3, 15, 13, 30, tzinfo=UTC),
            ),
            ChainlinkTick(
                event_id="cl:2",
                event_ts=datetime(2026, 3, 15, 13, 35, tzinfo=UTC),
                price=Decimal("84010.00"),
                recv_ts=datetime(2026, 3, 15, 13, 35, tzinfo=UTC),
            ),
            ChainlinkTick(
                event_id="cl:3",
                event_ts=datetime(2026, 3, 15, 13, 40, tzinfo=UTC),
                price=Decimal("84020.00"),
                recv_ts=datetime(2026, 3, 15, 13, 40, tzinfo=UTC),
            ),
        ],
    )

    last_sample_slug = (
        "btc-updown-15m-1773581700" if off_family_last_sample else market_two.market_slug
    )
    last_sample_market_id = "0x" + "f" * 64 if off_family_last_sample else market_two.market_id
    write_jsonl_rows(
        sample_diagnostics_path,
        [
            _sample(
                index=1,
                started_at=datetime(2026, 3, 15, 13, 33, 58, tzinfo=UTC),
                market_id=market_one.market_id,
                slug=market_one.market_slug,
                window_id="btc-5m-20260315T133000Z",
            ),
            _sample(
                index=2,
                started_at=datetime(2026, 3, 15, 13, 34, 59, tzinfo=UTC),
                market_id=market_one.market_id,
                slug=market_one.market_slug,
                window_id="btc-5m-20260315T133000Z",
                degraded=True,
                within_grace=True,
                refresh_attempted=True,
                polymarket_rows=0,
            ),
            _sample(
                index=3,
                started_at=datetime(2026, 3, 15, 13, 36, 0, tzinfo=UTC),
                market_id=last_sample_market_id,
                slug=last_sample_slug,
                window_id="btc-5m-20260315T133500Z",
            ),
            _sample(
                index=4,
                started_at=datetime(2026, 3, 15, 13, 41, 0, tzinfo=UTC),
                market_id=market_three.market_id,
                slug=market_three.market_slug,
                window_id="btc-5m-20260315T134000Z",
            ),
        ],
    )
    write_json_file(summary_path, {"session_id": "test-session"})

    return Phase1CaptureResult(
        session_id="test-session",
        capture_date=capture_date,
        selected_market_id=market_one.market_id,
        selected_market_slug=market_one.market_slug,
        selected_market_question=market_one.market_question,
        selected_window_id="btc-5m-20260315T133000Z",
        selector_diagnostics=MetadataSelectionDiagnostics(
            selected_market_id=market_one.market_id,
            selected_market_slug=market_one.market_slug,
            selected_window_id="btc-5m-20260315T133000Z",
            candidate_count=3,
            admitted_count=3,
            rejected_count_by_reason={},
        ),
        duration_seconds=180.0,
        poll_interval_seconds=60.0,
        sample_count=4,
        session_diagnostics=SessionDiagnostics(
            degraded_sample_count=1,
            failed_sample_count=0,
            empty_book_count=1,
            retry_count_by_source={},
            retry_exhaustion_count_by_source={},
            source_failure_count_by_source={"polymarket_quotes": 1},
            max_consecutive_missing_by_source={
                "chainlink": 0,
                "polymarket_quotes": 1,
            },
            polymarket_failure_count_by_class={"degraded_empty_book": 1},
            polymarket_selector_refresh_count=1,
            polymarket_selector_rebind_count=0,
            polymarket_rollover_grace_sample_count=1,
            termination_reason="completed",
            sample_diagnostics_path=sample_diagnostics_path,
        ),
        summary_path=summary_path,
        collectors=(
            CollectorArtifactSet(
                collector_name="polymarket_metadata",
                raw_path=tmp_path / "data" / "raw" / "polymarket_metadata" / "part-00000.jsonl",
                normalized_path=metadata_path,
                raw_row_count=1,
                normalized_row_count=3,
            ),
            CollectorArtifactSet(
                collector_name="chainlink",
                raw_path=tmp_path / "data" / "raw" / "chainlink" / "part-00000.jsonl",
                normalized_path=chainlink_path,
                raw_row_count=3,
                normalized_row_count=3,
            ),
            CollectorArtifactSet(
                collector_name="exchange",
                raw_path=tmp_path / "data" / "raw" / "exchange" / "part-00000.jsonl",
                normalized_path=tmp_path
                / "data"
                / "normalized"
                / "exchange_quotes"
                / "part-00000.jsonl",
                raw_row_count=9,
                normalized_row_count=9,
            ),
            CollectorArtifactSet(
                collector_name="polymarket_quotes",
                raw_path=tmp_path / "data" / "raw" / "polymarket_quotes" / "part-00000.jsonl",
                normalized_path=tmp_path
                / "data"
                / "normalized"
                / "polymarket_quotes"
                / "part-00000.jsonl",
                raw_row_count=4,
                normalized_row_count=3,
            ),
        ),
    )


def _sample(
    *,
    index: int,
    started_at: datetime,
    market_id: str,
    slug: str | None,
    window_id: str,
    chainlink_oracle_source: str = "chainlink_snapshot_rpc",
    degraded: bool = False,
    within_grace: bool = False,
    refresh_attempted: bool = False,
    polymarket_rows: int = 1,
) -> dict[str, object]:
    sample = SampleDiagnostics(
        sample_index=index,
        sample_started_at=started_at,
        sample_status="degraded" if degraded else "healthy",
        selected_market_id=market_id,
        selected_market_slug=slug,
        selected_window_id=window_id,
        family_validation_status="selected",
        degraded_sources=("polymarket_quotes",) if degraded else (),
        source_results={
            "chainlink": SourceCaptureResult(
                source_name="chainlink",
                status="success",
                raw_rows=({"id": f"chainlink:{index}"},),
                normalized_rows=({"id": f"chainlink:{index}"},),
                details={"oracle_source": chainlink_oracle_source},
            ),
            "exchange": SourceCaptureResult(
                source_name="exchange",
                status="success",
                raw_rows=(
                    {"venue_id": "binance"},
                    {"venue_id": "coinbase"},
                    {"venue_id": "kraken"},
                ),
                normalized_rows=(
                    {"venue_id": "binance"},
                    {"venue_id": "coinbase"},
                    {"venue_id": "kraken"},
                ),
                details={
                    "venue_statuses": {
                        "binance": "success",
                        "coinbase": "success",
                        "kraken": "success",
                    }
                },
            ),
            "polymarket_quotes": SourceCaptureResult(
                source_name="polymarket_quotes",
                status="degraded_empty_book" if degraded else "success",
                raw_rows=({"id": f"polymarket:{index}"},),
                normalized_rows=tuple(
                    {"id": f"polymarket:{index}"} for _ in range(polymarket_rows)
                ),
                failure_class="degraded_empty_book" if degraded else None,
                details={
                    "within_rollover_grace_window": within_grace,
                    "metadata_refresh_attempted": refresh_attempted,
                },
            ),
        },
    )
    return sample.to_summary_dict()


def _window_summary_stub(
    *,
    total_samples: int,
    samples_with_quote_rows: int,
    valid_empty_book_samples: int,
    degraded_samples_outside_rollover_grace_window: int,
    max_consecutive_valid_empty_book: int,
    snapshot_eligible_samples: int,
) -> dict[str, object]:
    return {
        "window_id": "btc-5m-20260315T133000Z",
        "selected_market_ids": {"0x" + "1" * 64},
        "selected_market_slugs": {"btc-updown-5m-1773581400"},
        "family_continuity_pass": True,
        "oracle_continuity_pass": True,
        "exchange_continuity_pass": True,
        "total_samples": total_samples,
        "samples_with_quote_rows": samples_with_quote_rows,
        "valid_empty_book_samples": valid_empty_book_samples,
        "quote_unavailable_samples": 0,
        "binding_invalid_samples": 0,
        "degraded_samples_inside_rollover_grace_window": valid_empty_book_samples,
        "degraded_samples_outside_rollover_grace_window": (
            degraded_samples_outside_rollover_grace_window
        ),
        "current_valid_empty_book_streak": 0,
        "current_quote_unavailable_streak": 0,
        "max_consecutive_valid_empty_book": max_consecutive_valid_empty_book,
        "max_consecutive_quote_unavailable": 0,
        "snapshot_eligible_samples": snapshot_eligible_samples,
    }


def _metadata_candidate(
    *,
    market_id: str,
    slug: str,
    start_ts: datetime,
) -> MarketMetadataCandidate:
    return MarketMetadataCandidate(
        venue_id="polymarket",
        market_id=market_id,
        recv_ts=start_ts,
        proc_ts=start_ts,
        raw_event_id=f"raw:{market_id}",
        normalizer_version="0.1.0",
        schema_version="0.1.0",
        created_ts=start_ts,
        event_id=f"event:{market_id}",
        event_ts=start_ts,
        asset_id="BTC",
        market_title="Bitcoin Up or Down",
        market_question="Bitcoin Up or Down",
        market_slug=slug,
        market_status="active",
        market_open_ts=start_ts,
        market_close_ts=start_ts + timedelta(minutes=5),
        active_flag=True,
        closed_flag=False,
        archived_flag=False,
        token_yes_id=f"yes:{market_id}",
        token_no_id=f"no:{market_id}",
        condition_id=market_id,
        gamma_market_id=market_id,
        category="Crypto",
        subcategory="Bitcoin",
    )
