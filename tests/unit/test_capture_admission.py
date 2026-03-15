from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from rtds.collectors.admission_summary import build_capture_admission_summary
from rtds.collectors.phase1_capture import (
    CollectorArtifactSet,
    MetadataSelectionDiagnostics,
    Phase1CaptureResult,
    SampleDiagnostics,
    SessionDiagnostics,
    SourceCaptureResult,
)
from rtds.collectors.polymarket.metadata import MarketMetadataCandidate
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
    assert summary["mapping_and_anchor"]["anchor_assignment_confidence_breakdown"] == {
        "high": 3
    }
    assert summary["snapshot_eligibility"]["snapshot_eligible_sample_count"] == 3


def test_build_capture_admission_summary_flags_off_family_switches(tmp_path: Path) -> None:
    result = _capture_result(tmp_path, off_family_last_sample=True)

    summary = build_capture_admission_summary(result)

    assert summary["verdict"] == "not_admissible"
    assert summary["family_validation"]["off_family_switch_count"] == 1


def _capture_result(tmp_path: Path, *, off_family_last_sample: bool = False) -> Phase1CaptureResult:
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
    write_jsonl_rows(metadata_path, [market_one, market_two, market_three])
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
