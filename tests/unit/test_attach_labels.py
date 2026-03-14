import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from rtds.collectors.polymarket.metadata import normalize_market_payload
from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import build_exchange_spot_instrument_id
from rtds.core.time import parse_utc
from rtds.features.composite_nowcast import compute_composite_nowcast
from rtds.mapping.anchor_assignment import ChainlinkTick, assign_window_reference
from rtds.mapping.market_mapper import WindowMarketMappingRecord, map_candidates_to_windows
from rtds.mapping.window_ids import daily_window_schedule
from rtds.quality.dispersion import assess_exchange_composite_quality
from rtds.quality.freshness import FreshnessPolicy, assess_source_freshness
from rtds.quality.gap_detection import assess_chainlink_quality
from rtds.replay.attach_labels import attach_label, attach_labels
from rtds.schemas.normalized import SCHEMA_VERSION, ExchangeQuote, PolymarketQuote
from rtds.snapshots.builder import SnapshotBuildInput, build_snapshot_row

RAW_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_metadata"
)
CHAINLINK_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "normalized_events"
    / "chainlink_ticks"
)


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _mapping_record() -> WindowMarketMappingRecord:
    event_payload = _load_json(RAW_FIXTURE_DIR / "btc_5m_event.json")
    market_payload = event_payload["markets"][0]
    candidate = normalize_market_payload(
        market_payload=market_payload,
        event_payload=event_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )
    windows = daily_window_schedule(date(2026, 3, 13))
    batch = map_candidates_to_windows(windows, [candidate])
    return next(record for record in batch.records if record.mapping_status == "mapped")


def _load_ticks(name: str) -> list[ChainlinkTick]:
    payload = _load_json(CHAINLINK_FIXTURE_DIR / name)
    return [
        ChainlinkTick(
            event_id=str(item["event_id"]),
            event_ts=parse_utc(str(item["event_ts"])),
            price=Decimal(str(item["price"])),
        )
        for item in payload
    ]


def _exchange_quote(
    venue: VenueCode,
    *,
    event_ts: datetime,
    mid_price: str,
) -> ExchangeQuote:
    mid = Decimal(mid_price)
    return ExchangeQuote(
        venue_id=venue.value,
        instrument_id=str(build_exchange_spot_instrument_id(venue, AssetCode.BTC)),
        asset_id=AssetCode.BTC.value,
        event_ts=event_ts,
        recv_ts=event_ts,
        proc_ts=event_ts,
        best_bid=mid - Decimal("1"),
        best_ask=mid + Decimal("1"),
        mid_price=mid,
        bid_size=Decimal("1"),
        ask_size=Decimal("1"),
        raw_event_id=f"{venue.value}:{event_ts.isoformat()}",
        normalizer_version="0.1.0",
        schema_version=SCHEMA_VERSION,
        created_ts=event_ts,
    )


def _polymarket_quote(market_id: str) -> PolymarketQuote:
    event_ts = datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC)
    return PolymarketQuote(
        venue_id="polymarket",
        market_id=market_id,
        asset_id="BTC",
        event_ts=event_ts,
        recv_ts=event_ts,
        proc_ts=event_ts,
        up_bid=Decimal("0.54"),
        up_ask=Decimal("0.56"),
        down_bid=Decimal("0.44"),
        down_ask=Decimal("0.46"),
        up_bid_size_contracts=Decimal("250"),
        up_ask_size_contracts=Decimal("300"),
        down_bid_size_contracts=Decimal("275"),
        down_ask_size_contracts=Decimal("290"),
        raw_event_id="poly-quote-1",
        normalizer_version="0.1.0",
        schema_version=SCHEMA_VERSION,
        created_ts=event_ts,
    )


def _snapshot_and_reference() -> tuple[object, object]:
    snapshot_ts = datetime(2026, 3, 13, 12, 5, 2, tzinfo=UTC)
    freshness_policy = FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000)
    window_reference = assign_window_reference(
        _mapping_record(),
        _load_ticks("clean_boundary_ticks.json"),
    )
    exchange_quotes = [
        _exchange_quote(
            VenueCode.BINANCE,
            event_ts=datetime(2026, 3, 13, 12, 5, 1, 200000, tzinfo=UTC),
            mid_price="84010",
        ),
        _exchange_quote(
            VenueCode.COINBASE,
            event_ts=datetime(2026, 3, 13, 12, 5, 1, 400000, tzinfo=UTC),
            mid_price="84012",
        ),
        _exchange_quote(
            VenueCode.KRAKEN,
            event_ts=datetime(2026, 3, 13, 12, 5, 1, 600000, tzinfo=UTC),
            mid_price="84011",
        ),
    ]
    snapshot = build_snapshot_row(
        SnapshotBuildInput(
            window_reference=window_reference,
            snapshot_ts=snapshot_ts,
            chainlink_current_tick=ChainlinkTick(
                event_id="chainlink-live",
                event_ts=datetime(2026, 3, 13, 12, 5, 1, 500000, tzinfo=UTC),
                price=Decimal("84011.25"),
            ),
            composite_nowcast=compute_composite_nowcast(
                exchange_quotes,
                as_of_ts=snapshot_ts,
                freshness_policy=freshness_policy,
                minimum_venue_count=3,
            ),
            exchange_quality=assess_exchange_composite_quality(
                exchange_quotes,
                as_of_ts=snapshot_ts,
                freshness_policy=freshness_policy,
            ),
            polymarket_quote=_polymarket_quote(window_reference.polymarket_market_id or "missing"),
            polymarket_quote_freshness=assess_source_freshness(
                "polymarket",
                as_of_ts=snapshot_ts,
                last_event_ts=datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC),
                policy=freshness_policy,
            ),
            chainlink_quality=assess_chainlink_quality(
                [
                    ChainlinkTick(
                        event_id="chainlink-prior",
                        event_ts=datetime(2026, 3, 13, 12, 5, 0, 500000, tzinfo=UTC),
                        price=Decimal("84010.90"),
                    ),
                    ChainlinkTick(
                        event_id="chainlink-live",
                        event_ts=datetime(2026, 3, 13, 12, 5, 1, 500000, tzinfo=UTC),
                        price=Decimal("84011.25"),
                    ),
                ],
                as_of_ts=snapshot_ts,
            ),
            created_ts=snapshot_ts,
        )
    )
    return snapshot, window_reference


def test_attach_label_attaches_resolved_truth_from_window_reference() -> None:
    snapshot, window_reference = _snapshot_and_reference()

    labeled = attach_label(snapshot, window_reference)

    assert labeled.label.resolved_up is True
    assert labeled.label.chainlink_settle_price == Decimal("84025.50")
    assert labeled.label.settle_minus_open == Decimal("25.40")
    assert labeled.label.realized_direction == "up"
    assert labeled.label.label_status == "attached"
    assert labeled.label.label_quality_flags == ()


def test_attach_label_marks_unresolved_truth_when_settlement_is_missing() -> None:
    snapshot, window_reference = _snapshot_and_reference()
    unresolved_reference = type(window_reference)(
        **{
            **window_reference.to_dict(),
            "chainlink_settle_price": None,
            "chainlink_settle_ts": None,
            "resolved_up": None,
            "settle_minus_open": None,
            "outcome_status": "missing_anchor",
            "assignment_status": "open_and_settle_missing",
        }
    )

    labeled = attach_label(snapshot, unresolved_reference)

    assert labeled.label.resolved_up is None
    assert labeled.label.realized_direction == "unknown"
    assert labeled.label.label_status == "unresolved"
    assert labeled.label.label_quality_flags == (
        "missing_settlement",
        "non_resolved_outcome",
    )


def test_attach_label_rejects_snapshot_reference_mismatch() -> None:
    snapshot, window_reference = _snapshot_and_reference()
    mismatched_snapshot = type(snapshot)(
        **{
            **snapshot.to_dict(),
            "polymarket_market_id": "0xdef456",
            "snapshot_id": None,
        }
    )

    with pytest.raises(ValueError, match="polymarket_market_id"):
        attach_label(mismatched_snapshot, window_reference)


def test_attach_labels_batches_by_window_and_market_identity() -> None:
    snapshot, window_reference = _snapshot_and_reference()

    labeled_rows = attach_labels([snapshot], [window_reference])

    assert len(labeled_rows) == 1
    assert labeled_rows[0].label.snapshot_id == snapshot.snapshot_id
