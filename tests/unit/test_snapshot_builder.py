import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

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
from rtds.schemas.normalized import SCHEMA_VERSION, ExchangeQuote, PolymarketQuote
from rtds.snapshots.assembler import assemble_snapshot_rows
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
        token_yes_id="token-up",
        token_no_id="token-down",
        last_trade_price=Decimal("0.55"),
        last_trade_size_contracts=Decimal("100"),
    )


def test_build_snapshot_row_for_active_window() -> None:
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
    exchange_quality = assess_exchange_composite_quality(
        exchange_quotes,
        as_of_ts=snapshot_ts,
        freshness_policy=freshness_policy,
    )
    composite_nowcast = compute_composite_nowcast(
        exchange_quotes,
        as_of_ts=snapshot_ts,
        freshness_policy=freshness_policy,
        minimum_venue_count=3,
    )

    polymarket_quote = _polymarket_quote(window_reference.polymarket_market_id or "missing")
    polymarket_freshness = assess_source_freshness(
        "polymarket",
        as_of_ts=snapshot_ts,
        last_event_ts=polymarket_quote.event_ts,
        policy=freshness_policy,
    )

    chainlink_tick = ChainlinkTick(
        event_id="chainlink-live",
        event_ts=datetime(2026, 3, 13, 12, 5, 1, 500000, tzinfo=UTC),
        recv_ts=datetime(2026, 3, 13, 12, 5, 1, 600000, tzinfo=UTC),
        price=Decimal("84011.25"),
    )
    chainlink_quality = assess_chainlink_quality(
        [
            ChainlinkTick(
                event_id="chainlink-prior",
                event_ts=datetime(2026, 3, 13, 12, 5, 0, 500000, tzinfo=UTC),
                recv_ts=datetime(2026, 3, 13, 12, 5, 0, 600000, tzinfo=UTC),
                price=Decimal("84010.90"),
            ),
            chainlink_tick,
        ],
        as_of_ts=snapshot_ts,
    )

    snapshot = build_snapshot_row(
        SnapshotBuildInput(
            window_reference=window_reference,
            snapshot_ts=snapshot_ts,
            chainlink_current_tick=chainlink_tick,
            composite_nowcast=composite_nowcast,
            exchange_quality=exchange_quality,
            polymarket_quote=polymarket_quote,
            polymarket_quote_freshness=polymarket_freshness,
            chainlink_quality=chainlink_quality,
            created_ts=snapshot_ts,
        )
    )

    assert snapshot.snapshot_id == (
        f"snap:{window_reference.window_id}:{window_reference.polymarket_market_id}:"
        "20260313T120502000Z"
    )
    assert snapshot.composite_now_price == Decimal("84011")
    assert snapshot.composite_method == "median_3"
    assert snapshot.chainlink_current_price == Decimal("84011.25")
    assert snapshot.chainlink_current_age_ms == 500
    assert snapshot.up_bid == Decimal("0.54")
    assert snapshot.up_ask == Decimal("0.56")
    assert snapshot.snapshot_usable_flag is True
    assert snapshot.exchange_quality_usable_flag is True
    assert snapshot.chainlink_quality_usable_flag is True
    assert snapshot.polymarket_quote_usable_flag is True
    assert snapshot.reference_complete_flag is True
    assert snapshot.quality_diagnostics == ()


def test_build_snapshot_row_marks_missing_polymarket_quote_unusable() -> None:
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
            polymarket_quote=None,
            polymarket_quote_freshness=assess_source_freshness(
                "polymarket",
                as_of_ts=snapshot_ts,
                last_event_ts=None,
                policy=freshness_policy,
            ),
            chainlink_quality=assess_chainlink_quality(
                [
                    ChainlinkTick(
                        event_id="chainlink-live",
                        event_ts=datetime(2026, 3, 13, 12, 5, 1, 500000, tzinfo=UTC),
                        price=Decimal("84011.25"),
                    )
                ],
                as_of_ts=snapshot_ts,
            ),
            created_ts=snapshot_ts,
        )
    )

    assert snapshot.polymarket_quote_usable_flag is False
    assert snapshot.snapshot_usable_flag is False
    assert snapshot.polymarket_quote_event_ts is None
    assert snapshot.up_bid is None
    assert "polymarket_quote_missing" in snapshot.quality_diagnostics
    assert "missing_source" in snapshot.quality_diagnostics


def test_assemble_snapshot_rows_sorts_by_timestamp_then_snapshot_id() -> None:
    window_reference = assign_window_reference(
        _mapping_record(),
        _load_ticks("clean_boundary_ticks.json"),
    )
    first_ts = datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC)
    second_ts = datetime(2026, 3, 13, 12, 5, 2, tzinfo=UTC)
    freshness_policy = FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000)
    exchange_quotes = [
        _exchange_quote(
            VenueCode.BINANCE,
            event_ts=datetime(2026, 3, 13, 12, 5, 0, 800000, tzinfo=UTC),
            mid_price="84010",
        ),
        _exchange_quote(
            VenueCode.COINBASE,
            event_ts=datetime(2026, 3, 13, 12, 5, 0, 850000, tzinfo=UTC),
            mid_price="84011",
        ),
        _exchange_quote(
            VenueCode.KRAKEN,
            event_ts=datetime(2026, 3, 13, 12, 5, 0, 900000, tzinfo=UTC),
            mid_price="84012",
        ),
    ]

    def _input(ts: datetime) -> SnapshotBuildInput:
        return SnapshotBuildInput(
            window_reference=window_reference,
            snapshot_ts=ts,
            chainlink_current_tick=ChainlinkTick(
                event_id=f"chainlink-{ts.second}",
                event_ts=ts,
                price=Decimal("84011.25"),
            ),
            composite_nowcast=compute_composite_nowcast(
                exchange_quotes,
                as_of_ts=ts,
                freshness_policy=freshness_policy,
                minimum_venue_count=3,
            ),
            exchange_quality=assess_exchange_composite_quality(
                exchange_quotes,
                as_of_ts=ts,
                freshness_policy=freshness_policy,
            ),
            polymarket_quote=_polymarket_quote(window_reference.polymarket_market_id or "missing"),
            polymarket_quote_freshness=assess_source_freshness(
                "polymarket",
                as_of_ts=ts,
                last_event_ts=datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC),
                policy=freshness_policy,
            ),
            chainlink_quality=assess_chainlink_quality(
                [
                    ChainlinkTick(
                        event_id=f"chainlink-{ts.second}",
                        event_ts=ts,
                        price=Decimal("84011.25"),
                    )
                ],
                as_of_ts=ts,
            ),
            created_ts=ts,
        )

    rows = assemble_snapshot_rows([_input(second_ts), _input(first_ts)])

    assert [row.snapshot_ts for row in rows] == [first_ts, second_ts]
