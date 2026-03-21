from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from rtds.collectors.polymarket.metadata import MarketMetadataCandidate
from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import build_exchange_spot_instrument_id
from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.replay.loader import (
    load_chainlink_ticks,
    load_exchange_quotes,
    load_metadata_candidates,
    load_polymarket_quotes,
)
from rtds.schemas.normalized import ExchangeQuote, PolymarketQuote
from rtds.storage.writer import serialize_value, write_jsonl_rows


def test_session_scoped_replay_loaders_read_one_session_across_utc_dates(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    trade_date = "2026-03-16"
    next_date = "2026-03-17"
    target_session = "session-a"
    other_session = "session-b"
    event_ts = datetime(2026, 3, 16, 10, 15, tzinfo=UTC)
    next_event_ts = datetime(2026, 3, 17, 0, 0, 1, tzinfo=UTC)

    write_jsonl_rows(
        data_root
        / "normalized"
        / "exchange_quotes"
        / f"date={trade_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                key: serialize_value(value)
                for key, value in asdict(
                    ExchangeQuote(
                    venue_id="binance",
                    instrument_id=str(
                        build_exchange_spot_instrument_id(VenueCode.BINANCE, AssetCode.BTC)
                    ),
                    asset_id="BTC",
                    event_ts=event_ts,
                    recv_ts=event_ts,
                    proc_ts=event_ts,
                    best_bid=Decimal("84000"),
                    best_ask=Decimal("84001"),
                    mid_price=Decimal("84000.5"),
                    bid_size=Decimal("1"),
                    ask_size=Decimal("1"),
                    raw_event_id="target",
                    normalizer_version="0.1.0",
                    schema_version="0.1.0",
                    created_ts=event_ts,
                    )
                ).items()
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "exchange_quotes"
        / f"date={next_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
                {
                    "venue_id": "kraken",
                    "instrument_id": str(
                        build_exchange_spot_instrument_id(VenueCode.KRAKEN, AssetCode.BTC)
                    ),
                "asset_id": "BTC",
                "event_ts": "2026-03-17T00:00:01Z",
                "recv_ts": "2026-03-17T00:00:01Z",
                "proc_ts": "2026-03-17T00:00:01Z",
                "best_bid": "84500",
                "best_ask": "84501",
                "mid_price": "84500.5",
                "bid_size": "1",
                "ask_size": "1",
                "raw_event_id": "target-next",
                "normalizer_version": "0.1.0",
                "schema_version": "0.1.0",
                "created_ts": "2026-03-17T00:00:01Z"
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "exchange_quotes"
        / f"date={trade_date}"
        / f"session={other_session}"
        / "part-00000.jsonl",
        [
            {
                "venue_id": "coinbase",
                "instrument_id": "coinbase_spot_btc_usd",
                "asset_id": "BTC",
                "event_ts": "2026-03-16T10:16:00Z",
                "recv_ts": "2026-03-16T10:16:00Z",
                "proc_ts": "2026-03-16T10:16:00Z",
                "best_bid": "85000",
                "best_ask": "85001",
                "mid_price": "85000.5",
                "bid_size": "1",
                "ask_size": "1",
                "raw_event_id": "other",
                "normalizer_version": "0.1.0",
                "schema_version": "0.1.0",
                "created_ts": "2026-03-16T10:16:00Z"
            }
        ],
    )

    write_jsonl_rows(
        data_root
        / "normalized"
        / "polymarket_quotes"
        / f"date={next_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                "venue_id": "polymarket",
                "market_id": "0x" + "1" * 64,
                "asset_id": "BTC",
                "event_ts": "2026-03-17T00:00:01Z",
                "recv_ts": "2026-03-17T00:00:01Z",
                "proc_ts": "2026-03-17T00:00:01Z",
                "up_bid": "0.53",
                "up_ask": "0.55",
                "down_bid": "0.45",
                "down_ask": "0.47",
                "up_bid_size_contracts": "100",
                "up_ask_size_contracts": "120",
                "down_bid_size_contracts": "110",
                "down_ask_size_contracts": "130",
                "raw_event_id": "poly-target-next",
                "normalizer_version": "0.1.0",
                "schema_version": "0.1.0",
                "created_ts": "2026-03-17T00:00:01Z"
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "polymarket_quotes"
        / f"date={trade_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                key: serialize_value(value)
                for key, value in asdict(
                    PolymarketQuote(
                    venue_id="polymarket",
                    market_id="0x" + "1" * 64,
                    asset_id="BTC",
                    event_ts=event_ts,
                    recv_ts=event_ts,
                    proc_ts=event_ts,
                    up_bid=Decimal("0.52"),
                    up_ask=Decimal("0.54"),
                    down_bid=Decimal("0.46"),
                    down_ask=Decimal("0.48"),
                    up_bid_size_contracts=Decimal("100"),
                    up_ask_size_contracts=Decimal("120"),
                    down_bid_size_contracts=Decimal("110"),
                    down_ask_size_contracts=Decimal("130"),
                    raw_event_id="poly-target",
                    normalizer_version="0.1.0",
                    schema_version="0.1.0",
                    created_ts=event_ts,
                    )
                ).items()
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "chainlink_ticks"
        / f"date={next_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                key: serialize_value(value)
                for key, value in asdict(
                    ChainlinkTick(
                        event_id="cl-target-next",
                        event_ts=next_event_ts,
                        price=Decimal("84500"),
                        recv_ts=next_event_ts,
                        oracle_source="chainlink_stream_public_delayed",
                    )
                ).items()
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "chainlink_ticks"
        / f"date={trade_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                key: serialize_value(value)
                for key, value in asdict(
                    ChainlinkTick(
                    event_id="cl-target",
                    event_ts=event_ts,
                    price=Decimal("84000"),
                    recv_ts=event_ts,
                    oracle_source="chainlink_stream_public_delayed",
                    )
                ).items()
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "market_metadata_events"
        / f"date={next_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                key: serialize_value(value)
                for key, value in asdict(
                    MarketMetadataCandidate(
                        venue_id="polymarket",
                        market_id="0x" + "2" * 64,
                        recv_ts=next_event_ts,
                        proc_ts=next_event_ts,
                        raw_event_id="meta-target-next",
                        normalizer_version="0.1.0",
                        schema_version="0.1.0",
                        created_ts=next_event_ts,
                        event_id="evt-2",
                        event_ts=next_event_ts,
                        asset_id="BTC",
                        market_slug="btc-updown-5m-1773705600",
                        market_question="Bitcoin Up or Down",
                        market_open_ts=datetime(2026, 3, 17, 0, 0, tzinfo=UTC),
                        market_close_ts=datetime(2026, 3, 17, 0, 5, tzinfo=UTC),
                        token_yes_id="yes-next",
                        token_no_id="no-next",
                        market_status="open",
                    )
                ).items()
            }
        ],
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "market_metadata_events"
        / f"date={trade_date}"
        / f"session={target_session}"
        / "part-00000.jsonl",
        [
            {
                key: serialize_value(value)
                for key, value in asdict(
                    MarketMetadataCandidate(
                    venue_id="polymarket",
                    market_id="0x" + "1" * 64,
                    recv_ts=event_ts,
                    proc_ts=event_ts,
                    raw_event_id="meta-target",
                    normalizer_version="0.1.0",
                    schema_version="0.1.0",
                    created_ts=event_ts,
                    event_id="evt-1",
                    event_ts=event_ts,
                    asset_id="BTC",
                    market_slug="btc-updown-5m-1773656100",
                    market_question="Bitcoin Up or Down",
                    market_open_ts=datetime(2026, 3, 16, 10, 15, tzinfo=UTC),
                    market_close_ts=datetime(2026, 3, 16, 10, 20, tzinfo=UTC),
                    token_yes_id="yes",
                    token_no_id="no",
                    market_status="open",
                    )
                ).items()
            }
        ],
    )

    exchange_quotes = load_exchange_quotes(
        data_root,
        date_utc=trade_date,
        session_id=target_session,
    )
    polymarket_quotes = load_polymarket_quotes(
        data_root,
        date_utc=trade_date,
        session_id=target_session,
    )
    chainlink_ticks = load_chainlink_ticks(
        data_root,
        date_utc=trade_date,
        session_id=target_session,
    )
    metadata_rows = load_metadata_candidates(
        data_root,
        date_utc=trade_date,
        session_id=target_session,
    )

    assert [quote.raw_event_id for quote in exchange_quotes] == [
        "target",
        "target-next",
    ]
    assert [quote.raw_event_id for quote in polymarket_quotes] == [
        "poly-target",
        "poly-target-next",
    ]
    assert [tick.event_id for tick in chainlink_ticks] == [
        "cl-target",
        "cl-target-next",
    ]
    assert [row.market_slug for row in metadata_rows] == [
        "btc-updown-5m-1773656100",
        "btc-updown-5m-1773705600",
    ]
