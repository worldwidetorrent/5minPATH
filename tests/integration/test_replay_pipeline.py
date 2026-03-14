from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from rtds.cli.replay_day import main as replay_day_main
from rtds.collectors.polymarket.metadata import normalize_market_payload
from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import build_exchange_spot_instrument_id
from rtds.schemas.normalized import ExchangeQuote, PolymarketQuote
from rtds.storage.writer import serialize_value, write_jsonl_rows

RAW_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_metadata"
)


def test_replay_day_cli_writes_canonical_run_artifacts(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "artifacts"
    trade_date = "2026-03-13"

    _write_metadata_candidates(data_root)
    _write_chainlink_ticks(data_root)
    _write_exchange_quotes(data_root)
    _write_polymarket_quotes(data_root)

    config_path = tmp_path / "replay_config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "snapshot_cadence_ms: 60000",
                "max_composite_age_ms: 70000",
                "max_oracle_age_ms: 70000",
                "min_active_venues: 3",
                "taker_fee_bps: 0",
                "slippage_buffer_bps: 1",
                "model_uncertainty_bps: 1",
                "fast_return_count: 2",
                "baseline_return_count: 3",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = replay_day_main(
        [
            "--date",
            trade_date,
            "--data-root",
            str(data_root),
            "--output-root",
            str(output_root),
            "--config",
            str(config_path),
            "--rebuild-reference",
            "true",
            "--rebuild-snapshots",
            "true",
            "--min-seconds-remaining",
            "60",
            "--max-seconds-remaining",
            "300",
            "--edge-threshold",
            "0.01",
        ]
    )

    assert exit_code == 0

    run_dirs = sorted((output_root / "replay" / trade_date).glob("run_*"))
    assert len(run_dirs) == 1
    run_dir = run_dirs[0]

    assert (run_dir / "config_effective.yaml").exists()
    assert (
        run_dir
        / "reference"
        / "window_reference"
        / "date=2026-03-13"
        / "part-00000.jsonl"
    ).exists()
    assert (run_dir / "snapshots" / "snapshots.jsonl").exists()
    assert (run_dir / "snapshots" / "labeled_snapshots.jsonl").exists()
    assert (run_dir / "simulation" / "trades.jsonl").exists()
    assert (run_dir / "simulation" / "summary.json").exists()
    assert (run_dir / "slices" / "by_seconds_remaining_bucket.csv").exists()
    assert (run_dir / "report" / "report.md").exists()

    summary = json.loads((run_dir / "simulation" / "summary.json").read_text(encoding="utf-8"))
    assert summary["snapshot_count"] >= 1

    labeled_rows = (run_dir / "snapshots" / "labeled_snapshots.jsonl").read_text(
        encoding="utf-8"
    ).splitlines()
    assert labeled_rows


def _write_metadata_candidates(data_root: Path) -> None:
    payload = json.loads((RAW_FIXTURE_DIR / "btc_5m_event.json").read_text(encoding="utf-8"))
    candidate = normalize_market_payload(
        market_payload=payload["markets"][0],
        event_payload=payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "polymarket_metadata"
        / "date=2026-03-13"
        / "part-00000.jsonl",
        [{key: serialize_value(value) for key, value in asdict(candidate).items()}],
    )


def _write_chainlink_ticks(data_root: Path) -> None:
    rows = []
    base_ts = datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC)
    prices = ["84000.00", "84005.00", "84010.00", "84015.00", "84020.00", "84025.00"]
    for offset, price in enumerate(prices):
        event_ts = base_ts + timedelta(minutes=offset)
        rows.append(
            {
                "event_id": f"cl-{offset}",
                "event_ts": event_ts.isoformat().replace("+00:00", "Z"),
                "recv_ts": event_ts.isoformat().replace("+00:00", "Z"),
                "price": price,
                "oracle_feed_id": "chainlink:stream:BTC-USD",
            }
        )
    write_jsonl_rows(
        data_root
        / "normalized"
        / "chainlink_ticks"
        / "date=2026-03-13"
        / "part-00000.jsonl",
        rows,
    )


def _write_exchange_quotes(data_root: Path) -> None:
    rows = []
    base_ts = datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC)
    base_prices = {
        VenueCode.BINANCE: Decimal("84000"),
        VenueCode.COINBASE: Decimal("84001"),
        VenueCode.KRAKEN: Decimal("83999"),
    }
    for minute in range(5):
        event_ts = base_ts + timedelta(minutes=minute)
        for venue, base_price in base_prices.items():
            mid = base_price + Decimal(minute * 5)
            quote = ExchangeQuote(
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
                schema_version="0.1.0",
                created_ts=event_ts,
            )
            rows.append({key: serialize_value(value) for key, value in asdict(quote).items()})
    write_jsonl_rows(
        data_root
        / "normalized"
        / "exchange_quotes"
        / "date=2026-03-13"
        / "part-00000.jsonl",
        rows,
    )


def _write_polymarket_quotes(data_root: Path) -> None:
    rows = []
    base_ts = datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC)
    payload = json.loads((RAW_FIXTURE_DIR / "btc_5m_event.json").read_text(encoding="utf-8"))
    market_id = str(payload["markets"][0]["conditionId"])
    for minute in range(5):
        event_ts = base_ts + timedelta(minutes=minute)
        quote = PolymarketQuote(
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
            raw_event_id=f"poly:{event_ts.isoformat()}",
            normalizer_version="0.1.0",
            schema_version="0.1.0",
            created_ts=event_ts,
            token_yes_id="token-up",
            token_no_id="token-down",
            last_trade_price=Decimal("0.55"),
            last_trade_size_contracts=Decimal("100"),
        )
        rows.append({key: serialize_value(value) for key, value in asdict(quote).items()})
    write_jsonl_rows(
        data_root
        / "normalized"
        / "polymarket_quotes"
        / "date=2026-03-13"
        / "part-00000.jsonl",
        rows,
    )
