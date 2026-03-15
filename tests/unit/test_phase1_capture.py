from __future__ import annotations

from rtds.collectors.phase1_capture import (
    _build_polymarket_quote_payload,
    _decode_latest_round_data,
)


def test_decode_latest_round_data_parses_chainlink_tuple() -> None:
    payload = (
        "0x"
        "00000000000000000000000000000000000000000000000100000000000c24a6"
        "0000000000000000000000000000000000000000000000000006792850a95800"
        "0000000000000000000000000000000000000000000000000000000069b5f396"
        "0000000000000000000000000000000000000000000000000000000069b5f396"
        "00000000000000000000000000000000000000000000000100000000000c24a6"
    )

    decoded = _decode_latest_round_data(payload)

    assert decoded["round_id"] == 18446744073710347430
    assert decoded["answer"] == 1822063919192064
    assert decoded["updated_at"] == 1773532054


def test_build_polymarket_quote_payload_uses_best_bid_and_best_ask() -> None:
    payload = _build_polymarket_quote_payload(
        market_id="0xbtc",
        yes_token_id="yes-token",
        no_token_id="no-token",
        yes_book={
            "timestamp": "1773532629107",
            "hash": "yes-hash",
            "bids": [{"price": "0.45", "size": "10"}, {"price": "0.48", "size": "12"}],
            "asks": [{"price": "0.52", "size": "20"}, {"price": "0.50", "size": "30"}],
        },
        no_book={
            "timestamp": "1773532629107",
            "hash": "no-hash",
            "bids": [{"price": "0.49", "size": "9"}, {"price": "0.51", "size": "8"}],
            "asks": [{"price": "0.55", "size": "11"}, {"price": "0.53", "size": "7"}],
        },
    )

    assert payload["market_id"] == "0xbtc"
    assert payload["sequence_id"] == "yes-hash:no-hash"
    assert payload["outcomes"]["up"]["bid"] == {"price": "0.48", "size": "12"}
    assert payload["outcomes"]["up"]["ask"] == {"price": "0.50", "size": "30"}
    assert payload["outcomes"]["down"]["bid"] == {"price": "0.51", "size": "8"}
    assert payload["outcomes"]["down"]["ask"] == {"price": "0.53", "size": "7"}
