"""Normalized event schemas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import (
    validate_exchange_spot_instrument_id,
    validate_polymarket_market_id,
)
from rtds.core.time import ensure_utc
from rtds.core.units import (
    to_decimal,
    validate_contract_price,
    validate_size,
    validate_usd_price,
)

SCHEMA_VERSION = "0.1.0"

SUPPORTED_EXCHANGE_QUOTE_VENUES: tuple[VenueCode, ...] = (
    VenueCode.BINANCE,
    VenueCode.COINBASE,
    VenueCode.KRAKEN,
)


@dataclass(slots=True, frozen=True)
class ExchangeQuote:
    """One normalized top-of-book quote observation for a BTC spot venue."""

    venue_id: str
    instrument_id: str
    asset_id: str
    event_ts: datetime
    recv_ts: datetime
    proc_ts: datetime
    best_bid: Decimal
    best_ask: Decimal
    mid_price: Decimal
    bid_size: Decimal
    ask_size: Decimal
    raw_event_id: str
    normalizer_version: str
    schema_version: str
    created_ts: datetime
    quote_type: str | None = None
    quote_depth_level: int | None = None
    sequence_id: str | None = None
    source_event_missing_ts_flag: bool = False
    crossed_market_flag: bool = False
    locked_market_flag: bool = False
    normalization_status: str = "normalized"

    def __post_init__(self) -> None:
        venue = VenueCode(self.venue_id)
        if venue not in SUPPORTED_EXCHANGE_QUOTE_VENUES:
            raise ValueError("exchange quote venue is not supported in phase 1")

        validate_exchange_spot_instrument_id(self.instrument_id)
        if self.asset_id != AssetCode.BTC.value:
            raise ValueError("phase-1 exchange quotes must use asset_id='BTC'")
        if not self.raw_event_id.strip():
            raise ValueError("raw_event_id must not be empty")

        object.__setattr__(
            self,
            "event_ts",
            ensure_utc(self.event_ts, field_name="event_ts"),
        )
        object.__setattr__(
            self,
            "recv_ts",
            ensure_utc(self.recv_ts, field_name="recv_ts"),
        )
        object.__setattr__(
            self,
            "proc_ts",
            ensure_utc(self.proc_ts, field_name="proc_ts"),
        )
        object.__setattr__(
            self,
            "created_ts",
            ensure_utc(self.created_ts, field_name="created_ts"),
        )

        best_bid = Decimal(validate_usd_price(self.best_bid, field_name="best_bid"))
        best_ask = Decimal(validate_usd_price(self.best_ask, field_name="best_ask"))
        bid_size = to_decimal(validate_size(self.bid_size, field_name="bid_size"))
        ask_size = to_decimal(validate_size(self.ask_size, field_name="ask_size"))
        expected_mid = (best_bid + best_ask) / Decimal("2")

        object.__setattr__(self, "best_bid", best_bid)
        object.__setattr__(self, "best_ask", best_ask)
        object.__setattr__(self, "bid_size", bid_size)
        object.__setattr__(self, "ask_size", ask_size)
        object.__setattr__(self, "mid_price", to_decimal(self.mid_price, field_name="mid_price"))

        if self.mid_price != expected_mid:
            raise ValueError("mid_price must equal (best_bid + best_ask) / 2")
        if not self.crossed_market_flag and best_bid > best_ask:
            raise ValueError("best_bid must be <= best_ask unless crossed_market_flag is true")
        if self.locked_market_flag and best_bid != best_ask:
            raise ValueError("locked_market_flag requires best_bid == best_ask")

    @property
    def bid(self) -> Decimal:
        """Alias for the best bid price."""

        return self.best_bid

    @property
    def ask(self) -> Decimal:
        """Alias for the best ask price."""

        return self.best_ask

    @property
    def mid(self) -> Decimal:
        """Alias for the mid price."""

        return self.mid_price


@dataclass(slots=True, frozen=True)
class PolymarketQuote:
    """One normalized Polymarket top-of-book observation for a mapped market."""

    venue_id: str
    market_id: str
    asset_id: str
    event_ts: datetime
    recv_ts: datetime
    proc_ts: datetime
    up_bid: Decimal
    up_ask: Decimal
    down_bid: Decimal
    down_ask: Decimal
    up_bid_size_contracts: Decimal
    up_ask_size_contracts: Decimal
    down_bid_size_contracts: Decimal
    down_ask_size_contracts: Decimal
    raw_event_id: str
    normalizer_version: str
    schema_version: str
    created_ts: datetime
    token_yes_id: str | None = None
    token_no_id: str | None = None
    market_quote_type: str | None = None
    quote_sequence_id: str | None = None
    market_mid_up: Decimal | None = None
    market_mid_down: Decimal | None = None
    market_spread_up_abs: Decimal | None = None
    market_spread_down_abs: Decimal | None = None
    last_trade_price: Decimal | None = None
    last_trade_size_contracts: Decimal | None = None
    last_trade_side: str | None = None
    last_trade_outcome: str | None = None
    source_event_missing_ts_flag: bool = False
    crossed_market_flag: bool = False
    locked_market_flag: bool = False
    quote_completeness_flag: bool = True
    normalization_status: str = "normalized"

    def __post_init__(self) -> None:
        if self.venue_id != VenueCode.POLYMARKET.value:
            raise ValueError("Polymarket quotes must use venue_id='polymarket'")
        validate_polymarket_market_id(self.market_id)
        if self.asset_id != AssetCode.BTC.value:
            raise ValueError("phase-1 Polymarket quotes must use asset_id='BTC'")
        if not self.raw_event_id.strip():
            raise ValueError("raw_event_id must not be empty")

        for field_name in ("event_ts", "recv_ts", "proc_ts", "created_ts"):
            object.__setattr__(
                self,
                field_name,
                ensure_utc(getattr(self, field_name), field_name=field_name),
            )

        up_bid = Decimal(validate_contract_price(self.up_bid, field_name="up_bid"))
        up_ask = Decimal(validate_contract_price(self.up_ask, field_name="up_ask"))
        down_bid = Decimal(validate_contract_price(self.down_bid, field_name="down_bid"))
        down_ask = Decimal(validate_contract_price(self.down_ask, field_name="down_ask"))
        up_bid_size = to_decimal(
            validate_size(self.up_bid_size_contracts, field_name="up_bid_size_contracts")
        )
        up_ask_size = to_decimal(
            validate_size(self.up_ask_size_contracts, field_name="up_ask_size_contracts")
        )
        down_bid_size = to_decimal(
            validate_size(
                self.down_bid_size_contracts,
                field_name="down_bid_size_contracts",
            )
        )
        down_ask_size = to_decimal(
            validate_size(
                self.down_ask_size_contracts,
                field_name="down_ask_size_contracts",
            )
        )

        object.__setattr__(self, "up_bid", up_bid)
        object.__setattr__(self, "up_ask", up_ask)
        object.__setattr__(self, "down_bid", down_bid)
        object.__setattr__(self, "down_ask", down_ask)
        object.__setattr__(self, "up_bid_size_contracts", up_bid_size)
        object.__setattr__(self, "up_ask_size_contracts", up_ask_size)
        object.__setattr__(self, "down_bid_size_contracts", down_bid_size)
        object.__setattr__(self, "down_ask_size_contracts", down_ask_size)

        implied_mid_up = (up_bid + up_ask) / Decimal("2")
        market_mid_up = to_decimal(
            self.market_mid_up if self.market_mid_up is not None else implied_mid_up,
            field_name="market_mid_up",
        )
        market_mid_down = to_decimal(
            self.market_mid_down
            if self.market_mid_down is not None
            else (down_bid + down_ask) / Decimal("2"),
            field_name="market_mid_down",
        )
        market_spread_up_abs = to_decimal(
            self.market_spread_up_abs
            if self.market_spread_up_abs is not None
            else up_ask - up_bid,
            field_name="market_spread_up_abs",
        )
        market_spread_down_abs = to_decimal(
            self.market_spread_down_abs
            if self.market_spread_down_abs is not None
            else down_ask - down_bid,
            field_name="market_spread_down_abs",
        )

        object.__setattr__(self, "market_mid_up", market_mid_up)
        object.__setattr__(self, "market_mid_down", market_mid_down)
        object.__setattr__(self, "market_spread_up_abs", market_spread_up_abs)
        object.__setattr__(self, "market_spread_down_abs", market_spread_down_abs)

        if self.last_trade_price is not None:
            object.__setattr__(
                self,
                "last_trade_price",
                Decimal(
                    validate_contract_price(
                        self.last_trade_price,
                        field_name="last_trade_price",
                    )
                ),
            )
        if self.last_trade_size_contracts is not None:
            object.__setattr__(
                self,
                "last_trade_size_contracts",
                to_decimal(
                    validate_size(
                        self.last_trade_size_contracts,
                        field_name="last_trade_size_contracts",
                    )
                ),
            )

        if not self.crossed_market_flag and (up_bid > up_ask or down_bid > down_ask):
            raise ValueError("bid must be <= ask unless crossed_market_flag is true")
        if self.locked_market_flag and not (
            up_bid == up_ask or down_bid == down_ask
        ):
            raise ValueError("locked_market_flag requires at least one locked side")
        if market_spread_up_abs < Decimal("0") or market_spread_down_abs < Decimal("0"):
            raise ValueError("spread fields must be non-negative")


__all__ = [
    "PolymarketQuote",
    "SCHEMA_VERSION",
    "SUPPORTED_EXCHANGE_QUOTE_VENUES",
    "ExchangeQuote",
]
