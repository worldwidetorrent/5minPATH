"""Typed aliases shared across the canonical layer."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import NewType, TypeAlias

UTCDateTime: TypeAlias = datetime
Milliseconds = NewType("Milliseconds", int)
Seconds = NewType("Seconds", int)
UsdPrice = NewType("UsdPrice", Decimal)
ContractPrice = NewType("ContractPrice", Decimal)

Identifier = NewType("Identifier", str)
WindowId = NewType("WindowId", str)
SnapshotId = NewType("SnapshotId", str)
OracleFeedId = NewType("OracleFeedId", str)
InstrumentId = NewType("InstrumentId", str)
MarketId = NewType("MarketId", str)
AssetId = NewType("AssetId", str)
VenueId = NewType("VenueId", str)

DecimalLike: TypeAlias = Decimal | int | float | str

__all__ = [
    "AssetId",
    "ContractPrice",
    "DecimalLike",
    "Identifier",
    "InstrumentId",
    "MarketId",
    "Milliseconds",
    "OracleFeedId",
    "Seconds",
    "SnapshotId",
    "UTCDateTime",
    "UsdPrice",
    "VenueId",
    "WindowId",
]
