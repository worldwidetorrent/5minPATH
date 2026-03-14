"""Replay label attachment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal
from rtds.schemas.snapshot import SnapshotRecord
from rtds.schemas.window_reference import WindowReferenceRecord

FEATURE_VERSION = "0.1.0"

REALIZED_UP = "up"
REALIZED_DOWN = "down"
REALIZED_FLAT = "flat"
REALIZED_UNKNOWN = "unknown"

LABEL_STATUS_ATTACHED = "attached"
LABEL_STATUS_UNRESOLVED = "unresolved"
LABEL_STATUS_MISMATCHED = "mismatched"

LABEL_FLAG_MISSING_SETTLEMENT = "missing_settlement"
LABEL_FLAG_AMBIGUOUS_ASSIGNMENT = "ambiguous_assignment"
LABEL_FLAG_NON_RESOLVED_OUTCOME = "non_resolved_outcome"


@dataclass(slots=True, frozen=True)
class SnapshotLabel:
    """Offline truth attached to one snapshot for replay evaluation."""

    snapshot_id: str
    window_id: str
    polymarket_market_id: str
    snapshot_ts: datetime
    resolved_up: bool | None
    chainlink_settle_price: Decimal | None
    chainlink_settle_ts: datetime | None
    settle_minus_open: Decimal | None
    realized_direction: str
    label_status: str
    label_quality_flags: tuple[str, ...]
    feature_version: str = FEATURE_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "snapshot_ts",
            ensure_utc(self.snapshot_ts, field_name="snapshot_ts"),
        )
        if self.chainlink_settle_ts is not None:
            object.__setattr__(
                self,
                "chainlink_settle_ts",
                ensure_utc(self.chainlink_settle_ts, field_name="chainlink_settle_ts"),
            )
        if self.chainlink_settle_price is not None:
            object.__setattr__(
                self,
                "chainlink_settle_price",
                to_decimal(self.chainlink_settle_price, field_name="chainlink_settle_price"),
            )
        if self.settle_minus_open is not None:
            object.__setattr__(
                self,
                "settle_minus_open",
                to_decimal(self.settle_minus_open, field_name="settle_minus_open"),
            )
        object.__setattr__(
            self,
            "label_quality_flags",
            tuple(sorted(set(self.label_quality_flags))),
        )


@dataclass(slots=True, frozen=True)
class LabeledSnapshotRecord:
    """Snapshot row paired with offline truth for replay evaluation."""

    snapshot: SnapshotRecord
    label: SnapshotLabel


def attach_label(
    snapshot: SnapshotRecord,
    window_reference: WindowReferenceRecord,
) -> LabeledSnapshotRecord:
    """Attach offline truth from a matching window-reference row to one snapshot."""

    _validate_match(snapshot, window_reference)

    label_quality_flags: list[str] = []
    label_status = LABEL_STATUS_ATTACHED

    if window_reference.assignment_status == "ambiguous":
        label_quality_flags.append(LABEL_FLAG_AMBIGUOUS_ASSIGNMENT)
        label_status = LABEL_STATUS_UNRESOLVED
    if (
        window_reference.chainlink_settle_price is None
        or window_reference.chainlink_settle_ts is None
    ):
        label_quality_flags.append(LABEL_FLAG_MISSING_SETTLEMENT)
        label_status = LABEL_STATUS_UNRESOLVED
    if window_reference.outcome_status != "resolved":
        label_quality_flags.append(LABEL_FLAG_NON_RESOLVED_OUTCOME)
        label_status = LABEL_STATUS_UNRESOLVED

    label = SnapshotLabel(
        snapshot_id=snapshot.snapshot_id or "",
        window_id=snapshot.window_id,
        polymarket_market_id=snapshot.polymarket_market_id,
        snapshot_ts=snapshot.snapshot_ts,
        resolved_up=window_reference.resolved_up,
        chainlink_settle_price=window_reference.chainlink_settle_price,
        chainlink_settle_ts=window_reference.chainlink_settle_ts,
        settle_minus_open=window_reference.settle_minus_open,
        realized_direction=_derive_realized_direction(window_reference),
        label_status=label_status,
        label_quality_flags=tuple(label_quality_flags),
    )
    return LabeledSnapshotRecord(snapshot=snapshot, label=label)


def attach_labels(
    snapshots: Iterable[SnapshotRecord],
    window_references: Iterable[WindowReferenceRecord],
) -> list[LabeledSnapshotRecord]:
    """Attach offline truth to a batch of snapshots using `(window_id, market_id)` identity."""

    reference_index = {
        (reference.window_id, reference.polymarket_market_id): reference
        for reference in window_references
    }

    labeled_rows: list[LabeledSnapshotRecord] = []
    for snapshot in snapshots:
        key = (snapshot.window_id, snapshot.polymarket_market_id)
        reference = reference_index.get(key)
        if reference is None:
            raise ValueError(
                "no window reference found for snapshot "
                f"window_id={snapshot.window_id} market_id={snapshot.polymarket_market_id}"
            )
        labeled_rows.append(attach_label(snapshot, reference))
    return labeled_rows


def _validate_match(
    snapshot: SnapshotRecord,
    window_reference: WindowReferenceRecord,
) -> None:
    if snapshot.window_id != window_reference.window_id:
        raise ValueError("snapshot.window_id does not match window_reference.window_id")
    if snapshot.polymarket_market_id != window_reference.polymarket_market_id:
        raise ValueError(
            "snapshot.polymarket_market_id does not match window_reference.polymarket_market_id"
        )


def _derive_realized_direction(window_reference: WindowReferenceRecord) -> str:
    if window_reference.resolved_up is True:
        return REALIZED_UP
    if window_reference.resolved_up is False:
        return REALIZED_DOWN
    if window_reference.settle_minus_open == Decimal("0"):
        return REALIZED_FLAT
    return REALIZED_UNKNOWN


__all__ = [
    "FEATURE_VERSION",
    "LABEL_FLAG_AMBIGUOUS_ASSIGNMENT",
    "LABEL_FLAG_MISSING_SETTLEMENT",
    "LABEL_FLAG_NON_RESOLVED_OUTCOME",
    "LABEL_STATUS_ATTACHED",
    "LABEL_STATUS_MISMATCHED",
    "LABEL_STATUS_UNRESOLVED",
    "LabeledSnapshotRecord",
    "REALIZED_DOWN",
    "REALIZED_FLAT",
    "REALIZED_UNKNOWN",
    "REALIZED_UP",
    "SnapshotLabel",
    "attach_label",
    "attach_labels",
]
