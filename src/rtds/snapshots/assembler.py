"""Snapshot assembly utilities."""

from __future__ import annotations

from typing import Iterable

from rtds.schemas.snapshot import SnapshotRecord
from rtds.snapshots.builder import SnapshotBuildInput, build_snapshot_row


def assemble_snapshot_rows(build_inputs: Iterable[SnapshotBuildInput]) -> list[SnapshotRecord]:
    """Build and deterministically order a batch of snapshot rows."""

    rows = [build_snapshot_row(build_input) for build_input in build_inputs]
    return sorted(rows, key=lambda row: (row.snapshot_ts, row.snapshot_id))


__all__ = ["assemble_snapshot_rows"]
