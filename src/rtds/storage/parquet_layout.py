"""Dataset layout helpers for persisted canonical tables."""

from __future__ import annotations

from pathlib import Path

from rtds.storage.partitions import partition_path_component

WINDOW_REFERENCE_DATASET = "window_reference"


def reference_dataset_root(base_dir: str | Path) -> Path:
    """Return the canonical root for persisted reference datasets."""

    return Path(base_dir)


def window_reference_dataset_root(base_dir: str | Path) -> Path:
    """Return the root directory for the window-reference dataset."""

    return reference_dataset_root(base_dir) / WINDOW_REFERENCE_DATASET


def window_reference_partition_dir(base_dir: str | Path, date_utc: str) -> Path:
    """Return the partition directory for one UTC date."""

    return window_reference_dataset_root(base_dir) / partition_path_component(
        "date",
        date_utc,
    )


def window_reference_part_path(
    base_dir: str | Path,
    date_utc: str,
    *,
    part_index: int = 0,
    extension: str = "jsonl",
) -> Path:
    """Return the deterministic file path for one partition part."""

    if part_index < 0:
        raise ValueError("part_index must be non-negative")
    normalized_extension = extension.strip().lstrip(".")
    if not normalized_extension:
        raise ValueError("extension must not be empty")
    return window_reference_partition_dir(base_dir, date_utc) / (
        f"part-{part_index:05d}.{normalized_extension}"
    )


__all__ = [
    "WINDOW_REFERENCE_DATASET",
    "reference_dataset_root",
    "window_reference_dataset_root",
    "window_reference_part_path",
    "window_reference_partition_dir",
]
