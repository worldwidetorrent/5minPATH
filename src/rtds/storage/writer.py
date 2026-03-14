"""Deterministic writers for canonical persisted datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from rtds.schemas.window_reference import WindowReferenceRecord
from rtds.storage.parquet_layout import window_reference_part_path


@dataclass(slots=True, frozen=True)
class ReferenceWriteResult:
    """Summary of one reference-dataset write operation."""

    dataset_name: str
    dataset_root: Path
    files_written: tuple[Path, ...]
    row_count: int
    partition_dates: tuple[str, ...]


class WindowReferenceWriter:
    """Persist window-reference rows as stable JSONL partitions."""

    def __init__(self, base_dir: str | Path = "data/reference") -> None:
        self._base_dir = Path(base_dir)

    @property
    def base_dir(self) -> Path:
        """Return the configured reference dataset root."""

        return self._base_dir

    def write(
        self,
        records: Iterable[WindowReferenceRecord],
        *,
        overwrite: bool = True,
        part_index: int = 0,
    ) -> ReferenceWriteResult:
        """Write rows grouped by UTC day with deterministic ordering."""

        grouped_records: dict[str, list[WindowReferenceRecord]] = {}
        for record in records:
            grouped_records.setdefault(record.date_utc.isoformat(), []).append(record)

        files_written: list[Path] = []
        total_rows = 0
        for date_utc in sorted(grouped_records):
            partition_records = sorted(
                grouped_records[date_utc],
                key=lambda record: (
                    record.window_start_ts,
                    record.window_id,
                    record.polymarket_market_id or "",
                ),
            )
            output_path = window_reference_part_path(
                self._base_dir,
                date_utc,
                part_index=part_index,
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.exists() and not overwrite:
                raise FileExistsError(f"window-reference output already exists: {output_path}")

            with output_path.open("w", encoding="utf-8", newline="\n") as handle:
                for record in partition_records:
                    handle.write(_json_dumps_stable(record.to_storage_dict()))
                    handle.write("\n")
                    total_rows += 1

            files_written.append(output_path)

        return ReferenceWriteResult(
            dataset_name="window_reference",
            dataset_root=self._base_dir,
            files_written=tuple(files_written),
            row_count=total_rows,
            partition_dates=tuple(sorted(grouped_records)),
        )


def _json_dumps_stable(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


__all__ = [
    "ReferenceWriteResult",
    "WindowReferenceWriter",
]
