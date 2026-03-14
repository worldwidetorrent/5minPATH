"""Deterministic writers for canonical persisted datasets and replay artifacts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from rtds.core.time import format_utc
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
                    handle.write(json_dumps_stable(record.to_storage_dict()))
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


def json_dumps_stable(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def serialize_value(value: object) -> Any:
    """Convert common dataclass, datetime, and Decimal values to storage-safe objects."""

    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, MappingProxyType):
        return {key: serialize_value(item) for key, item in dict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [serialize_value(item) for item in value]
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if is_dataclass(value):
        return {key: serialize_value(item) for key, item in asdict(value).items()}
    return value


def write_jsonl_rows(
    path: str | Path,
    rows: Iterable[Mapping[str, object] | object],
) -> Path:
    """Write stable JSONL rows to one file."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            if isinstance(row, Mapping):
                payload = {str(key): serialize_value(value) for key, value in row.items()}
            else:
                payload = serialize_value(row)
            handle.write(json_dumps_stable(payload))
            handle.write("\n")
    return output_path


def write_json_file(path: str | Path, payload: Mapping[str, object] | object) -> Path:
    """Write one stable JSON payload."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = (
        {str(key): serialize_value(value) for key, value in payload.items()}
        if isinstance(payload, Mapping)
        else serialize_value(payload)
    )
    output_path.write_text(
        f"{json_dumps_stable(serialized)}\n",
        encoding="utf-8",
    )
    return output_path


def write_csv_rows(
    path: str | Path,
    rows: Sequence[Mapping[str, object]],
    *,
    fieldnames: Sequence[str] | None = None,
) -> Path:
    """Write a stable CSV file from mapping rows."""

    import csv

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_fieldnames = list(fieldnames or _infer_fieldnames(rows))
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: _csv_cell(serialize_value(row.get(key)))
                    for key in resolved_fieldnames
                }
            )
    return output_path


def write_text_file(path: str | Path, text: str) -> Path:
    """Write one UTF-8 text artifact with a trailing newline."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = text if text.endswith("\n") else f"{text}\n"
    output_path.write_text(normalized, encoding="utf-8")
    return output_path


def _infer_fieldnames(rows: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    fieldnames: set[str] = set()
    for row in rows:
        fieldnames.update(str(key) for key in row)
    return tuple(sorted(fieldnames))


def _csv_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return json_dumps_stable(value)
    if isinstance(value, dict):
        return json_dumps_stable(value)
    return str(value)


__all__ = [
    "ReferenceWriteResult",
    "WindowReferenceWriter",
    "json_dumps_stable",
    "serialize_value",
    "write_csv_rows",
    "write_json_file",
    "write_jsonl_rows",
    "write_text_file",
]
