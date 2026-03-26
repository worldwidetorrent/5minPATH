"""Incremental fail-open JSONL tailing for execution sidecars."""

from __future__ import annotations

import glob
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class JsonlFileTail:
    """Tail session-scoped JSONL partitions without rereading old rows.

    The tailer is intentionally fail-open:
    - missing files are ignored
    - newly created files are discovered on the next poll
    - partial trailing lines are skipped until they are complete
    - read or decode failures are logged and isolated to the broken file
    """

    pattern: str
    _offsets: dict[Path, int] = field(default_factory=dict)
    _error_count: int = 0

    def read_new_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for raw_path in sorted(glob.glob(self.pattern)):
            path = Path(raw_path)
            if not path.is_file():
                continue
            rows.extend(self._read_new_rows_from_path(path))
        return rows

    def _read_new_rows_from_path(self, path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = self._offsets.get(path, 0)
        try:
            with path.open("r", encoding="utf-8") as handle:
                handle.seek(offset)
                while True:
                    start_pos = handle.tell()
                    line = handle.readline()
                    if not line:
                        break
                    if not line.endswith("\n"):
                        handle.seek(start_pos)
                        break
                    payload = line.strip()
                    if not payload:
                        continue
                    decoded = json.loads(payload)
                    if not isinstance(decoded, dict):
                        raise ValueError("expected JSON object row")
                    rows.append(decoded)
                self._offsets[path] = handle.tell()
        except FileNotFoundError:
            return rows
        except Exception as exc:  # pragma: no cover - exact failure types vary by runtime
            self._error_count += 1
            logger.warning(
                "execution file tail read failed",
                extra={
                    "path": str(path),
                    "offset": offset,
                    "error": repr(exc),
                },
            )
            self._offsets[path] = offset
        return rows

    def consume_error_count(self) -> int:
        """Return and reset the number of fail-open read errors seen so far."""

        count = self._error_count
        self._error_count = 0
        return count


__all__ = ["JsonlFileTail"]
