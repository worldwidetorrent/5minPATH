from __future__ import annotations

import logging
from pathlib import Path

from rtds.execution.file_tail import JsonlFileTail


def test_jsonl_file_tail_reads_new_rows_without_duplication(tmp_path: Path) -> None:
    path = tmp_path / "data/normalized/chainlink_ticks/date=2026-03-26/session=s1/part-00000.jsonl"
    _append_lines(path, ['{"a": 1}\n', '{"a": 2}\n'])
    tail = JsonlFileTail(
        str(tmp_path / "data/normalized/chainlink_ticks/date=*/session=s1/*.jsonl")
    )

    first = tail.read_new_rows()
    second = tail.read_new_rows()

    assert first == [{"a": 1}, {"a": 2}]
    assert second == []

    _append_lines(path, ['{"a": 3}\n'])

    third = tail.read_new_rows()

    assert third == [{"a": 3}]


def test_jsonl_file_tail_discovers_late_created_and_cross_midnight_partitions(
    tmp_path: Path,
) -> None:
    tail = JsonlFileTail(
        str(tmp_path / "data/normalized/exchange_quotes/date=*/session=s1/*.jsonl")
    )

    assert tail.read_new_rows() == []

    first_path = (
        tmp_path
        / "data/normalized/exchange_quotes/date=2026-03-26/session=s1/part-00000.jsonl"
    )
    second_path = (
        tmp_path
        / "data/normalized/exchange_quotes/date=2026-03-27/session=s1/part-00000.jsonl"
    )
    _append_lines(first_path, ['{"date": "2026-03-26", "n": 1}\n'])
    assert tail.read_new_rows() == [{"date": "2026-03-26", "n": 1}]

    _append_lines(second_path, ['{"date": "2026-03-27", "n": 2}\n'])
    assert tail.read_new_rows() == [{"date": "2026-03-27", "n": 2}]


def test_jsonl_file_tail_waits_for_complete_line(tmp_path: Path) -> None:
    path = tmp_path / "artifacts/collect/date=2026-03-26/session=s1/sample_diagnostics.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"sample": 1}', encoding="utf-8")
    tail = JsonlFileTail(
        str(tmp_path / "artifacts/collect/date=*/session=s1/sample_diagnostics.jsonl")
    )

    assert tail.read_new_rows() == []

    _append_lines(path, ['\n'])
    assert tail.read_new_rows() == [{"sample": 1}]


def test_jsonl_file_tail_logs_and_continues_on_bad_json(
    tmp_path: Path,
    caplog,
) -> None:
    broken = (
        tmp_path
        / "data/normalized/polymarket_quotes/date=2026-03-26/session=s1/part-00000.jsonl"
    )
    good = (
        tmp_path
        / "data/normalized/polymarket_quotes/date=2026-03-27/session=s1/part-00000.jsonl"
    )
    _append_lines(broken, ["not-json\n"])
    _append_lines(good, ['{"ok": 1}\n'])
    tail = JsonlFileTail(
        str(tmp_path / "data/normalized/polymarket_quotes/date=*/session=s1/*.jsonl")
    )

    with caplog.at_level(logging.WARNING):
        rows = tail.read_new_rows()

    assert rows == [{"ok": 1}]
    assert "execution file tail read failed" in caplog.text

    broken.write_text("", encoding="utf-8")
    _append_lines(broken, ['{"ok": 2}\n'])

    rows = tail.read_new_rows()

    assert rows == [{"ok": 2}]


def _append_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as handle:
        for line in lines:
            handle.write(line)
