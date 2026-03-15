from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from rtds.cli.collect import _resolve_capture_timing, main


def test_collect_cli_rejects_direct_invocation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("RTDS_COLLECTION_ENTRYPOINT", raising=False)

    exit_code = main(
        [
            "--data-root",
            str(tmp_path / "data"),
            "--artifacts-root",
            str(tmp_path / "artifacts"),
            "--logs-root",
            str(tmp_path / "logs"),
            "--temp-root",
            str(tmp_path / "tmp"),
        ]
    )

    assert exit_code == 2


def test_collect_cli_prepare_only_bootstraps_layout(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("RTDS_COLLECTION_ENTRYPOINT", "scripts/run_collectors.sh")

    exit_code = main(
        [
            "--data-root",
            str(tmp_path / "data"),
            "--artifacts-root",
            str(tmp_path / "artifacts"),
            "--logs-root",
            str(tmp_path / "logs"),
            "--temp-root",
            str(tmp_path / "tmp"),
            "--prepare-only",
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "data" / "raw").exists()
    assert (tmp_path / "data" / "normalized").exists()
    assert (tmp_path / "data" / "reference").exists()
    assert (tmp_path / "artifacts").exists()
    assert (tmp_path / "logs").exists()
    assert (tmp_path / "tmp").exists()


def test_resolve_capture_timing_uses_pilot_density_defaults() -> None:
    timing = _resolve_capture_timing(
        Namespace(
            capture_mode="pilot",
            poll_interval_seconds=None,
            metadata_poll_interval_seconds=None,
            chainlink_poll_interval_seconds=None,
            exchange_poll_interval_seconds=None,
            polymarket_quote_poll_interval_seconds=None,
            boundary_burst_enabled=None,
            boundary_burst_window_seconds=None,
            boundary_burst_interval_seconds=None,
            max_consecutive_selection_failures=None,
            max_consecutive_chainlink_failures=None,
            max_consecutive_exchange_failures=None,
            max_consecutive_polymarket_failures=None,
            max_consecutive_polymarket_failures_in_grace=None,
        )
    )

    assert timing["metadata_poll_interval_seconds"] == 30.0
    assert timing["chainlink_poll_interval_seconds"] == 1.0
    assert timing["exchange_poll_interval_seconds"] == 1.0
    assert timing["polymarket_quote_poll_interval_seconds"] == 1.0
    assert timing["boundary_burst_enabled"] is True
    assert timing["max_consecutive_chainlink_failures"] == 15
    assert timing["max_consecutive_exchange_failures"] == 15
    assert timing["max_consecutive_polymarket_failures"] == 15
    assert timing["max_consecutive_polymarket_failures_in_grace"] == 30
    assert timing["poll_interval_seconds"] == 1.0
