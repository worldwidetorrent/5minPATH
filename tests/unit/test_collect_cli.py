from __future__ import annotations

from pathlib import Path

from rtds.cli.collect import main


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
