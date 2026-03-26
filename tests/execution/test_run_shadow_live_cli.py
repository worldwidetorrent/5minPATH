from __future__ import annotations

from rtds.cli.run_shadow_live import main
from tests.execution.test_capture_output_live_state_adapter import _write_fixture_session


def test_run_shadow_live_cli_processes_fixture_session(tmp_path) -> None:
    session_id = "20260326T010000000Z"
    _write_fixture_session(tmp_path, session_id=session_id)

    exit_code = main(
        [
            "--session-id",
            session_id,
            "--normalized-root",
            str(tmp_path / "data/normalized"),
            "--artifacts-root",
            str(tmp_path / "artifacts/collect"),
            "--shadow-root",
            str(tmp_path / "artifacts/shadow"),
            "--idle-sleep-seconds",
            "0",
            "--max-iterations",
            "2",
        ]
    )

    assert exit_code == 0
    assert (
        tmp_path / f"artifacts/shadow/{session_id}/shadow_decisions.jsonl"
    ).exists()
    assert (
        tmp_path / f"artifacts/shadow/{session_id}/shadow_summary.json"
    ).exists()
