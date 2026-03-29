from __future__ import annotations

import signal

from rtds.cli.run_shadow_live import _install_shutdown_signal_handlers, main
from rtds.execution.enums import PolicyMode
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SizingPolicy
from tests.execution.support import FakeLiveAdapter
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
            "--shadow-attach-ts",
            "2026-03-26T00:59:59Z",
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


def test_install_shutdown_signal_handlers_requests_graceful_stop(tmp_path) -> None:
    engine = ShadowEngine(
        adapter=FakeLiveAdapter([]),
        config=ShadowEngineConfig(
            session_id="20260326T000000000Z",
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts="10",
            ),
            min_net_edge="0.01",
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )

    restore_handlers = _install_shutdown_signal_handlers(engine)
    try:
        signal.raise_signal(signal.SIGTERM)
        assert engine._stop_requested is True
    finally:
        restore_handlers()
