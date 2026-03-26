from __future__ import annotations

import json

from rtds.execution.capture_output_live_state_adapter import (
    CaptureOutputLiveStateAdapter,
    CaptureOutputLiveStateConfig,
)
from rtds.execution.enums import PolicyMode
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SizingPolicy
from tests.execution.test_capture_output_live_state_adapter import (
    _append_jsonl,
    _append_lines,
    _write_fixture_session,
)


def test_malformed_appended_row_increments_shadow_errors_without_breaking_capture(tmp_path) -> None:
    session_id = "20260326T010000000Z"
    _write_fixture_session(tmp_path, session_id=session_id)
    capture_path = tmp_path / "artifacts/capture/session123/capture_rows.jsonl"
    capture_path.parent.mkdir(parents=True, exist_ok=True)
    capture_path.write_text("", encoding="utf-8")

    adapter = CaptureOutputLiveStateAdapter(
        CaptureOutputLiveStateConfig(
            session_id=session_id,
            normalized_root=tmp_path / "data/normalized",
            artifacts_root=tmp_path / "artifacts/collect",
        )
    )
    engine = ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id=session_id,
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts="10",
            ),
            min_net_edge="0.01",
            max_quote_age_ms=2000,
            max_spread_abs="0.03",
            idle_sleep_seconds=0,
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )

    capture_path.write_text(json.dumps({"capture_step": 0}) + "\n", encoding="utf-8")
    assert engine.process_next_state() is True
    assert engine.stats.error_count == 0

    _append_lines(
        tmp_path
        / (
            "data/normalized/polymarket_quotes/"
            f"date=2026-03-27/session={session_id}/part-00000.jsonl"
        ),
        ["not-json\n"],
    )
    _append_jsonl(
        tmp_path
        / f"artifacts/collect/date=2026-03-26/session={session_id}/sample_diagnostics.jsonl",
        [
            {
                "sample_index": 2,
                "sample_started_at": "2026-03-26T01:00:06.000Z",
                "sample_status": "healthy",
                "degraded_sources": [],
                "selected_market_id": "0xmarket",
                "selected_market_slug": "btc-updown-5m-1770000600",
                "selected_window_id": "btc-5m-20260326T010000Z",
                "source_results": {
                    "chainlink": {"status": "success", "details": {"fallback_used": False}},
                    "polymarket_quotes": {
                        "status": "success",
                        "details": {"seconds_remaining": 294},
                    },
                },
            }
        ],
    )
    capture_path.write_text(
        capture_path.read_text(encoding="utf-8")
        + json.dumps({"capture_step": 1})
        + "\n",
        encoding="utf-8",
    )

    assert engine.process_next_state() is True
    assert engine.stats.error_count == 1
    assert len(capture_path.read_text(encoding="utf-8").splitlines()) == 2
    assert (
        tmp_path / f"artifacts/shadow/{session_id}/shadow_summary.json"
    ).exists()
