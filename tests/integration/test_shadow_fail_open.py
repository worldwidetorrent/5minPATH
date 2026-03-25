from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rtds.execution.adapters import ADAPTER_ROLE_LIVE_STATE, AdapterDescriptor
from rtds.execution.enums import PolicyMode
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SizingPolicy
from tests.execution.support import FakeLiveAdapter, build_state_view


class BlockingLiveAdapter:
    descriptor = AdapterDescriptor(
        adapter_name="blocking-live",
        adapter_role=ADAPTER_ROLE_LIVE_STATE,
        production_safe=True,
    )

    def __init__(self, release: threading.Event) -> None:
        self._release = release
        self.entered = threading.Event()
        self.closed = False
        self._returned = False

    def read_state(self):
        self.entered.set()
        self._release.wait(timeout=2)
        if self._returned:
            return None
        self._returned = True
        return build_state_view()

    def close(self) -> None:
        self.closed = True


def _build_engine(tmp_path, adapter) -> ShadowEngine:
    return ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id="20260326T000000000Z",
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts="10",
            ),
            min_net_edge="0.03",
            max_quote_age_ms=100,
            max_spread_abs="0.03",
            idle_sleep_seconds=0,
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )


def _run_capture_loop(path: Path, *, steps: int) -> list[int]:
    processed: list[int] = []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        for index in range(steps):
            handle.write(json.dumps({"capture_step": index}) + "\n")
            handle.flush()
            processed.append(index)
    return processed


def test_shadow_crash_does_not_break_capture(tmp_path, monkeypatch) -> None:
    capture_path = tmp_path / "artifacts/capture/session123/capture_rows.jsonl"
    states = [
        build_state_view(snapshot_ts=datetime(2026, 3, 26, 0, 0, tzinfo=UTC) + timedelta(seconds=i))
        for i in range(3)
    ]
    engine = _build_engine(tmp_path, adapter=FakeLiveAdapter(states))
    monkeypatch.setattr(
        engine.writer,
        "append_shadow_decision",
        lambda decision: (_ for _ in ()).throw(OSError("shadow disk failure")),
    )

    for index in range(3):
        _run_capture_loop(capture_path, steps=1)
        assert engine.process_next_state() is False
        assert len(capture_path.read_text(encoding="utf-8").splitlines()) == index + 1

    assert len(capture_path.read_text(encoding="utf-8").splitlines()) == 3
    assert engine.stats.error_count == 3


def test_shadow_lag_does_not_backpressure_capture(tmp_path) -> None:
    release = threading.Event()
    adapter = BlockingLiveAdapter(release)
    engine = _build_engine(tmp_path, adapter)
    capture_path = tmp_path / "artifacts/capture/session123/capture_rows.jsonl"

    worker = threading.Thread(target=engine.process_next_state, daemon=True)
    worker.start()
    assert adapter.entered.wait(timeout=1)

    start = time.monotonic()
    processed = _run_capture_loop(capture_path, steps=3)
    elapsed = time.monotonic() - start

    assert processed == [0, 1, 2]
    assert worker.is_alive() is True
    assert elapsed < 0.1

    release.set()
    worker.join(timeout=2)
    assert worker.is_alive() is False


def test_broken_shadow_output_path_does_not_break_capture(tmp_path, monkeypatch) -> None:
    capture_path = tmp_path / "artifacts/capture/session123/capture_rows.jsonl"
    engine = _build_engine(tmp_path, adapter=type("ImmediateAdapter", (), {
        "descriptor": AdapterDescriptor(
            adapter_name="immediate-live",
            adapter_role=ADAPTER_ROLE_LIVE_STATE,
            production_safe=True,
        ),
        "__init__": lambda self: None,
        "read_state": lambda self: build_state_view(),
        "close": lambda self: None,
    })())
    monkeypatch.setattr(
        engine.writer,
        "write_shadow_summary",
        lambda summary: (_ for _ in ()).throw(NotADirectoryError("broken shadow path")),
    )

    processed = _run_capture_loop(capture_path, steps=2)
    assert processed == [0, 1]
    assert engine.process_next_state() is False
    assert len(capture_path.read_text(encoding="utf-8").splitlines()) == 2
    assert engine.stats.error_count == 1
