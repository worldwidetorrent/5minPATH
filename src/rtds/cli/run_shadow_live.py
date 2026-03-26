"""Run the execution-v0 shadow engine against live capture outputs."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from rtds.execution.capture_output_live_state_adapter import (
    CaptureOutputLiveStateAdapter,
    CaptureOutputLiveStateConfig,
)
from rtds.execution.enums import PolicyMode
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import (
    SIZE_MODE_FIXED_CONTRACTS,
    SIZE_MODE_FIXED_NOTIONAL,
    SizingPolicy,
)

LOGGER = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    _configure_logging(level=args.log_level)
    run_shadow_live(args)
    print(Path(args.shadow_root) / args.session_id)
    return 0


def run_shadow_live(args: argparse.Namespace) -> Path:
    adapter = CaptureOutputLiveStateAdapter(
        CaptureOutputLiveStateConfig(
            session_id=args.session_id,
            normalized_root=Path(args.normalized_root),
            artifacts_root=Path(args.artifacts_root),
            calibration_config_path=(
                None if args.calibration_config is None else Path(args.calibration_config)
            ),
            calibration_summary_path=(
                None if args.calibration_summary is None else Path(args.calibration_summary)
            ),
        )
    )
    engine = ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id=args.session_id,
            policy_name=args.policy_name,
            policy_role=args.policy_role,
            policy_mode=PolicyMode(args.policy_mode),
            sizing_policy=_build_sizing_policy(args),
            min_net_edge=args.min_net_edge,
            max_quote_age_ms=args.max_quote_age_ms,
            max_spread_abs=args.max_spread_abs,
            heartbeat_interval_seconds=args.heartbeat_interval_seconds,
            idle_sleep_seconds=args.idle_sleep_seconds,
            shadow_root_dir=args.shadow_root,
        ),
    )
    LOGGER.info(
        "starting shadow live runtime session=%s normalized_root=%s "
        "artifacts_root=%s shadow_root=%s",
        args.session_id,
        args.normalized_root,
        args.artifacts_root,
        args.shadow_root,
    )
    engine.run(max_iterations=args.max_iterations)
    return Path(args.shadow_root) / args.session_id


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--normalized-root", default="data/normalized")
    parser.add_argument("--artifacts-root", default="artifacts/collect")
    parser.add_argument("--shadow-root", default="artifacts/shadow")
    parser.add_argument("--policy-name", default="good_only_baseline")
    parser.add_argument("--policy-role", default="baseline")
    parser.add_argument(
        "--policy-mode",
        choices=tuple(mode.value for mode in PolicyMode),
        default=PolicyMode.BASELINE.value,
    )
    parser.add_argument(
        "--size-mode",
        choices=(SIZE_MODE_FIXED_CONTRACTS, SIZE_MODE_FIXED_NOTIONAL),
        default=SIZE_MODE_FIXED_CONTRACTS,
    )
    parser.add_argument("--fixed-size-contracts", default="10")
    parser.add_argument("--fixed-notional-value")
    parser.add_argument("--min-net-edge", default="0.01")
    parser.add_argument("--max-quote-age-ms", type=int, default=2000)
    parser.add_argument("--max-spread-abs", default="0.03")
    parser.add_argument("--heartbeat-interval-seconds", type=float, default=60.0)
    parser.add_argument("--idle-sleep-seconds", type=float, default=0.25)
    parser.add_argument("--max-iterations", type=int)
    parser.add_argument("--calibration-config")
    parser.add_argument("--calibration-summary")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser


def _build_sizing_policy(args: argparse.Namespace) -> SizingPolicy:
    if args.size_mode == SIZE_MODE_FIXED_NOTIONAL:
        return SizingPolicy(
            size_mode=args.size_mode,
            fixed_notional_value=args.fixed_notional_value or "100",
        )
    return SizingPolicy(
        size_mode=args.size_mode,
        fixed_size_contracts=args.fixed_size_contracts,
    )


def _configure_logging(*, level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


if __name__ == "__main__":
    raise SystemExit(main())
