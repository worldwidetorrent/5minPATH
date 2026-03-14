"""Canonical end-to-end replay runner for one UTC trade date."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

from rtds.core.enums import SnapshotOrigin
from rtds.core.time import format_utc_compact, seconds_remaining, utc_now
from rtds.features.composite_nowcast import (
    DEFAULT_MINIMUM_VENUE_COUNT,
    CompositeNowcast,
    compute_composite_nowcast,
)
from rtds.features.executable_edge import (
    EdgeCostPolicy,
    ExecutableEdgeEstimate,
    compute_executable_edge,
)
from rtds.features.fair_value_base import FairValueBaseEstimate, compute_fair_value_base
from rtds.features.volatility import (
    DEFAULT_VOLATILITY_POLICY,
    VolatilityEstimate,
    VolatilityPolicy,
    compute_volatility_from_nowcasts,
)
from rtds.mapping.anchor_assignment import (
    ANCHOR_ASSIGNMENT_VERSION,
    ChainlinkTick,
    assign_window_reference,
)
from rtds.mapping.market_mapper import (
    MAPPING_VERSION,
    WindowMarketMappingRecord,
    map_candidates_to_windows,
)
from rtds.mapping.window_ids import daily_window_schedule
from rtds.quality.dispersion import DispersionPolicy, assess_exchange_composite_quality
from rtds.quality.freshness import FreshnessPolicy, assess_source_freshness
from rtds.quality.gap_detection import GapDetectionPolicy, assess_chainlink_quality
from rtds.replay.attach_labels import LabeledSnapshotRecord, attach_label
from rtds.replay.loader import (
    load_chainlink_ticks,
    load_exchange_quotes,
    load_metadata_candidates,
    load_polymarket_quotes,
    load_snapshots,
    load_window_references,
)
from rtds.replay.simulate import (
    DEFAULT_ENTRY_RULE_POLICY,
    EntryRulePolicy,
    FeeCurvePolicy,
    ReplaySimulationInput,
    SimulatedTrade,
    simulate_replay,
)
from rtds.replay.slices import ReplaySliceInput, ReplaySlicePolicy, generate_replay_slices
from rtds.schemas.normalized import PolymarketQuote
from rtds.schemas.snapshot import SnapshotRecord
from rtds.schemas.window_reference import SCHEMA_VERSION as WINDOW_REFERENCE_SCHEMA_VERSION
from rtds.schemas.window_reference import WindowReferenceRecord
from rtds.snapshots.builder import SnapshotBuildInput, build_snapshot_row
from rtds.storage.writer import (
    WindowReferenceWriter,
    serialize_value,
    write_csv_rows,
    write_json_file,
    write_jsonl_rows,
    write_text_file,
)

DEFAULT_OUTPUT_ROOT = "artifacts"
DEFAULT_DATA_ROOT = "data"
DEFAULT_REPORT_NAME = "report.md"
DEFAULT_MIN_SECONDS_REMAINING = 0
DEFAULT_MAX_SECONDS_REMAINING = 300
DEFAULT_EDGE_THRESHOLD = Decimal("0")
DEFAULT_SNAPSHOT_CADENCE_MS = 1_000
DEFAULT_REFERENCE_WRITE_BASE = "reference"


@dataclass(slots=True, frozen=True)
class ReplayRunConfig:
    """Effective configuration for one canonical replay run."""

    trade_date: date
    data_root: Path
    output_root: Path
    run_dir: Path
    rebuild_reference: bool
    rebuild_snapshots: bool
    snapshot_cadence_ms: int
    min_seconds_remaining: int
    max_seconds_remaining: int
    edge_threshold: Decimal
    freshness_policy: FreshnessPolicy
    polymarket_freshness_policy: FreshnessPolicy
    dispersion_policy: DispersionPolicy
    gap_detection_policy: GapDetectionPolicy
    volatility_policy_fast_count: int
    volatility_policy_baseline_count: int
    volatility_policy: VolatilityPolicy
    edge_cost_policy: EdgeCostPolicy
    fee_curve: FeeCurvePolicy
    entry_rules: EntryRulePolicy
    minimum_venue_count: int
    config_sources: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class SnapshotEvaluationRow:
    """One snapshot plus downstream fair-value, edge, label, and simulation state."""

    snapshot: SnapshotRecord
    composite_nowcast: CompositeNowcast
    volatility: VolatilityEstimate
    fair_value: FairValueBaseEstimate
    edge: ExecutableEdgeEstimate
    labeled_snapshot: LabeledSnapshotRecord
    simulated_trade: SimulatedTrade
    seconds_remaining: int


def main(argv: Sequence[str] | None = None) -> int:
    """Run the canonical replay pipeline for one UTC date."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_replay_day(args)
    print(run_dir)
    return 0


def run_replay_day(args: argparse.Namespace) -> Path:
    """Execute the end-to-end replay pipeline and write deterministic artifacts."""

    trade_date = date.fromisoformat(args.date)
    run_ts = utc_now()
    run_dir = (
        Path(args.output_root)
        / "replay"
        / trade_date.isoformat()
        / f"run_{format_utc_compact(run_ts)}"
    )
    config = _build_effective_config(args, trade_date=trade_date, run_dir=run_dir)
    _write_effective_config(config)

    chainlink_ticks = load_chainlink_ticks(config.data_root, date_utc=config.trade_date)
    references = _load_or_build_references(config, chainlink_ticks=chainlink_ticks)
    write_reference_artifacts(references, run_dir=config.run_dir)

    snapshots = _load_or_build_snapshots(
        config,
        references=references,
        chainlink_ticks=chainlink_ticks,
    )
    write_snapshot_artifacts(snapshots, run_dir=config.run_dir)

    evaluation_rows = evaluate_snapshots(
        snapshots=snapshots,
        references=references,
        config=config,
    )
    write_evaluation_artifacts(evaluation_rows, run_dir=config.run_dir)
    return config.run_dir


def evaluate_snapshots(
    *,
    snapshots: list[SnapshotRecord],
    references: list[WindowReferenceRecord],
    config: ReplayRunConfig,
) -> list[SnapshotEvaluationRow]:
    """Compute volatility, fair value, executable edge, labels, simulation, and slices."""

    reference_by_identity = {
        (reference.window_id, reference.polymarket_market_id): reference
        for reference in references
        if reference.polymarket_market_id is not None
    }
    nowcast_history: list[CompositeNowcast] = []
    simulation_inputs: list[ReplaySimulationInput] = []
    evaluation_rows: list[SnapshotEvaluationRow] = []

    for snapshot in sorted(snapshots, key=lambda row: (row.snapshot_ts, row.snapshot_id or "")):
        reference = reference_by_identity.get((snapshot.window_id, snapshot.polymarket_market_id))
        if reference is None:
            continue

        composite_nowcast = CompositeNowcast(
            as_of_ts=snapshot.snapshot_ts,
            composite_now_price=snapshot.composite_now_price,
            composite_method=snapshot.composite_method,
            feature_version=snapshot.feature_version,
            composite_missing_flag=snapshot.composite_missing_flag,
            contributing_venue_count=snapshot.composite_contributing_venue_count,
            contributing_venues=snapshot.composite_contributing_venues,
            per_venue_mids=snapshot.composite_per_venue_mids,
            per_venue_ages=snapshot.composite_per_venue_ages,
            dispersion_abs_usd=snapshot.composite_dispersion_abs_usd,
            dispersion_bps=snapshot.composite_dispersion_bps,
            quality_score=snapshot.composite_quality_score,
            outlier_venue_ids=(),
            diagnostics=snapshot.quality_diagnostics,
        )
        nowcast_history.append(composite_nowcast)
        recent_nowcasts = nowcast_history[-(config.volatility_policy_baseline_count + 1) :]
        volatility = _compute_row_volatility(
            recent_nowcasts,
            as_of_ts=snapshot.snapshot_ts,
            config=config,
        )
        remaining_seconds = int(seconds_remaining(snapshot.window_end_ts, snapshot.snapshot_ts))
        fair_value = compute_fair_value_base(
            chainlink_open_anchor_price=snapshot.chainlink_open_anchor_price,
            composite_now_price=snapshot.composite_now_price,
            seconds_remaining=remaining_seconds,
            sigma_eff=volatility.sigma_eff,
        )
        edge = compute_executable_edge(
            fair_value_base=fair_value.fair_value_base,
            polymarket_quote=_snapshot_to_polymarket_quote(snapshot),
            cost_policy=config.edge_cost_policy,
        )
        labeled_snapshot = attach_label(snapshot, reference)
        simulated_trade = _simulate_one(
            labeled_snapshot=labeled_snapshot,
            edge=edge,
            seconds_remaining_value=remaining_seconds,
            config=config,
        )
        evaluation_rows.append(
            SnapshotEvaluationRow(
                snapshot=snapshot,
                composite_nowcast=composite_nowcast,
                volatility=volatility,
                fair_value=fair_value,
                edge=edge,
                labeled_snapshot=labeled_snapshot,
                simulated_trade=simulated_trade,
                seconds_remaining=remaining_seconds,
            )
        )
        if _within_entry_window(remaining_seconds, config=config):
            simulation_inputs.append(
                ReplaySimulationInput(
                    labeled_snapshot=labeled_snapshot,
                    executable_edge=edge,
                )
            )

    replay_result = simulate_replay(
        simulation_inputs,
        fee_curve=config.fee_curve,
        entry_rules=config.entry_rules,
    )
    trade_by_snapshot_id = {
        trade.snapshot_id: trade for trade in replay_result.trades
    }
    finalized_rows = [
        SnapshotEvaluationRow(
            snapshot=row.snapshot,
            composite_nowcast=row.composite_nowcast,
            volatility=row.volatility,
            fair_value=row.fair_value,
            edge=row.edge,
            labeled_snapshot=row.labeled_snapshot,
            simulated_trade=trade_by_snapshot_id.get(
                row.snapshot.snapshot_id or "",
                row.simulated_trade,
            ),
            seconds_remaining=row.seconds_remaining,
        )
        for row in evaluation_rows
    ]
    _write_slice_and_report_artifacts(finalized_rows, replay_result.summary, config=config)
    return finalized_rows


def write_reference_artifacts(
    references: Iterable[WindowReferenceRecord],
    *,
    run_dir: Path,
) -> None:
    """Write window-reference artifacts under the canonical run contract."""

    writer = WindowReferenceWriter(run_dir / DEFAULT_REFERENCE_WRITE_BASE)
    writer.write(list(references))


def write_snapshot_artifacts(snapshots: Iterable[SnapshotRecord], *, run_dir: Path) -> None:
    """Write snapshot artifacts for one replay run."""

    write_jsonl_rows(
        run_dir / "snapshots" / "snapshots.jsonl",
        [snapshot.to_storage_dict() for snapshot in snapshots],
    )


def write_evaluation_artifacts(
    evaluation_rows: list[SnapshotEvaluationRow],
    *,
    run_dir: Path,
) -> None:
    """Write labeled snapshots and simulation artifacts."""

    labeled_rows = [_labeled_snapshot_row(row) for row in evaluation_rows]
    trade_rows = [_trade_row(row) for row in evaluation_rows]
    write_jsonl_rows(run_dir / "snapshots" / "labeled_snapshots.jsonl", labeled_rows)
    write_jsonl_rows(run_dir / "simulation" / "trades.jsonl", trade_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC trade date in YYYY-MM-DD form.")
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--config")
    parser.add_argument("--rebuild-snapshots", default="true", type=_parse_bool)
    parser.add_argument("--rebuild-reference", default="false", type=_parse_bool)
    parser.add_argument(
        "--min-seconds-remaining",
        default=DEFAULT_MIN_SECONDS_REMAINING,
        type=int,
    )
    parser.add_argument(
        "--max-seconds-remaining",
        default=DEFAULT_MAX_SECONDS_REMAINING,
        type=int,
    )
    parser.add_argument(
        "--edge-threshold",
        default=str(DEFAULT_EDGE_THRESHOLD),
        type=Decimal,
    )
    parser.add_argument("--fee-config")
    parser.add_argument("--slippage-config")
    return parser


def _build_effective_config(
    args: argparse.Namespace,
    *,
    trade_date: date,
    run_dir: Path,
) -> ReplayRunConfig:
    defaults = _load_default_replay_config_files()
    for optional_path in (args.config, args.fee_config, args.slippage_config):
        if optional_path:
            defaults.update(_load_flat_yaml(Path(optional_path)))

    snapshot_cadence_ms = int(defaults.get("snapshot_cadence_ms", DEFAULT_SNAPSHOT_CADENCE_MS))
    max_composite_age_ms = int(defaults.get("max_composite_age_ms", 1_500))
    max_oracle_age_ms = int(defaults.get("max_oracle_age_ms", 5_000))
    min_active_venues = int(defaults.get("min_active_venues", DEFAULT_MINIMUM_VENUE_COUNT))
    taker_fee_bps = int(defaults.get("taker_fee_bps", 0))
    slippage_buffer_bps = int(defaults.get("slippage_buffer_bps", 10))
    model_uncertainty_bps = int(defaults.get("model_uncertainty_bps", 15))
    fast_return_count = int(
        defaults.get(
            "fast_return_count",
            DEFAULT_VOLATILITY_POLICY.fast_return_count,
        )
    )
    baseline_return_count = int(
        defaults.get("baseline_return_count", DEFAULT_VOLATILITY_POLICY.baseline_return_count)
    )
    volatility_policy = VolatilityPolicy(
        fast_return_count=fast_return_count,
        baseline_return_count=baseline_return_count,
        fast_weight=DEFAULT_VOLATILITY_POLICY.fast_weight,
        sigma_floor=DEFAULT_VOLATILITY_POLICY.sigma_floor,
        sigma_cap=DEFAULT_VOLATILITY_POLICY.sigma_cap,
    )
    edge_cost_policy = EdgeCostPolicy.from_bps(
        taker_fee_bps=taker_fee_bps,
        slippage_up_bps=slippage_buffer_bps,
        slippage_down_bps=slippage_buffer_bps,
        model_uncertainty_bps=model_uncertainty_bps,
    )
    entry_rules = EntryRulePolicy(
        min_net_edge=Decimal(args.edge_threshold),
        target_trade_size_contracts=DEFAULT_ENTRY_RULE_POLICY.target_trade_size_contracts,
        allow_buy_up=DEFAULT_ENTRY_RULE_POLICY.allow_buy_up,
        allow_buy_down=DEFAULT_ENTRY_RULE_POLICY.allow_buy_down,
    )
    return ReplayRunConfig(
        trade_date=trade_date,
        data_root=Path(args.data_root),
        output_root=Path(args.output_root),
        run_dir=run_dir,
        rebuild_reference=bool(args.rebuild_reference),
        rebuild_snapshots=bool(args.rebuild_snapshots),
        snapshot_cadence_ms=snapshot_cadence_ms,
        min_seconds_remaining=int(args.min_seconds_remaining),
        max_seconds_remaining=int(args.max_seconds_remaining),
        edge_threshold=Decimal(args.edge_threshold),
        freshness_policy=FreshnessPolicy(
            stale_after_ms=max_composite_age_ms,
            missing_after_ms=max_composite_age_ms * 5,
        ),
        polymarket_freshness_policy=FreshnessPolicy(
            stale_after_ms=max_composite_age_ms,
            missing_after_ms=max_composite_age_ms * 5,
        ),
        dispersion_policy=DispersionPolicy(min_contributing_venues=min_active_venues),
        gap_detection_policy=GapDetectionPolicy(
            stale_after_ms=max_oracle_age_ms,
            missing_after_ms=max_oracle_age_ms * 2,
            silence_after_ms=max_oracle_age_ms,
            max_inter_tick_gap_ms=max_oracle_age_ms,
        ),
        volatility_policy_fast_count=fast_return_count,
        volatility_policy_baseline_count=baseline_return_count,
        volatility_policy=volatility_policy,
        edge_cost_policy=edge_cost_policy,
        fee_curve=FeeCurvePolicy(taker_fee_rate=edge_cost_policy.fee_rate_estimate),
        entry_rules=entry_rules,
        minimum_venue_count=min_active_venues,
        config_sources=tuple(
            str(path)
            for path in (
                Path("configs/replay/snapshot_builder.yaml"),
                Path("configs/replay/quality_thresholds.yaml"),
                Path("configs/replay/fee_slippage.yaml"),
                *(
                    Path(path)
                    for path in (args.config, args.fee_config, args.slippage_config)
                    if path
                ),
            )
        ),
    )


def _write_effective_config(config: ReplayRunConfig) -> None:
    payload = {
        "trade_date": config.trade_date.isoformat(),
        "data_root": str(config.data_root),
        "output_root": str(config.output_root),
        "run_dir": str(config.run_dir),
        "rebuild_reference": config.rebuild_reference,
        "rebuild_snapshots": config.rebuild_snapshots,
        "snapshot_cadence_ms": config.snapshot_cadence_ms,
        "min_seconds_remaining": config.min_seconds_remaining,
        "max_seconds_remaining": config.max_seconds_remaining,
        "edge_threshold": str(config.edge_threshold),
        "minimum_venue_count": config.minimum_venue_count,
        "taker_fee_rate": str(config.fee_curve.taker_fee_rate),
        "slippage_estimate_up": str(config.edge_cost_policy.slippage_estimate_up),
        "slippage_estimate_down": str(config.edge_cost_policy.slippage_estimate_down),
        "model_error_buffer": str(config.edge_cost_policy.model_error_buffer),
        "config_sources": list(config.config_sources),
    }
    yaml_text = "\n".join(f"{key}: {_yaml_scalar(value)}" for key, value in payload.items())
    write_text_file(config.run_dir / "config_effective.yaml", yaml_text)


def _load_or_build_references(
    config: ReplayRunConfig,
    *,
    chainlink_ticks: list[ChainlinkTick],
) -> list[WindowReferenceRecord]:
    if not config.rebuild_reference:
        loaded = load_window_references(config.data_root, date_utc=config.trade_date)
        if loaded:
            return loaded
        raise FileNotFoundError(
            "no persisted window_reference rows found and rebuild_reference=false"
        )

    candidates = load_metadata_candidates(config.data_root, date_utc=config.trade_date)
    windows = daily_window_schedule(config.trade_date)
    mapping_batch = map_candidates_to_windows(windows, candidates)
    references: list[WindowReferenceRecord] = []
    for mapping_record in mapping_batch.records:
        if mapping_record.mapping_status == "mapped":
            references.append(assign_window_reference(mapping_record, chainlink_ticks))
        else:
            references.append(_unmapped_reference_row(mapping_record))
    return references


def _load_or_build_snapshots(
    config: ReplayRunConfig,
    *,
    references: list[WindowReferenceRecord],
    chainlink_ticks: list[ChainlinkTick],
) -> list[SnapshotRecord]:
    if not config.rebuild_snapshots:
        snapshot_root = (
            config.data_root
            / "snapshots"
            / "snapshots"
            / f"date={config.trade_date.isoformat()}"
        )
        loaded = load_snapshots(snapshot_root)
        if loaded:
            return loaded
        raise FileNotFoundError("no persisted snapshots found and rebuild_snapshots=false")

    exchange_quotes = sorted(
        load_exchange_quotes(config.data_root, date_utc=config.trade_date),
        key=lambda quote: quote.event_ts,
    )
    polymarket_quotes = load_polymarket_quotes(config.data_root, date_utc=config.trade_date)
    polymarket_by_market: dict[str, list[Any]] = {}
    for quote in sorted(polymarket_quotes, key=lambda item: item.event_ts):
        polymarket_by_market.setdefault(quote.market_id, []).append(quote)

    snapshots: list[SnapshotRecord] = []
    exchange_history: list[Any] = []
    exchange_index = 0
    chainlink_history: list[ChainlinkTick] = []
    chainlink_index = 0

    for reference in sorted(references, key=lambda row: row.window_start_ts):
        if reference.mapping_status != "mapped" or reference.polymarket_market_id is None:
            continue
        market_quotes = polymarket_by_market.get(reference.polymarket_market_id, [])
        market_history: list[Any] = []
        market_index = 0
        for snapshot_ts in _snapshot_times(
            reference.window_start_ts,
            reference.window_end_ts,
            cadence_ms=config.snapshot_cadence_ms,
        ):
            while (
                exchange_index < len(exchange_quotes)
                and exchange_quotes[exchange_index].event_ts <= snapshot_ts
            ):
                exchange_history.append(exchange_quotes[exchange_index])
                exchange_index += 1
            while (
                chainlink_index < len(chainlink_ticks)
                and chainlink_ticks[chainlink_index].event_ts <= snapshot_ts
            ):
                chainlink_history.append(chainlink_ticks[chainlink_index])
                chainlink_index += 1
            while (
                market_index < len(market_quotes)
                and market_quotes[market_index].event_ts <= snapshot_ts
            ):
                market_history.append(market_quotes[market_index])
                market_index += 1

            exchange_quality = assess_exchange_composite_quality(
                exchange_history,
                as_of_ts=snapshot_ts,
                freshness_policy=config.freshness_policy,
                dispersion_policy=config.dispersion_policy,
            )
            composite_nowcast = compute_composite_nowcast(
                exchange_history,
                as_of_ts=snapshot_ts,
                freshness_policy=config.freshness_policy,
                minimum_venue_count=config.minimum_venue_count,
            )
            current_market_quote = market_history[-1] if market_history else None
            polymarket_freshness = assess_source_freshness(
                "polymarket",
                as_of_ts=snapshot_ts,
                last_event_ts=(
                    None if current_market_quote is None else current_market_quote.event_ts
                ),
                policy=config.polymarket_freshness_policy,
            )
            chainlink_quality = assess_chainlink_quality(
                chainlink_history,
                as_of_ts=snapshot_ts,
                policy=config.gap_detection_policy,
            )
            snapshots.append(
                build_snapshot_row(
                    SnapshotBuildInput(
                        window_reference=reference,
                        snapshot_ts=snapshot_ts,
                        chainlink_current_tick=(
                            None if not chainlink_history else chainlink_history[-1]
                        ),
                        composite_nowcast=composite_nowcast,
                        exchange_quality=exchange_quality,
                        polymarket_quote=current_market_quote,
                        polymarket_quote_freshness=polymarket_freshness,
                        chainlink_quality=chainlink_quality,
                        snapshot_origin=SnapshotOrigin.FIXED_1S,
                        created_ts=snapshot_ts,
                    )
                )
            )
    return snapshots


def _simulate_one(
    *,
    labeled_snapshot: LabeledSnapshotRecord,
    edge: ExecutableEdgeEstimate,
    seconds_remaining_value: int,
    config: ReplayRunConfig,
) -> SimulatedTrade:
    if not _within_entry_window(seconds_remaining_value, config=config):
        return SimulatedTrade(
            snapshot_id=labeled_snapshot.snapshot.snapshot_id or "",
            window_id=labeled_snapshot.snapshot.window_id,
            polymarket_market_id=labeled_snapshot.snapshot.polymarket_market_id,
            sim_trade_direction="no_trade",
            sim_entry_price=None,
            sim_exit_price=None,
            sim_fee_paid=None,
            sim_slippage_paid=None,
            sim_pnl=Decimal("0"),
            sim_roi=Decimal("0"),
            sim_outcome="no_trade",
            predicted_edge_net=None,
            realized_edge=None,
            no_trade_reason="entry_rule_blocked",
            simulation_version="0.1.0",
        )
    return simulate_replay(
        [
            ReplaySimulationInput(
                labeled_snapshot=labeled_snapshot,
                executable_edge=edge,
            )
        ],
        fee_curve=config.fee_curve,
        entry_rules=config.entry_rules,
    ).trades[0]


def _write_slice_and_report_artifacts(
    evaluation_rows: list[SnapshotEvaluationRow],
    simulation_summary,
    *,
    config: ReplayRunConfig,
) -> None:
    slice_inputs = [
        ReplaySliceInput(
            labeled_snapshot=row.labeled_snapshot,
            executable_edge=row.edge,
            simulated_trade=row.simulated_trade,
            seconds_remaining=row.seconds_remaining,
            sigma_eff=row.volatility.sigma_eff,
        )
        for row in evaluation_rows
    ]
    slice_report = generate_replay_slices(slice_inputs, policy=ReplaySlicePolicy())
    write_json_file(
        config.run_dir / "simulation" / "summary.json",
        {
            "snapshot_count": simulation_summary.snapshot_count,
            "trade_count": simulation_summary.trade_count,
            "hit_rate": simulation_summary.hit_rate,
            "total_pnl": simulation_summary.total_pnl,
            "average_predicted_edge": simulation_summary.average_predicted_edge,
            "average_realized_edge": simulation_summary.average_realized_edge,
            "realized_minus_predicted_edge": simulation_summary.realized_minus_predicted_edge,
            "simulation_version": simulation_summary.simulation_version,
        },
    )
    for dimension, rows in slice_report.by_dimension.items():
        csv_rows = [_slice_result_row(row) for row in rows]
        write_csv_rows(config.run_dir / "slices" / f"by_{dimension}.csv", csv_rows)
    write_text_file(
        config.run_dir / "report" / DEFAULT_REPORT_NAME,
        _render_report(
            evaluation_rows=evaluation_rows,
            simulation_summary=simulation_summary,
            config=config,
        ),
    )


def _render_report(
    *,
    evaluation_rows: list[SnapshotEvaluationRow],
    simulation_summary,
    config: ReplayRunConfig,
) -> str:
    positive_raw = sum(
        1
        for row in evaluation_rows
        if (
            (row.edge.edge_up_raw is not None and row.edge.edge_up_raw > 0)
            or (row.edge.edge_down_raw is not None and row.edge.edge_down_raw > 0)
        )
    )
    positive_net = sum(
        1
        for row in evaluation_rows
        if (
            (row.edge.edge_up_net is not None and row.edge.edge_up_net > 0)
            or (row.edge.edge_down_net is not None and row.edge.edge_down_net > 0)
        )
    )
    return "\n".join(
        [
            f"# Replay Report — {config.trade_date.isoformat()}",
            "",
            "## Run",
            f"- run_dir: `{config.run_dir}`",
            f"- snapshots: {len(evaluation_rows)}",
            f"- trade_count: {simulation_summary.trade_count}",
            f"- hit_rate: {simulation_summary.hit_rate}",
            f"- total_pnl: {simulation_summary.total_pnl}",
            "",
            "## Edge",
            f"- positive_raw_edge_rows: {positive_raw}",
            f"- positive_net_edge_rows: {positive_net}",
            f"- edge_threshold: {config.edge_threshold}",
            "",
            "## Policy Inputs",
            f"- min_seconds_remaining: {config.min_seconds_remaining}",
            f"- max_seconds_remaining: {config.max_seconds_remaining}",
            f"- minimum_venue_count: {config.minimum_venue_count}",
            f"- taker_fee_rate: {config.fee_curve.taker_fee_rate}",
            f"- slippage_up: {config.edge_cost_policy.slippage_estimate_up}",
            f"- slippage_down: {config.edge_cost_policy.slippage_estimate_down}",
            f"- model_error_buffer: {config.edge_cost_policy.model_error_buffer}",
        ]
    )


def _snapshot_times(
    window_start_ts: datetime,
    window_end_ts: datetime,
    *,
    cadence_ms: int,
) -> list[datetime]:
    if cadence_ms <= 0:
        raise ValueError("snapshot cadence must be positive")
    step = timedelta(milliseconds=cadence_ms)
    timestamps: list[datetime] = []
    current = window_start_ts
    while current < window_end_ts:
        timestamps.append(current)
        current = current + step
    return timestamps


def _snapshot_to_polymarket_quote(snapshot: SnapshotRecord):
    if snapshot.polymarket_quote_event_ts is None:
        return None
    if (
        snapshot.up_bid is None
        or snapshot.up_ask is None
        or snapshot.down_bid is None
        or snapshot.down_ask is None
    ):
        return None
    return PolymarketQuote(
        venue_id="polymarket",
        market_id=snapshot.polymarket_market_id,
        asset_id=snapshot.asset_id,
        event_ts=snapshot.polymarket_quote_event_ts,
        recv_ts=snapshot.polymarket_quote_recv_ts or snapshot.polymarket_quote_event_ts,
        proc_ts=snapshot.created_ts,
        up_bid=snapshot.up_bid,
        up_ask=snapshot.up_ask,
        down_bid=snapshot.down_bid,
        down_ask=snapshot.down_ask,
        up_bid_size_contracts=snapshot.up_bid_size_contracts or Decimal("0"),
        up_ask_size_contracts=snapshot.up_ask_size_contracts or Decimal("0"),
        down_bid_size_contracts=snapshot.down_bid_size_contracts or Decimal("0"),
        down_ask_size_contracts=snapshot.down_ask_size_contracts or Decimal("0"),
        raw_event_id=f"snapshot:{snapshot.snapshot_id}",
        normalizer_version="0.1.0",
        schema_version="0.1.0",
        created_ts=snapshot.created_ts,
        market_mid_up=snapshot.market_mid_up,
        market_mid_down=snapshot.market_mid_down,
        market_spread_up_abs=snapshot.market_spread_up_abs,
        market_spread_down_abs=snapshot.market_spread_down_abs,
        last_trade_price=snapshot.last_trade_price,
        last_trade_size_contracts=snapshot.last_trade_size_contracts,
    )


def _labeled_snapshot_row(row: SnapshotEvaluationRow) -> dict[str, object]:
    payload = dict(row.snapshot.to_storage_dict())
    payload.update(
        {
            "resolved_up": row.labeled_snapshot.label.resolved_up,
            "chainlink_settle_price_label": row.labeled_snapshot.label.chainlink_settle_price,
            "chainlink_settle_ts_label": row.labeled_snapshot.label.chainlink_settle_ts,
            "settle_minus_open_label": row.labeled_snapshot.label.settle_minus_open,
            "realized_direction": row.labeled_snapshot.label.realized_direction,
            "label_status": row.labeled_snapshot.label.label_status,
            "label_quality_flags": row.labeled_snapshot.label.label_quality_flags,
            "sigma_fast": row.volatility.sigma_fast,
            "sigma_baseline": row.volatility.sigma_baseline,
            "sigma_eff": row.volatility.sigma_eff,
            "log_move_from_open": row.fair_value.log_move_from_open,
            "abs_move_from_open": row.fair_value.abs_move_from_open,
            "z_base": row.fair_value.z_base,
            "fair_value_base": row.fair_value.fair_value_base,
            "edge_up_raw": row.edge.edge_up_raw,
            "edge_down_raw": row.edge.edge_down_raw,
            "edge_up_net": row.edge.edge_up_net,
            "edge_down_net": row.edge.edge_down_net,
            "preferred_side": row.edge.preferred_side,
            "edge_no_trade_reason": row.edge.no_trade_reason,
            "seconds_remaining": row.seconds_remaining,
        }
    )
    return {key: serialize_value(value) for key, value in payload.items()}


def _trade_row(row: SnapshotEvaluationRow) -> dict[str, object]:
    payload = {
        "snapshot_id": row.simulated_trade.snapshot_id,
        "window_id": row.simulated_trade.window_id,
        "polymarket_market_id": row.simulated_trade.polymarket_market_id,
        "sim_trade_direction": row.simulated_trade.sim_trade_direction,
        "sim_entry_price": row.simulated_trade.sim_entry_price,
        "sim_exit_price": row.simulated_trade.sim_exit_price,
        "sim_fee_paid": row.simulated_trade.sim_fee_paid,
        "sim_slippage_paid": row.simulated_trade.sim_slippage_paid,
        "sim_pnl": row.simulated_trade.sim_pnl,
        "sim_roi": row.simulated_trade.sim_roi,
        "sim_outcome": row.simulated_trade.sim_outcome,
        "predicted_edge_net": row.simulated_trade.predicted_edge_net,
        "realized_edge": row.simulated_trade.realized_edge,
        "no_trade_reason": row.simulated_trade.no_trade_reason,
        "seconds_remaining": row.seconds_remaining,
        "fair_value_base": row.fair_value.fair_value_base,
        "sigma_eff": row.volatility.sigma_eff,
    }
    return {key: serialize_value(value) for key, value in payload.items()}


def _slice_result_row(row) -> dict[str, object]:
    return {
        "slice_dimension": row.slice_dimension,
        "slice_key": row.slice_key,
        "row_count": row.row_count,
        "trade_count": row.trade_count,
        "no_trade_count": row.no_trade_count,
        "hit_rate": serialize_value(row.hit_rate),
        "total_pnl": serialize_value(row.total_pnl),
        "average_pnl": serialize_value(row.average_pnl),
        "average_roi": serialize_value(row.average_roi),
        "average_predicted_edge": serialize_value(row.average_predicted_edge),
        "average_realized_edge": serialize_value(row.average_realized_edge),
        "realized_minus_predicted_edge": serialize_value(row.realized_minus_predicted_edge),
    }


def _within_entry_window(seconds_remaining_value: int, *, config: ReplayRunConfig) -> bool:
    return config.min_seconds_remaining <= seconds_remaining_value <= config.max_seconds_remaining


def _unmapped_reference_row(mapping_record: WindowMarketMappingRecord) -> WindowReferenceRecord:
    return WindowReferenceRecord(
        window_id=mapping_record.window_id,
        asset_id=mapping_record.asset_id,
        window_start_ts=mapping_record.window_start_ts,
        window_end_ts=mapping_record.window_end_ts,
        oracle_feed_id="chainlink:stream:BTC-USD",
        polymarket_market_id=mapping_record.polymarket_market_id,
        polymarket_event_id=mapping_record.polymarket_event_id,
        polymarket_slug=mapping_record.polymarket_slug,
        clob_token_id_up=mapping_record.clob_token_id_up,
        clob_token_id_down=mapping_record.clob_token_id_down,
        listing_discovered_ts=mapping_record.listing_discovered_ts,
        market_active_flag=mapping_record.market_active_flag,
        market_closed_flag=mapping_record.market_closed_flag,
        mapping_status=mapping_record.mapping_status,
        mapping_confidence=mapping_record.mapping_confidence,
        mapping_method=mapping_record.mapping_method,
        chainlink_open_anchor_price=None,
        chainlink_open_anchor_ts=None,
        chainlink_open_anchor_event_id=None,
        chainlink_open_anchor_method="missing",
        chainlink_open_anchor_confidence="none",
        chainlink_open_anchor_status="missing",
        chainlink_open_anchor_offset_ms=None,
        chainlink_settle_price=None,
        chainlink_settle_ts=None,
        chainlink_settle_event_id=None,
        chainlink_settle_method="missing",
        chainlink_settle_confidence="none",
        chainlink_settle_status="missing",
        chainlink_settle_offset_ms=None,
        resolved_up=None,
        settle_minus_open=None,
        outcome_status="unresolved",
        assignment_status="not_mapped",
        assignment_diagnostics=("mapping_not_accepted",),
        notes=mapping_record.notes,
        schema_version=WINDOW_REFERENCE_SCHEMA_VERSION,
        normalizer_version=mapping_record.normalizer_version,
        mapping_version=MAPPING_VERSION,
        anchor_assignment_version=ANCHOR_ASSIGNMENT_VERSION,
        created_ts=mapping_record.created_ts,
        updated_ts=mapping_record.updated_ts,
    )


def _load_default_replay_config_files() -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for path in (
        Path("configs/replay/snapshot_builder.yaml"),
        Path("configs/replay/quality_thresholds.yaml"),
        Path("configs/replay/fee_slippage.yaml"),
    ):
        if path.exists():
            merged.update(_load_flat_yaml(path))
    return merged


def _load_flat_yaml(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        payload[key.strip()] = _parse_scalar(value.strip())
    return payload


def _parse_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    try:
        if "." in value:
            return Decimal(value)
        return int(value)
    except Exception:
        return value


def _yaml_scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


def _compute_row_volatility(
    recent_nowcasts: list[CompositeNowcast],
    *,
    as_of_ts: datetime,
    config: ReplayRunConfig,
) -> VolatilityEstimate:
    try:
        return compute_volatility_from_nowcasts(
            recent_nowcasts,
            as_of_ts=as_of_ts,
            policy=config.volatility_policy,
        )
    except ValueError:
        return compute_volatility_from_nowcasts(
            [
                CompositeNowcast(
                    as_of_ts=as_of_ts,
                    composite_now_price=Decimal("1"),
                    composite_method="fallback",
                    feature_version="0.1.0",
                    composite_missing_flag=False,
                    contributing_venue_count=0,
                    contributing_venues=(),
                    per_venue_mids={},
                    per_venue_ages={},
                    dispersion_abs_usd=None,
                    dispersion_bps=None,
                    quality_score=Decimal("0"),
                    outlier_venue_ids=(),
                    diagnostics=("no_composite_history",),
                )
            ],
            as_of_ts=as_of_ts,
            policy=config.volatility_policy,
        )


def _parse_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("expected true|false")


if __name__ == "__main__":
    raise SystemExit(main())
