"""Cross-horizon policy-stack comparison across pinned capture sessions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from rtds.storage.writer import serialize_value


@dataclass(slots=True, frozen=True)
class HorizonSession:
    """One pinned capture session used in cross-horizon comparison."""

    label: str
    session_id: str
    capture_date: str
    baseline_manifest_path: str
    baseline_note_path: str
    summary_path: str
    admission_summary_path: str
    policy_stack_summary_path: str


@dataclass(slots=True, frozen=True)
class HorizonSessionSummary:
    """Top-line continuity and quality summary for one session."""

    label: str
    session_id: str
    capture_date: str
    admission_verdict: str
    legacy_verdict: str | None
    snapshot_eligible_sample_ratio: Decimal | None
    off_family_switch_count: int
    selected_binding_unresolved_window_count: int
    window_verdict_counts: dict[str, int]


@dataclass(slots=True, frozen=True)
class StackHorizonMetrics:
    """One policy stack evaluated on one session horizon."""

    session_label: str
    session_id: str
    capture_date: str
    snapshot_count: int
    window_count: int
    trade_count: int
    hit_rate: Decimal | None
    average_selected_net_edge: Decimal | None
    total_pnl: Decimal
    average_roi: Decimal | None
    pnl_per_window: Decimal | None
    pnl_per_1000_snapshots: Decimal | None
    pnl_per_100_trades: Decimal | None


@dataclass(slots=True, frozen=True)
class CrossHorizonStackResult:
    """One stack compared across all configured horizons."""

    stack_name: str
    stack_role: str
    horizons: tuple[StackHorizonMetrics, ...]


@dataclass(slots=True, frozen=True)
class CrossHorizonComparison:
    """Full cross-horizon comparison output."""

    analysis_id: str
    description: str
    comparison_config_path: str
    stack_order: tuple[str, ...]
    metrics: tuple[str, ...]
    sessions: tuple[HorizonSessionSummary, ...]
    stack_results: tuple[CrossHorizonStackResult, ...]


def load_cross_horizon_manifest(path: str | Path) -> dict[str, object]:
    """Load one cross-horizon manifest from JSON."""

    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_cross_horizon_comparison(
    manifest: Mapping[str, object],
) -> CrossHorizonComparison:
    """Build the policy-stack comparison across pinned horizons."""

    sessions = tuple(
        _session_from_payload(item) for item in _payload_sequence(manifest["sessions"])
    )
    session_summaries = tuple(_session_summary(session) for session in sessions)
    stack_order = tuple(str(item) for item in _payload_sequence(manifest["stack_order"]))
    metrics = tuple(str(item) for item in _payload_sequence(manifest["metrics"]))

    stack_payloads_by_session = {
        session.label: _load_policy_stack_summary(Path(session.policy_stack_summary_path))
        for session in sessions
    }
    stack_results = tuple(
        _build_stack_result(
            stack_name=stack_name,
            sessions=sessions,
            stack_payloads_by_session=stack_payloads_by_session,
        )
        for stack_name in stack_order
    )
    return CrossHorizonComparison(
        analysis_id=str(manifest["analysis_id"]),
        description=str(manifest["description"]),
        comparison_config_path=str(manifest["comparison_config_path"]),
        stack_order=stack_order,
        metrics=metrics,
        sessions=session_summaries,
        stack_results=stack_results,
    )


def cross_horizon_comparison_to_dict(
    comparison: CrossHorizonComparison,
) -> dict[str, object]:
    """Serialize one cross-horizon comparison to stable JSON."""

    return {
        "analysis_id": comparison.analysis_id,
        "description": comparison.description,
        "comparison_config_path": comparison.comparison_config_path,
        "stack_order": list(comparison.stack_order),
        "metrics": list(comparison.metrics),
        "sessions": [serialize_value(session) for session in comparison.sessions],
        "stack_results": [
            {
                "stack_name": result.stack_name,
                "stack_role": result.stack_role,
                "horizons": [serialize_value(item) for item in result.horizons],
            }
            for result in comparison.stack_results
        ],
    }


def render_cross_horizon_report(comparison: CrossHorizonComparison) -> str:
    """Render the cross-horizon policy-stack comparison report."""

    lines = [
        f"# Policy Stack Cross-Horizon Comparison — {comparison.analysis_id}",
        "",
        comparison.description,
        "",
        "## Sessions",
    ]
    for session in comparison.sessions:
        lines.extend(
            [
                f"### {session.label}",
                f"- session_id: `{session.session_id}`",
                f"- capture_date: `{session.capture_date}`",
                f"- admission_verdict: `{session.admission_verdict}`",
                f"- legacy_verdict: `{session.legacy_verdict}`",
                f"- snapshot_eligible_sample_ratio: {session.snapshot_eligible_sample_ratio}",
                f"- off_family_switch_count: {session.off_family_switch_count}",
                (
                    "- selected_binding_unresolved_window_count: "
                    f"{session.selected_binding_unresolved_window_count}"
                ),
                f"- window_verdict_counts: {session.window_verdict_counts}",
            ]
        )

    lines.extend(["", "## Stack Comparison"])
    for result in comparison.stack_results:
        lines.extend(
            [
                f"### {result.stack_name}",
                f"- stack_role: `{result.stack_role}`",
            ]
        )
        for horizon in result.horizons:
            lines.append(
                f"- {horizon.session_label}: trades={horizon.trade_count}, "
                f"hit_rate={horizon.hit_rate}, avg_net_edge={horizon.average_selected_net_edge}, "
                f"total_pnl={horizon.total_pnl}, avg_roi={horizon.average_roi}, "
                f"pnl_per_window={horizon.pnl_per_window}, "
                f"pnl_per_100_trades={horizon.pnl_per_100_trades}, "
                f"pnl_per_1000_snapshots={horizon.pnl_per_1000_snapshots}"
            )
        lines.extend(_render_stack_verdict_lines(result))
    return "\n".join(lines) + "\n"


def _render_stack_verdict_lines(result: CrossHorizonStackResult) -> list[str]:
    horizons = result.horizons
    if not horizons:
        return ["- no horizon data found for this stack."]
    lines: list[str] = []
    net_edges = [item.average_selected_net_edge for item in horizons]
    if all(value is not None for value in net_edges):
        if _is_nonincreasing([value for value in net_edges if value is not None]):
            lines.append(
                "- average selected net edge stays below or equal to the "
                "shortest-horizon value as horizon grows."
            )
        else:
            lines.append(
                "- average selected net edge is not monotonic across horizons."
            )
    total_pnls = [item.total_pnl for item in horizons]
    if _is_nondecreasing(total_pnls):
        lines.append("- total PnL scales up with horizon length for this stack.")
    else:
        lines.append("- total PnL is not monotonic across horizons.")
    rois = [item.average_roi for item in horizons]
    if all(value is not None for value in rois):
        if _is_nonincreasing([value for value in rois if value is not None]):
            lines.append("- average ROI compresses or stays flat as horizon length increases.")
        else:
            lines.append("- average ROI is not monotonic across horizons.")
    return lines


def _is_nondecreasing(values: Sequence[Decimal]) -> bool:
    return all(left <= right for left, right in zip(values, values[1:]))


def _is_nonincreasing(values: Sequence[Decimal]) -> bool:
    return all(left >= right for left, right in zip(values, values[1:]))


def _build_stack_result(
    *,
    stack_name: str,
    sessions: Sequence[HorizonSession],
    stack_payloads_by_session: Mapping[str, Mapping[str, object]],
) -> CrossHorizonStackResult:
    session_entries: list[StackHorizonMetrics] = []
    stack_role: str | None = None
    for session in sessions:
        payload = stack_payloads_by_session[session.label]
        stack_payload = _stack_payload_by_name(payload, stack_name)
        if stack_role is None:
            stack_role = str(stack_payload["stack_role"])
        session_entries.append(
            StackHorizonMetrics(
                session_label=session.label,
                session_id=session.session_id,
                capture_date=session.capture_date,
                snapshot_count=int(stack_payload["snapshot_count"]),
                window_count=int(stack_payload["window_count"]),
                trade_count=int(stack_payload["trade_count"]),
                hit_rate=_optional_decimal(stack_payload.get("hit_rate")),
                average_selected_net_edge=_optional_decimal(
                    stack_payload.get("average_selected_net_edge")
                ),
                total_pnl=_required_decimal(stack_payload["total_pnl"]),
                average_roi=_optional_decimal(stack_payload.get("average_roi")),
                pnl_per_window=_optional_decimal(stack_payload.get("pnl_per_window")),
                pnl_per_1000_snapshots=_optional_decimal(
                    stack_payload.get("pnl_per_1000_snapshots")
                ),
                pnl_per_100_trades=_optional_decimal(
                    stack_payload.get("pnl_per_100_trades")
                ),
            )
        )
    return CrossHorizonStackResult(
        stack_name=stack_name,
        stack_role=stack_role or "",
        horizons=tuple(session_entries),
    )


def _session_summary(session: HorizonSession) -> HorizonSessionSummary:
    payload = json.loads(Path(session.admission_summary_path).read_text(encoding="utf-8"))
    return HorizonSessionSummary(
        label=session.label,
        session_id=session.session_id,
        capture_date=session.capture_date,
        admission_verdict=str(payload["verdict"]),
        legacy_verdict=_optional_str(payload.get("legacy_verdict")),
        snapshot_eligible_sample_ratio=_optional_decimal(
            payload.get("snapshot_eligibility", {}).get("snapshot_eligible_sample_ratio")
        ),
        off_family_switch_count=int(
            payload.get("family_validation", {}).get("off_family_switch_count", 0)
        ),
        selected_binding_unresolved_window_count=int(
            payload.get("mapping_and_anchor", {}).get(
                "selected_binding_unresolved_window_count",
                0,
            )
        ),
        window_verdict_counts={
            str(key): int(value)
            for key, value in payload.get("polymarket_continuity", {})
            .get("window_verdict_counts", {})
            .items()
        },
    )


def _session_from_payload(payload: Mapping[str, object]) -> HorizonSession:
    return HorizonSession(
        label=str(payload["label"]),
        session_id=str(payload["session_id"]),
        capture_date=str(payload["capture_date"]),
        baseline_manifest_path=str(payload["baseline_manifest_path"]),
        baseline_note_path=str(payload["baseline_note_path"]),
        summary_path=str(payload["summary_path"]),
        admission_summary_path=str(payload["admission_summary_path"]),
        policy_stack_summary_path=str(payload["policy_stack_summary_path"]),
    )


def _load_policy_stack_summary(path: Path) -> Mapping[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stack_payload_by_name(payload: Mapping[str, object], stack_name: str) -> Mapping[str, object]:
    for item in _payload_sequence(payload["stacks"]):
        item_mapping = _payload_mapping(item)
        if str(item_mapping["stack_name"]) == stack_name:
            return item_mapping
    raise KeyError(f"stack `{stack_name}` not found in policy-stack summary")


def _payload_sequence(value: object) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"expected a sequence payload, got {type(value)!r}")
    return value


def _payload_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"expected a mapping payload, got {type(value)!r}")
    return value


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _required_decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


__all__ = [
    "CrossHorizonComparison",
    "CrossHorizonStackResult",
    "HorizonSession",
    "HorizonSessionSummary",
    "StackHorizonMetrics",
    "build_cross_horizon_comparison",
    "cross_horizon_comparison_to_dict",
    "load_cross_horizon_manifest",
    "render_cross_horizon_report",
]
