from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from rtds.replay.policy_cross_horizon import (
    CrossHorizonComparison,
    CrossHorizonStackResult,
    HorizonSessionSummary,
    StackHorizonMetrics,
    build_cross_horizon_comparison,
    cross_horizon_comparison_to_dict,
    render_cross_horizon_report,
)
from rtds.storage.writer import write_json_file


def test_build_cross_horizon_comparison_aligns_stack_metrics(tmp_path: Path) -> None:
    session_a_admission = tmp_path / "session-a-admission.json"
    session_b_admission = tmp_path / "session-b-admission.json"
    session_a_stack = tmp_path / "session-a-stack.json"
    session_b_stack = tmp_path / "session-b-stack.json"

    write_json_file(
        session_a_admission,
        {
            "verdict": "conditionally_admissible",
            "legacy_verdict": "not_admissible",
            "snapshot_eligibility": {"snapshot_eligible_sample_ratio": "0.93"},
            "family_validation": {"off_family_switch_count": 0},
            "mapping_and_anchor": {"selected_binding_unresolved_window_count": 0},
            "polymarket_continuity": {"window_verdict_counts": {"good": 12}},
        },
    )
    write_json_file(
        session_b_admission,
        {
            "verdict": "conditionally_admissible",
            "legacy_verdict": "not_admissible",
            "snapshot_eligibility": {"snapshot_eligible_sample_ratio": "0.94"},
            "family_validation": {"off_family_switch_count": 0},
            "mapping_and_anchor": {"selected_binding_unresolved_window_count": 0},
            "polymarket_continuity": {"window_verdict_counts": {"good": 19}},
        },
    )
    write_json_file(
        session_a_stack,
        {
            "stacks": [
                {
                    "stack_name": "baseline_only",
                    "stack_role": "baseline",
                    "snapshot_count": 100,
                    "window_count": 12,
                    "trade_count": 80,
                    "hit_rate": "0.55",
                    "average_selected_net_edge": "0.17",
                    "total_pnl": "12.5",
                    "average_roi": "0.45",
                    "pnl_per_window": "1.0",
                    "pnl_per_1000_snapshots": "125.0",
                    "pnl_per_100_trades": "15.625",
                },
                {
                    "stack_name": "baseline_plus_degraded_light",
                    "stack_role": "exploratory_overlay",
                    "snapshot_count": 200,
                    "window_count": 24,
                    "trade_count": 150,
                    "hit_rate": "0.52",
                    "average_selected_net_edge": "0.15",
                    "total_pnl": "15.0",
                    "average_roi": "0.40",
                    "pnl_per_window": "0.625",
                    "pnl_per_1000_snapshots": "75.0",
                    "pnl_per_100_trades": "10.0",
                },
            ]
        },
    )
    write_json_file(
        session_b_stack,
        {
            "stacks": [
                {
                    "stack_name": "baseline_only",
                    "stack_role": "baseline",
                    "snapshot_count": 300,
                    "window_count": 19,
                    "trade_count": 250,
                    "hit_rate": "0.60",
                    "average_selected_net_edge": "0.16",
                    "total_pnl": "30.0",
                    "average_roi": "0.42",
                    "pnl_per_window": "1.5789",
                    "pnl_per_1000_snapshots": "100.0",
                    "pnl_per_100_trades": "12.0",
                },
                {
                    "stack_name": "baseline_plus_degraded_light",
                    "stack_role": "exploratory_overlay",
                    "snapshot_count": 500,
                    "window_count": 61,
                    "trade_count": 430,
                    "hit_rate": "0.58",
                    "average_selected_net_edge": "0.14",
                    "total_pnl": "40.0",
                    "average_roi": "0.38",
                    "pnl_per_window": "0.6557",
                    "pnl_per_1000_snapshots": "80.0",
                    "pnl_per_100_trades": "9.3023",
                },
            ]
        },
    )

    comparison = build_cross_horizon_comparison(
        {
            "analysis_id": "policy-v1-cross-horizon",
            "description": "Pinned policy-stack comparison across horizons.",
            "comparison_config_path": "configs/replay/task7_reference_comparison.yaml",
            "stack_order": [
                "baseline_only",
                "baseline_plus_degraded_light",
            ],
            "metrics": [
                "trade_count",
                "hit_rate",
                "average_selected_net_edge",
                "total_pnl",
                "average_roi",
                "pnl_per_window",
                "pnl_per_100_trades",
                "pnl_per_1000_snapshots",
            ],
            "sessions": [
                {
                    "label": "baseline_6h",
                    "session_id": "session-a",
                    "capture_date": "2026-03-16",
                    "baseline_manifest_path": "configs/baselines/capture/session-a.json",
                    "baseline_note_path": "docs/baselines/session-a.md",
                    "summary_path": "artifacts/collect/session-a/summary.json",
                    "admission_summary_path": str(session_a_admission),
                    "policy_stack_summary_path": str(session_a_stack),
                },
                {
                    "label": "pilot_12h",
                    "session_id": "session-b",
                    "capture_date": "2026-03-17",
                    "baseline_manifest_path": "configs/baselines/capture/session-b.json",
                    "baseline_note_path": "docs/baselines/session-b.md",
                    "summary_path": "artifacts/collect/session-b/summary.json",
                    "admission_summary_path": str(session_b_admission),
                    "policy_stack_summary_path": str(session_b_stack),
                },
            ],
        }
    )

    assert [session.label for session in comparison.sessions] == [
        "baseline_6h",
        "pilot_12h",
    ]
    assert [result.stack_name for result in comparison.stack_results] == [
        "baseline_only",
        "baseline_plus_degraded_light",
    ]
    assert comparison.stack_results[0].horizons[0].trade_count == 80
    assert comparison.stack_results[0].horizons[1].trade_count == 250

    payload = cross_horizon_comparison_to_dict(comparison)
    assert payload["analysis_id"] == "policy-v1-cross-horizon"
    assert payload["stack_results"][0]["horizons"][1]["trade_count"] == 250

    report = render_cross_horizon_report(comparison)
    assert "Policy Stack Cross-Horizon Comparison" in report
    assert "baseline_plus_degraded_light" in report
    assert "pilot_12h: trades=430" in report
    assert "- total PnL scales up with horizon length for this stack." in report


def test_render_cross_horizon_report_marks_non_monotonic_paths() -> None:
    comparison = CrossHorizonComparison(
        analysis_id="policy-v1-cross-horizon",
        description="Pinned policy-stack comparison across horizons.",
        comparison_config_path="configs/replay/task7_reference_comparison.yaml",
        stack_order=("baseline_only",),
        metrics=(
            "trade_count",
            "hit_rate",
            "average_selected_net_edge",
            "total_pnl",
            "average_roi",
            "pnl_per_window",
            "pnl_per_100_trades",
            "pnl_per_1000_snapshots",
        ),
        sessions=(
            HorizonSessionSummary(
                label="short",
                session_id="a",
                capture_date="2026-03-16",
                admission_verdict="conditionally_admissible",
                legacy_verdict="not_admissible",
                snapshot_eligible_sample_ratio=Decimal("0.9"),
                off_family_switch_count=0,
                selected_binding_unresolved_window_count=0,
                window_verdict_counts={"good": 10},
            ),
            HorizonSessionSummary(
                label="mid",
                session_id="b",
                capture_date="2026-03-17",
                admission_verdict="conditionally_admissible",
                legacy_verdict="not_admissible",
                snapshot_eligible_sample_ratio=Decimal("0.9"),
                off_family_switch_count=0,
                selected_binding_unresolved_window_count=0,
                window_verdict_counts={"good": 20},
            ),
            HorizonSessionSummary(
                label="long",
                session_id="c",
                capture_date="2026-03-20",
                admission_verdict="conditionally_admissible",
                legacy_verdict="not_admissible",
                snapshot_eligible_sample_ratio=Decimal("0.9"),
                off_family_switch_count=0,
                selected_binding_unresolved_window_count=0,
                window_verdict_counts={"good": 30},
            ),
        ),
        stack_results=(
            CrossHorizonStackResult(
                stack_name="baseline_only",
                stack_role="baseline",
                horizons=(
                    StackHorizonMetrics(
                        session_label="short",
                        session_id="a",
                        capture_date="2026-03-16",
                        snapshot_count=100,
                        window_count=10,
                        trade_count=80,
                        hit_rate=Decimal("0.50"),
                        average_selected_net_edge=Decimal("0.20"),
                        total_pnl=Decimal("10"),
                        average_roi=Decimal("0.30"),
                        pnl_per_window=Decimal("1"),
                        pnl_per_1000_snapshots=Decimal("100"),
                        pnl_per_100_trades=Decimal("12.5"),
                    ),
                    StackHorizonMetrics(
                        session_label="mid",
                        session_id="b",
                        capture_date="2026-03-17",
                        snapshot_count=200,
                        window_count=20,
                        trade_count=150,
                        hit_rate=Decimal("0.55"),
                        average_selected_net_edge=Decimal("0.18"),
                        total_pnl=Decimal("30"),
                        average_roi=Decimal("0.40"),
                        pnl_per_window=Decimal("1.5"),
                        pnl_per_1000_snapshots=Decimal("150"),
                        pnl_per_100_trades=Decimal("20"),
                    ),
                    StackHorizonMetrics(
                        session_label="long",
                        session_id="c",
                        capture_date="2026-03-20",
                        snapshot_count=300,
                        window_count=30,
                        trade_count=220,
                        hit_rate=Decimal("0.45"),
                        average_selected_net_edge=Decimal("0.19"),
                        total_pnl=Decimal("25"),
                        average_roi=Decimal("0.35"),
                        pnl_per_window=Decimal("0.83"),
                        pnl_per_1000_snapshots=Decimal("83"),
                        pnl_per_100_trades=Decimal("11.36"),
                    ),
                ),
            ),
        ),
    )

    report = render_cross_horizon_report(comparison)

    assert "- total PnL is not monotonic across horizons." in report
    assert "- average selected net edge is not monotonic across horizons." in report
    assert "- average ROI is not monotonic across horizons." in report
