from __future__ import annotations

import json
from pathlib import Path

from rtds.cli.export_window_quality_summary import export_window_quality_summary
from rtds.storage.writer import write_json_file


def test_export_window_quality_summary_writes_combined_reference_rows(tmp_path: Path) -> None:
    session_one_admission = (
        tmp_path
        / "artifacts"
        / "collect"
        / "date=2026-03-16"
        / "session=s1"
        / "admission_summary.json"
    )
    session_two_admission = (
        tmp_path
        / "artifacts"
        / "collect"
        / "date=2026-03-17"
        / "session=s2"
        / "admission_summary.json"
    )
    write_json_file(
        session_one_admission,
        {
            "polymarket_continuity": {
                "window_quality_classifier": {
                    "classifier_version": "window_quality_v1",
                    "config_path": "configs/replay/window_quality_classifier_v1.json",
                },
                "window_verdict_counts": {"good": 1},
                "window_quote_coverage": [{"window_id": "w1", "window_verdict": "good"}],
            }
        },
    )
    write_json_file(
        session_two_admission,
        {
            "polymarket_continuity": {
                "window_quality_classifier": {
                    "classifier_version": "window_quality_v1",
                    "config_path": "configs/replay/window_quality_classifier_v1.json",
                },
                "window_verdict_counts": {"degraded_light": 1},
                "window_quote_coverage": [
                    {"window_id": "w2", "window_verdict": "degraded_light"}
                ],
            }
        },
    )
    manifest_path = tmp_path / "configs" / "baselines" / "analysis" / "task7_reference_runs.json"
    write_json_file(
        manifest_path,
        {
            "analysis_id": "task7-degraded-regime-reference-runs",
            "comparison_config_path": "configs/replay/task7_reference_comparison.yaml",
            "quality_label_source": (
                "capture admission_summary.json per-window window_verdict rows"
            ),
            "sessions": [
                {
                    "label": "baseline_6h",
                    "session_id": "s1",
                    "capture_date": "2026-03-16",
                    "admission_summary_path": str(session_one_admission),
                },
                {
                    "label": "pilot_12h",
                    "session_id": "s2",
                    "capture_date": "2026-03-17",
                    "admission_summary_path": str(session_two_admission),
                },
            ],
        },
    )

    output_path = export_window_quality_summary(
        manifest_path=manifest_path,
        output_root=tmp_path / "artifacts",
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["analysis_id"] == "task7-degraded-regime-reference-runs"
    assert len(payload["sessions"]) == 2
    assert payload["sessions"][0]["window_verdict_counts"] == {"good": 1}
    assert payload["sessions"][1]["windows"][0]["window_verdict"] == "degraded_light"
