"""Export pinned capture-session window-quality verdicts for analysis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from rtds.collectors.session_baseline import refresh_capture_admission_from_summary
from rtds.storage.writer import write_json_file

DEFAULT_MANIFEST_PATH = "configs/baselines/analysis/task7_reference_runs.json"
DEFAULT_OUTPUT_ROOT = "artifacts"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    output_path = export_window_quality_summary(
        manifest_path=args.manifest_path,
        output_root=Path(args.output_root),
    )
    print(output_path)
    return 0


def export_window_quality_summary(
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST_PATH,
    output_root: Path = Path(DEFAULT_OUTPUT_ROOT),
) -> Path:
    """Write one combined Task 7 window-quality summary for pinned reference runs."""

    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    sessions_payload: list[dict[str, Any]] = []
    for session in manifest["sessions"]:
        summary_path = session.get("summary_path")
        if summary_path is not None:
            admission_summary_path = refresh_capture_admission_from_summary(str(summary_path))
        else:
            admission_summary_path = Path(str(session["admission_summary_path"]))
        admission_summary = json.loads(admission_summary_path.read_text(encoding="utf-8"))
        continuity = dict(admission_summary["polymarket_continuity"])
        sessions_payload.append(
            {
                "label": str(session["label"]),
                "session_id": str(session["session_id"]),
                "capture_date": str(session["capture_date"]),
                "admission_summary_path": str(admission_summary_path),
                "classifier": dict(continuity["window_quality_classifier"]),
                "window_verdict_counts": dict(continuity["window_verdict_counts"]),
                "windows": list(continuity["window_quote_coverage"]),
            }
        )

    output_path = output_root / "analysis" / "task7_reference_runs" / "window_quality_summary.json"
    write_json_file(
        output_path,
        {
            "analysis_id": str(manifest["analysis_id"]),
            "manifest_path": str(Path(manifest_path)),
            "comparison_config_path": str(manifest["comparison_config_path"]),
            "quality_label_source": str(manifest["quality_label_source"]),
            "sessions": sessions_payload,
        },
    )
    return output_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-path",
        default=DEFAULT_MANIFEST_PATH,
        help="Path to the pinned Task 7 reference-runs manifest.",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Root output directory for the combined summary artifact.",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
