"""Compare bucket-level calibration profiles across two sessions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from rtds.storage.writer import serialize_value

DEFAULT_DIMENSIONS: tuple[str, ...] = (
    "hour_utc",
    "seconds_remaining_bucket",
    "volatility_regime",
    "spread_bucket",
)


@dataclass(slots=True, frozen=True)
class CalibrationProfileSession:
    """One calibration profile artifact loaded from disk."""

    profile_path: str
    analysis: str
    session_id: str
    capture_date: str
    session_label: str
    frozen_manifest_path: str
    skipped_missing_calibrated_rows: int
    raw_total_pnl: Decimal
    calibrated_total_pnl: Decimal
    delta_total_pnl: Decimal
    raw_trade_count: int
    calibrated_trade_count: int
    delta_trade_count: int
    bucket_profiles: Mapping[str, Mapping[str, Sequence[Mapping[str, object]]]]


@dataclass(slots=True, frozen=True)
class ComparedSlice:
    """One side-by-side slice row across two calibration profiles."""

    value: str
    left_raw_trade_count: int
    left_calibrated_trade_count: int
    left_raw_pnl: Decimal
    left_calibrated_pnl: Decimal
    left_delta_pnl: Decimal
    left_raw_hit_rate: Decimal | None
    left_calibrated_hit_rate: Decimal | None
    left_raw_avg_edge: Decimal | None
    left_calibrated_avg_edge: Decimal | None
    right_raw_trade_count: int
    right_calibrated_trade_count: int
    right_raw_pnl: Decimal
    right_calibrated_pnl: Decimal
    right_delta_pnl: Decimal
    right_raw_hit_rate: Decimal | None
    right_calibrated_hit_rate: Decimal | None
    right_raw_avg_edge: Decimal | None
    right_calibrated_avg_edge: Decimal | None


@dataclass(slots=True, frozen=True)
class BucketDimensionComparison:
    """One bucket/dimension comparison table."""

    bucket: str
    dimension: str
    rows: tuple[ComparedSlice, ...]


@dataclass(slots=True, frozen=True)
class CalibrationProfileComparison:
    """Full side-by-side comparison across two calibration profile artifacts."""

    analysis_id: str
    description: str
    left: CalibrationProfileSession
    right: CalibrationProfileSession
    buckets: tuple[str, ...]
    dimensions: tuple[str, ...]
    comparisons: tuple[BucketDimensionComparison, ...]


def load_calibration_profile(path: str | Path) -> CalibrationProfileSession:
    """Load one calibration profile artifact from JSON."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    session_summary = payload["session_summary"]
    return CalibrationProfileSession(
        profile_path=str(path),
        analysis=str(payload["analysis"]),
        session_id=str(payload["session_id"]),
        capture_date=str(payload["capture_date"]),
        session_label=str(payload["session_label"]),
        frozen_manifest_path=str(payload["frozen_manifest_path"]),
        skipped_missing_calibrated_rows=int(payload["skipped_missing_calibrated_rows"]),
        raw_total_pnl=_required_decimal(session_summary["raw_total_pnl"]),
        calibrated_total_pnl=_required_decimal(session_summary["calibrated_total_pnl"]),
        delta_total_pnl=_required_decimal(session_summary["delta_total_pnl"]),
        raw_trade_count=int(session_summary["raw_trade_count"]),
        calibrated_trade_count=int(session_summary["calibrated_trade_count"]),
        delta_trade_count=int(session_summary["delta_trade_count"]),
        bucket_profiles=payload["bucket_profiles"],
    )


def build_calibration_profile_comparison(
    *,
    left: CalibrationProfileSession,
    right: CalibrationProfileSession,
    buckets: Sequence[str],
    dimensions: Sequence[str] = DEFAULT_DIMENSIONS,
    analysis_id: str = "12h-vs-24h-calibration-diagnosis",
    description: str = (
        "Direct side-by-side comparison of calibrated bucket behavior across "
        "the 12-hour anomaly and the 24-hour validation session."
    ),
) -> CalibrationProfileComparison:
    """Build one side-by-side comparison for selected buckets and dimensions."""

    comparison_rows: list[BucketDimensionComparison] = []
    for bucket in buckets:
        for dimension in dimensions:
            comparison_rows.append(
                BucketDimensionComparison(
                    bucket=bucket,
                    dimension=dimension,
                    rows=_merge_dimension_rows(
                        left.bucket_profiles.get(bucket, {}).get(dimension, []),
                        right.bucket_profiles.get(bucket, {}).get(dimension, []),
                        dimension,
                    ),
                )
            )
    return CalibrationProfileComparison(
        analysis_id=analysis_id,
        description=description,
        left=left,
        right=right,
        buckets=tuple(buckets),
        dimensions=tuple(dimensions),
        comparisons=tuple(comparison_rows),
    )


def calibration_profile_comparison_to_dict(
    comparison: CalibrationProfileComparison,
) -> dict[str, object]:
    """Serialize one side-by-side comparison to JSON."""

    return {
        "analysis_id": comparison.analysis_id,
        "description": comparison.description,
        "buckets": list(comparison.buckets),
        "dimensions": list(comparison.dimensions),
        "left": serialize_value(comparison.left),
        "right": serialize_value(comparison.right),
        "comparisons": [
            {
                "bucket": item.bucket,
                "dimension": item.dimension,
                "rows": [serialize_value(row) for row in item.rows],
            }
            for item in comparison.comparisons
        ],
    }


def render_calibration_profile_comparison(
    comparison: CalibrationProfileComparison,
) -> str:
    """Render one side-by-side calibration profile report."""

    lines = [
        "# 12h vs 24h Far-Up / Lean-Up Diagnosis",
        "",
        comparison.description,
        "",
        "## Sessions",
        f"### Left: {comparison.left.session_label}",
        f"- session_id: `{comparison.left.session_id}`",
        f"- profile_path: `{comparison.left.profile_path}`",
        f"- raw_total_pnl: `{comparison.left.raw_total_pnl}`",
        f"- calibrated_total_pnl: `{comparison.left.calibrated_total_pnl}`",
        f"- delta_total_pnl: `{comparison.left.delta_total_pnl}`",
        f"- skipped_missing_calibrated_rows: `{comparison.left.skipped_missing_calibrated_rows}`",
        "",
        f"### Right: {comparison.right.session_label}",
        f"- session_id: `{comparison.right.session_id}`",
        f"- profile_path: `{comparison.right.profile_path}`",
        f"- raw_total_pnl: `{comparison.right.raw_total_pnl}`",
        f"- calibrated_total_pnl: `{comparison.right.calibrated_total_pnl}`",
        f"- delta_total_pnl: `{comparison.right.delta_total_pnl}`",
        f"- skipped_missing_calibrated_rows: `{comparison.right.skipped_missing_calibrated_rows}`",
        "",
        "## Verdict",
    ]
    lines.extend(_render_verdict_lines(comparison))
    lines.extend(["", "## Comparisons"])
    for item in comparison.comparisons:
        lines.extend(
            [
                f"### {item.bucket} / {item.dimension}",
                "",
                _TABLE_HEADER,
                _TABLE_DIVIDER,
            ]
        )
        for row in item.rows:
            lines.append(
                _render_table_row(row)
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def _render_verdict_lines(comparison: CalibrationProfileComparison) -> list[str]:
    lines: list[str] = []
    by_key = {
        (item.bucket, item.dimension): item
        for item in comparison.comparisons
    }
    far_up_vol = by_key.get(("far_up", "volatility_regime"))
    far_up_spread = by_key.get(("far_up", "spread_bucket"))
    far_up_time = by_key.get(("far_up", "seconds_remaining_bucket"))
    lean_up_vol = by_key.get(("lean_up", "volatility_regime"))
    lean_up_spread = by_key.get(("lean_up", "spread_bucket"))
    far_down_vol = by_key.get(("far_down", "volatility_regime"))

    if far_up_vol is not None:
        low = _find_value(far_up_vol.rows, "low_vol")
        lines.append(
            "- `far_up` does not show the same sign pattern across the two sessions: "
            f"`low_vol` is {low.left_delta_pnl} on 12h and "
            f"{low.right_delta_pnl} on 24h."
        )
    if far_up_spread is not None:
        tight = _find_value(far_up_spread.rows, "tight_spread")
        lines.append(
            "- `far_up` is not a recurring `tight_spread` failure: "
            f"12h delta `{tight.left_delta_pnl}` vs 24h delta `{tight.right_delta_pnl}`."
        )
    if far_up_time is not None:
        early = _find_value(far_up_time.rows, "early_window")
        mid = _find_value(far_up_time.rows, "mid_window")
        lines.append(
            "- `far_up` flips sign by session in the same time buckets: "
            f"`early` {early.left_delta_pnl} vs {early.right_delta_pnl}, "
            f"`mid` {mid.left_delta_pnl} vs {mid.right_delta_pnl}."
        )
    if lean_up_vol is not None and lean_up_spread is not None:
        low = _find_value(lean_up_vol.rows, "low_vol")
        tight = _find_value(lean_up_spread.rows, "tight_spread")
        lines.append(
            "- `lean_up` behaves like a weaker version of the same up-side phenomenon: "
            f"`low_vol` {low.left_delta_pnl} vs {low.right_delta_pnl}, "
            f"`tight_spread` {tight.left_delta_pnl} vs {tight.right_delta_pnl}."
        )
    if far_down_vol is not None:
        low = _find_value(far_down_vol.rows, "low_vol")
        lines.append(
            "- `far_down` remains a stable control bucket: "
            f"`low_vol` improves in both sessions "
            f"({low.left_delta_pnl if low.left_delta_pnl is not None else 'n/a'} / "
            f"{low.right_delta_pnl})."
        )
    lines.append(
        "- Current evidence favors a session-specific anomaly, not a recurring "
        "time/vol/spread context failure. The same coarse contexts that hurt 12h "
        "improve materially on 24h."
    )
    lines.append(
        "- If a Stage 2 gate is attempted later, it likely needs a persistence-style "
        "signal rather than a simple `time_remaining`, `volatility_regime`, or "
        "`spread_bucket` gate."
    )
    return lines


def _merge_dimension_rows(
    left_rows: Sequence[Mapping[str, object]],
    right_rows: Sequence[Mapping[str, object]],
    dimension: str,
) -> tuple[ComparedSlice, ...]:
    left_by_value = {str(row[dimension]): row for row in left_rows}
    right_by_value = {str(row[dimension]): row for row in right_rows}
    all_values = sorted(
        set(left_by_value) | set(right_by_value),
        key=lambda item: _sort_key(dimension, item),
    )
    return tuple(
        ComparedSlice(
            value=value,
            left_raw_trade_count=_int_value(left_by_value.get(value), "raw_trade_count"),
            left_calibrated_trade_count=_int_value(
                left_by_value.get(value), "calibrated_trade_count"
            ),
            left_raw_pnl=_decimal_value(left_by_value.get(value), "raw_pnl"),
            left_calibrated_pnl=_decimal_value(
                left_by_value.get(value), "calibrated_pnl"
            ),
            left_delta_pnl=_decimal_value(left_by_value.get(value), "delta_pnl"),
            left_raw_hit_rate=_optional_decimal_value(
                left_by_value.get(value), "raw_hit_rate"
            ),
            left_calibrated_hit_rate=_optional_decimal_value(
                left_by_value.get(value), "calibrated_hit_rate"
            ),
            left_raw_avg_edge=_optional_decimal_value(
                left_by_value.get(value), "raw_avg_edge"
            ),
            left_calibrated_avg_edge=_optional_decimal_value(
                left_by_value.get(value), "calibrated_avg_edge"
            ),
            right_raw_trade_count=_int_value(
                right_by_value.get(value), "raw_trade_count"
            ),
            right_calibrated_trade_count=_int_value(
                right_by_value.get(value), "calibrated_trade_count"
            ),
            right_raw_pnl=_decimal_value(right_by_value.get(value), "raw_pnl"),
            right_calibrated_pnl=_decimal_value(
                right_by_value.get(value), "calibrated_pnl"
            ),
            right_delta_pnl=_decimal_value(right_by_value.get(value), "delta_pnl"),
            right_raw_hit_rate=_optional_decimal_value(
                right_by_value.get(value), "raw_hit_rate"
            ),
            right_calibrated_hit_rate=_optional_decimal_value(
                right_by_value.get(value), "calibrated_hit_rate"
            ),
            right_raw_avg_edge=_optional_decimal_value(
                right_by_value.get(value), "raw_avg_edge"
            ),
            right_calibrated_avg_edge=_optional_decimal_value(
                right_by_value.get(value), "calibrated_avg_edge"
            ),
        )
        for value in all_values
    )


def _sort_key(dimension: str, value: str) -> tuple[int, str]:
    if dimension == "hour_utc":
        return (int(value), value)
    if dimension == "seconds_remaining_bucket":
        order = {
            "early_window": 0,
            "mid_window": 1,
            "late_window": 2,
        }
        return (order.get(value, 99), value)
    if dimension == "volatility_regime":
        order = {"low_vol": 0, "mid_vol": 1, "high_vol": 2}
        return (order.get(value, 99), value)
    if dimension == "spread_bucket":
        order = {
            "tight_spread": 0,
            "medium_spread": 1,
            "wide_spread": 2,
            "unknown_spread": 3,
        }
        return (order.get(value, 99), value)
    return (99, value)


def _find_value(rows: Sequence[ComparedSlice], value: str) -> ComparedSlice:
    for row in rows:
        if row.value == value:
            return row
    raise KeyError(value)


def _int_value(payload: Mapping[str, object] | None, key: str) -> int:
    if payload is None:
        return 0
    return int(payload[key])


def _decimal_value(payload: Mapping[str, object] | None, key: str) -> Decimal:
    if payload is None:
        return Decimal("0")
    return _required_decimal(payload[key])


def _optional_decimal_value(
    payload: Mapping[str, object] | None,
    key: str,
) -> Decimal | None:
    if payload is None:
        return None
    value = payload[key]
    if value is None:
        return None
    return _required_decimal(value)


def _required_decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _fmt_optional(value: Decimal | None) -> str:
    return "" if value is None else str(value)


_TABLE_HEADER = (
    "| value | 12h raw trades | 12h cal trades | 12h raw pnl | 12h cal pnl | "
    "12h delta pnl | 12h raw hit | 12h cal hit | 12h raw edge | 12h cal edge | "
    "24h raw trades | 24h cal trades | 24h raw pnl | 24h cal pnl | 24h delta pnl | "
    "24h raw hit | 24h cal hit | 24h raw edge | 24h cal edge |"
)
_TABLE_DIVIDER = (
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    "---:|---:|---:|---:|---:|---:|---:|---:|---:|"
)


def _render_table_row(row: ComparedSlice) -> str:
    return (
        "| {value} | {l_rt} | {l_ct} | {l_rp} | {l_cp} | {l_dp} | {l_rh} | "
        "{l_ch} | {l_re} | {l_ce} | {r_rt} | {r_ct} | {r_rp} | {r_cp} | "
        "{r_dp} | {r_rh} | {r_ch} | {r_re} | {r_ce} |"
    ).format(
        value=row.value,
        l_rt=row.left_raw_trade_count,
        l_ct=row.left_calibrated_trade_count,
        l_rp=row.left_raw_pnl,
        l_cp=row.left_calibrated_pnl,
        l_dp=row.left_delta_pnl,
        l_rh=_fmt_optional(row.left_raw_hit_rate),
        l_ch=_fmt_optional(row.left_calibrated_hit_rate),
        l_re=_fmt_optional(row.left_raw_avg_edge),
        l_ce=_fmt_optional(row.left_calibrated_avg_edge),
        r_rt=row.right_raw_trade_count,
        r_ct=row.right_calibrated_trade_count,
        r_rp=row.right_raw_pnl,
        r_cp=row.right_calibrated_pnl,
        r_dp=row.right_delta_pnl,
        r_rh=_fmt_optional(row.right_raw_hit_rate),
        r_ch=_fmt_optional(row.right_calibrated_hit_rate),
        r_re=_fmt_optional(row.right_raw_avg_edge),
        r_ce=_fmt_optional(row.right_calibrated_avg_edge),
    )
