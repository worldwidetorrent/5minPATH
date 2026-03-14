from datetime import UTC, date, datetime, timedelta

import pytest

from rtds.core.ids import build_window_id, parse_window_id, validate_window_id
from rtds.core.time import age_ms, floor_to_5m, format_utc, parse_utc, seconds_remaining, window_end
from rtds.mapping.window_ids import (
    WINDOWS_PER_DAY,
    daily_window_schedule,
    generate_window_strip,
    generate_window_strip_for_horizon,
    get_window_bounds,
    owning_window_id,
    owning_window_start,
)


def test_window_id_round_trip() -> None:
    start_ts = datetime(2026, 3, 13, 12, 5, tzinfo=UTC)
    window_id = build_window_id("BTC", start_ts)
    asset_id, parsed_start_ts = parse_window_id(str(window_id))

    assert window_id == "btc-5m-20260313T120500Z"
    assert asset_id.value == "BTC"
    assert parsed_start_ts == start_ts
    assert validate_window_id(str(window_id)) == window_id


def test_window_id_rejects_unaligned_boundaries() -> None:
    with pytest.raises(ValueError):
        build_window_id("BTC", datetime(2026, 3, 13, 12, 6, tzinfo=UTC))

    with pytest.raises(ValueError):
        validate_window_id("btc-5m-20260313T120601Z")


def test_time_helpers_follow_utc_and_window_rules() -> None:
    ts = parse_utc("2026-03-13T12:07:03.250000Z")
    assert format_utc(ts, timespec="milliseconds") == "2026-03-13T12:07:03.250Z"
    assert floor_to_5m(ts) == datetime(2026, 3, 13, 12, 5, tzinfo=UTC)
    assert window_end(datetime(2026, 3, 13, 12, 5, tzinfo=UTC)) == datetime(
        2026,
        3,
        13,
        12,
        10,
        tzinfo=UTC,
    )
    assert seconds_remaining(
        datetime(2026, 3, 13, 12, 10, tzinfo=UTC),
        datetime(2026, 3, 13, 12, 7, 15, tzinfo=UTC),
    ) == 165
    assert age_ms(
        datetime(2026, 3, 13, 12, 7, 3, 250000, tzinfo=UTC),
        datetime(2026, 3, 13, 12, 7, 2, tzinfo=UTC),
    ) == 1250


def test_mapping_window_ids_floor_to_owning_window() -> None:
    ts = datetime(2026, 3, 13, 12, 7, 3, 250000, tzinfo=UTC)

    assert owning_window_start(ts) == datetime(2026, 3, 13, 12, 5, tzinfo=UTC)
    assert owning_window_id(ts) == "btc-5m-20260313T120500Z"


def test_get_window_bounds_round_trip_from_window_id() -> None:
    bounds = get_window_bounds("btc-5m-20260313T120500Z")

    assert bounds.window_start_ts == datetime(2026, 3, 13, 12, 5, tzinfo=UTC)
    assert bounds.window_end_ts == datetime(2026, 3, 13, 12, 10, tzinfo=UTC)


def test_generate_daily_schedule_returns_288_windows() -> None:
    schedule = daily_window_schedule(date(2026, 3, 13))

    assert len(schedule) == WINDOWS_PER_DAY
    assert schedule[0].window_id == "btc-5m-20260313T000000Z"
    assert schedule[-1].window_id == "btc-5m-20260313T235500Z"
    assert schedule[-1].window_end_ts == datetime(2026, 3, 14, 0, 0, tzinfo=UTC)


def test_generate_window_strip_for_horizon_uses_window_ownership() -> None:
    strip = generate_window_strip_for_horizon(
        datetime(2026, 3, 13, 12, 7, tzinfo=UTC),
        horizon=timedelta(minutes=16),
    )

    assert strip == [
        "btc-5m-20260313T120500Z",
        "btc-5m-20260313T121000Z",
        "btc-5m-20260313T121500Z",
        "btc-5m-20260313T122000Z",
    ]


def test_generate_window_strip_supports_date_and_period_modes() -> None:
    by_date = generate_window_strip(date(2026, 3, 13))
    by_period = generate_window_strip(
        datetime(2026, 3, 13, 12, 7, tzinfo=UTC),
        periods=3,
    )

    assert len(by_date) == WINDOWS_PER_DAY
    assert [window.window_id for window in by_period] == [
        "btc-5m-20260313T120500Z",
        "btc-5m-20260313T121000Z",
        "btc-5m-20260313T121500Z",
    ]


def test_generate_window_strip_rejects_ambiguous_requests() -> None:
    with pytest.raises(ValueError):
        generate_window_strip(datetime(2026, 3, 13, 12, 7, tzinfo=UTC))

    with pytest.raises(ValueError):
        generate_window_strip(
            datetime(2026, 3, 13, 12, 7, tzinfo=UTC),
            periods=2,
            horizon=timedelta(minutes=10),
        )
