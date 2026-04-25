"""Static evidence data for the read-only 5minPATH showcase dashboard.

The clean-day rates are copied from:
artifacts/diagnostics/clean_shadow_condition_panel/20260421T010000Z/summary.json
"""

from __future__ import annotations

CLEAN_DAYS = [
    {
        "day": "Day 4",
        "survival_pct": 0.75,
        "classification": "Weak",
        "main_drag": "Availability collapse",
        "trusted_venue_rate_pct": 3.20,
        "side_match_rate_pct": 96.6,
    },
    {
        "day": "Day 7",
        "survival_pct": 33.70,
        "classification": "Strong",
        "main_drag": "Best alignment day",
        "trusted_venue_rate_pct": 82.67,
        "side_match_rate_pct": 96.2,
    },
    {
        "day": "Day 8",
        "survival_pct": 0.94,
        "classification": "Weak",
        "main_drag": "Side agreement failure",
        "trusted_venue_rate_pct": 93.74,
        "side_match_rate_pct": 55.2,
    },
    {
        "day": "Day 9",
        "survival_pct": 5.37,
        "classification": "Middle",
        "main_drag": "Availability",
        "trusted_venue_rate_pct": 21.13,
        "side_match_rate_pct": 65.4,
    },
    {
        "day": "Day 10",
        "survival_pct": 7.09,
        "classification": "Middle",
        "main_drag": "Availability + side mismatch",
        "trusted_venue_rate_pct": 76.42,
        "side_match_rate_pct": 53.3,
    },
    {
        "day": "Day 11",
        "survival_pct": 3.78,
        "classification": "Middle",
        "main_drag": "Availability",
        "trusted_venue_rate_pct": 29.62,
        "side_match_rate_pct": 66.5,
    },
]

FAILURE_HIERARCHY = [
    {
        "rank": 1,
        "drag": "Availability",
        "summary": "Enough trusted live state was often unavailable when replay found edge.",
    },
    {
        "rank": 2,
        "drag": "Side mismatch / directional disagreement",
        "summary": (
            "When rows were actionable, live state did not always agree "
            "with replay direction."
        ),
    },
    {
        "rank": 3,
        "drag": "Fill loss",
        "summary": "Top-of-book fill mechanics were not the dominant observed bottleneck.",
    },
]
