## Checkpoint Cadence

The research contract stays frozen while the workflow gets cheaper.

Default rule:
- use the fast lane after each completed session
- do not run the heavy cumulative checkpoint after every day

Run the heavy checkpoint when one of these is true:
- every `3` clean/valid sessions
- after a major runtime patch
- before a formal reporting milestone
- after a new clean shadow baseline day materially changes the evidence set or before a formal comparison refresh

Heavy checkpoint scope:
- cumulative calibration refresh validation
- refreshed `cross_horizon_summary.json`
- block-level report regeneration
- clean shadow baseline comparison refresh

Current interpretation after the Day 8/Day 9/Day 10 fast-lane workflow:
- multiple clean shadow runtime comparison days now exist
- Day 10 materially expanded the clean-shadow comparison set, but did not by itself justify changing policy
- the default next sessions should use the fast lane first and defer the heavy checkpoint unless a milestone, formal comparison refresh, or runtime-change condition is hit
- the next clean-shadow collection tranche should stop after `3` additional clean shadow days or `2` weeks, whichever comes first

The point of this cadence is simple:
- daily closes stay cheap
- cumulative evidence still gets refreshed on a deliberate schedule
- the repo stops paying the full recomputation tax by default
