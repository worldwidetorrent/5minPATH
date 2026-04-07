## Checkpoint Cadence

The research contract stays frozen while the workflow gets cheaper.

Default rule:
- use the fast lane after each completed session
- do not run the heavy cumulative checkpoint after every day

Run the heavy checkpoint when one of these is true:
- every `3` clean/valid sessions
- after a major runtime patch
- before a formal reporting milestone
- after a new clean shadow baseline day if the repo does not already have two clean shadow baseline days

Heavy checkpoint scope:
- cumulative calibration refresh validation
- refreshed `cross_horizon_summary.json`
- block-level report regeneration
- clean shadow baseline comparison refresh

Current interpretation after Day 7:
- clean shadow baseline days already exist on Day 4 and Day 7
- so the default next sessions should use the fast lane first and defer the heavy checkpoint unless a milestone or runtime-change condition is hit

The point of this cadence is simple:
- daily closes stay cheap
- cumulative evidence still gets refreshed on a deliberate schedule
- the repo stops paying the full recomputation tax by default
