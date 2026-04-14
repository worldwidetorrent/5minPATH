# 0008 — Execution v0 Shadow Boundary

## Status

Accepted on `main`.

## Context

Execution v0 is being introduced to answer one narrow question:

> Was the intended price actually there, on the correct side of the book, at the exact decision timestamp, with enough size and fresh enough quotes?

The repo already has:
- durable capture
- window-aware admission semantics `v2`
- a stable policy-v1 universe split
- a frozen stage-1 `good_only` calibrator

At the time of this decision, what the repo did not yet have was a frozen execution-side boundary. Without that, live shadow work could easily blur:
- production vs replay adapters
- capture vs shadow runtime ownership
- normalized internal execution state vs raw venue payloads

## Decision

Freeze execution v0 as a shadow sidecar only.

The fixed boundary is:
- production path uses `live_state` adapters only
- replay/tail adapters are non-production only
- capture remains the primary runtime owner
- shadow remains fail-open relative to capture
- shadow writes only to its own artifact tree
- core execution consumes internal normalized execution state, not SDK objects and not raw venue payloads

The frozen execution-v0 core contract lives in:
- [`src/rtds/execution/enums.py`](/home/ubuntu/testingproject/src/rtds/execution/enums.py)
- [`src/rtds/execution/version.py`](/home/ubuntu/testingproject/src/rtds/execution/version.py)
- [`src/rtds/execution/models.py`](/home/ubuntu/testingproject/src/rtds/execution/models.py)

The older schema layer remains in:
- [`src/rtds/schemas/execution.py`](/home/ubuntu/testingproject/src/rtds/schemas/execution.py)

The frozen adapter split lives in:
- [`src/rtds/execution/adapters.py`](/home/ubuntu/testingproject/src/rtds/execution/adapters.py)
- [`src/rtds/execution/capture_output_live_state_adapter.py`](/home/ubuntu/testingproject/src/rtds/execution/capture_output_live_state_adapter.py) is the current production-safe `live_state` implementation over session-scoped normalized capture outputs
- [`src/rtds/execution/state_assembler.py`](/home/ubuntu/testingproject/src/rtds/execution/state_assembler.py) is the dedicated live execution-state transformation layer from normalized capture rows into `ExecutableStateView`

The frozen v0 live-state input surfaces are:
- required normalized datasets:
  - `data/normalized/chainlink_ticks/date=*/session=*`
  - `data/normalized/exchange_quotes/date=*/session=*`
  - `data/normalized/polymarket_quotes/date=*/session=*`
- optional secondary dataset:
  - `data/normalized/market_metadata_events/date=*/session=*`

Metadata is secondary only. It may fill token IDs or stable market context if the
primary Polymarket quote rows do not already contain those fields, but it must never
act as a second truth source for price, timing, spread, size, or tradability.

Incremental file-tail behavior is also frozen for the capture-output live-state path:
- watch session-scoped JSONL partitions as capture appends them
- track per-file offsets and return appended rows only
- tolerate file creation during runtime
- tolerate cross-midnight `date=*` session partition rollover
- never block capture
- fail open on tail-read failures by logging and continuing

Emission cadence is also frozen:
- default trigger is one newly-complete Polymarket quote sample from capture
- freshest available Chainlink and exchange state are merged into that decision-time row
- duplicate state rows are suppressed by `state_fingerprint` and stable decision identity
- rows that cannot support tradability checks are skipped rather than emitted as partial executable states
- Polymarket rows must not become visible to the live adapter before their `recv_ts`; if a
  Polymarket row has `event_ts <= decision_ts` but `recv_ts > decision_ts`, it must stay buffered
  for a later sample

The capture-output live-state adapter also maintains one latest-known in-memory state
surface rather than rebuilding from scratch on each loop. That cache is frozen to:
- latest Chainlink tick
- latest exchange quote by venue
- latest exchange mid by venue
- latest Polymarket quote by market
- optional latest metadata row by market

The minimal derived state exposed to execution from that cache is:
- current oracle tick
- latest exchange mids by venue
- latest Polymarket executable book for the selected market
- quote age relative to the current decision timestamp

The executable-state assembly contract is also frozen:
- `snapshot_ts` is the sample timestamp from capture
- `window_start_ts`, `window_end_ts`, and `seconds_remaining` are derived with the repo UTC/window helpers
- the assembler must emit one coherent `ExecutableStateView` from current live normalized inputs
- this path must not invent replay snapshots or rebuild replay-style datasets

The frozen venue-neutral core boundaries live in:
- [`src/rtds/execution/policy_adapter.py`](/home/ubuntu/testingproject/src/rtds/execution/policy_adapter.py)
- [`src/rtds/execution/sizing.py`](/home/ubuntu/testingproject/src/rtds/execution/sizing.py)
- [`src/rtds/execution/book_pricer.py`](/home/ubuntu/testingproject/src/rtds/execution/book_pricer.py)
- [`src/rtds/execution/tradability.py`](/home/ubuntu/testingproject/src/rtds/execution/tradability.py)

The frozen tradability mapping is:
- buy `up` -> `up_ask`
- buy `down` -> `down_ask`
- sell-style evaluation -> corresponding `bid`
- `decision_ts == snapshot_ts`
- `entry_slippage_vs_top_of_book` is a first-class kernel output

The frozen shadow artifact tree is:
- `artifacts/shadow/<session_id>/shadow_decisions.jsonl`
- `artifacts/shadow/<session_id>/shadow_order_states.jsonl`
- `artifacts/shadow/<session_id>/shadow_summary.json`
- `artifacts/shadow/<session_id>/shadow_outcomes.jsonl`
- `artifacts/shadow/<session_id>/shadow_vs_replay.json`

The current evidence set is:
- append-only shadow decisions
- append-only shadow order-state transitions
- atomic structured shadow summary
- append-only reconciled shadow outcomes
- atomic shadow-vs-replay comparison summary

The minimal runtime is now also frozen to:
- consume `live_state` adapter output only
- evaluate policy with the frozen decision kernel
- write shadow decisions, order-state transitions, and refresh shadow summary
- log heartbeat
- isolate internal exceptions and continue
- shut down safely without affecting capture

Wave-two evidence logic lives in:
- [`src/rtds/execution/ledger.py`](/home/ubuntu/testingproject/src/rtds/execution/ledger.py)
- [`src/rtds/execution/summary.py`](/home/ubuntu/testingproject/src/rtds/execution/summary.py)
- [`src/rtds/execution/reconciler.py`](/home/ubuntu/testingproject/src/rtds/execution/reconciler.py)

Observed baseline state after adoption:
- Day 4 established the first clean live-forward shadow baseline
- Day 7 established the first strong post-fix clean shadow runtime baseline
- Day 8, Day 9, and Day 10 repeated clean shadow runtime behavior, but with weaker modeled-edge survival than Day 7
- Day 10 completed cleanly through capture, shadow, fast-lane replay/calibration, and edge-survival closeout; it preserved `7.09%` of calibrated modeled edge with availability and side mismatch still dominant
- Day 5 capture remained valid, but its paired shadow run is historically quarantined because
  `future_state_leak_detected` exposed a Polymarket recv-time visibility bug before the patch above
- Day 6 remains a debugging specimen because it combined capture failure with pre-fix shadow semantics
- the main remaining execution-side bottleneck is economic survival under live conditions, especially availability and side agreement, not runtime safety

## Consequences

What execution v0 may do:
- consume live executable state
- evaluate frozen policy
- compute intended taker execution terms
- log decisions and no-trade reasons
- write append-only shadow artifacts

What execution v0 may not do:
- live order submission
- authenticated trading
- maker logic
- queue modeling
- partial fills
- cancel/replace
- on-chain approval/funding flows

If later execution work needs richer behavior, it must extend this boundary deliberately rather than leaking venue-specific types into core logic.
