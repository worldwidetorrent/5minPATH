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

What it does not yet have is a frozen execution-side boundary. Without that, live shadow work could easily blur:
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
