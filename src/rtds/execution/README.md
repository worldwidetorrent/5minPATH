# Execution v0 Shadow Boundary

Execution v0 is a shadow sidecar only.

Runtime ownership is fixed:
- capture remains primary
- shadow is a secondary observer
- shadow does not block capture
- shadow failure must not affect capture
- shadow writes only to its own artifact tree

Adapter split is fixed:
- `live_state` adapters are the only production path
- `replay_tail` adapters are non-production and exist only for smoke tests, replay, and debugging
- the concrete production adapter currently tails session-scoped capture outputs via
  [`capture_output_live_state_adapter.py`](/home/ubuntu/testingproject/src/rtds/execution/capture_output_live_state_adapter.py)

Core execution consumes normalized internal state from:
- [`enums.py`](/home/ubuntu/testingproject/src/rtds/execution/enums.py)
- [`version.py`](/home/ubuntu/testingproject/src/rtds/execution/version.py)
- [`models.py`](/home/ubuntu/testingproject/src/rtds/execution/models.py)
- [`book_pricer.py`](/home/ubuntu/testingproject/src/rtds/execution/book_pricer.py)
- [`tradability.py`](/home/ubuntu/testingproject/src/rtds/execution/tradability.py)

The older [`rtds.schemas.execution`](/home/ubuntu/testingproject/src/rtds/schemas/execution.py) module remains only as a lower-level compatibility layer while the sidecar is being built.

Boundary rule:
- no raw venue payloads in core execution logic
- no SDK-specific client types in core execution logic
- no direct venue dependency in [`policy_adapter.py`](/home/ubuntu/testingproject/src/rtds/execution/policy_adapter.py)
- no direct venue dependency in [`sizing.py`](/home/ubuntu/testingproject/src/rtds/execution/sizing.py)
- no direct venue dependency in [`book_pricer.py`](/home/ubuntu/testingproject/src/rtds/execution/book_pricer.py)
- no direct venue dependency in [`tradability.py`](/home/ubuntu/testingproject/src/rtds/execution/tradability.py)

Tradability kernel is frozen to:
- buy `up` -> `up_ask`
- buy `down` -> `down_ask`
- sell-style evaluation -> corresponding `bid`
- `decision_ts == snapshot_ts`
- `entry_slippage_vs_top_of_book` is a first-class output

Decision kernel is frozen to:
- one executable-state row in -> one deterministic `ShadowDecision` out
- policy selects side from frozen fair value and top-of-book asks
- sizing is fixed-contracts or fixed-notional only
- intended size is capped by displayed top-of-book liquidity
- no queue-aware logic
- no balance or venue-account logic

Evidence storage now includes:
- [`artifacts/shadow/<session_id>/shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow)
- [`artifacts/shadow/<session_id>/shadow_order_states.jsonl`](/home/ubuntu/testingproject/artifacts/shadow)
- [`artifacts/shadow/<session_id>/shadow_summary.json`](/home/ubuntu/testingproject/artifacts/shadow)
- [`artifacts/shadow/<session_id>/shadow_outcomes.jsonl`](/home/ubuntu/testingproject/artifacts/shadow)
- [`artifacts/shadow/<session_id>/shadow_vs_replay.json`](/home/ubuntu/testingproject/artifacts/shadow)

Storage rules:
- append-only JSONL for decisions
- append-only JSONL for order-state transitions
- append-only JSONL for reconciled outcomes
- atomic summary writes
- atomic shadow-vs-replay writes
- schema validation on write through frozen dataclass contracts
- shadow writes only to the shadow tree, never to capture artifacts

Wave-two evidence modules:
- [`ledger.py`](/home/ubuntu/testingproject/src/rtds/execution/ledger.py) tracks decision transitions and reconciled outcomes
- [`summary.py`](/home/ubuntu/testingproject/src/rtds/execution/summary.py) computes structured pass-rate and reject-rate metrics
- [`reconciler.py`](/home/ubuntu/testingproject/src/rtds/execution/reconciler.py) computes `shadow_outcomes` and `shadow_vs_replay`

Current live-state ingestion path:
- tail `sample_diagnostics.jsonl` as the pacing stream
- tail session-scoped normalized `chainlink_ticks`, `exchange_quotes`, `polymarket_quotes`, and `market_metadata_events`
- assemble one in-memory `ExecutableStateView` per sample timestamp

Minimal runtime is frozen to:
- read normalized live state from a `live_state` adapter only
- evaluate frozen policy and tradability
- write append-only shadow decisions
- refresh atomic shadow summary
- log heartbeat without affecting capture
- isolate internal exceptions and continue
- shut down safely and close its own adapter resources

Out of scope in v0:
- live order submission
- authenticated CLOB trading
- maker logic
- queue modeling
- partial fills
- cancel/replace
- on-chain approval or funding flows
