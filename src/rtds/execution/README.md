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
  and delegates the deterministic live-state transformation into
  [`state_assembler.py`](/home/ubuntu/testingproject/src/rtds/execution/state_assembler.py)

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
- tail session-scoped normalized primary datasets:
  - `chainlink_ticks`
  - `exchange_quotes`
  - `polymarket_quotes`
- use `market_metadata_events` only as a secondary fallback for token IDs or stable market context if those fields are missing from the primary quote rows
- assemble one coherent `ExecutableStateView` per sample timestamp in
  [`state_assembler.py`](/home/ubuntu/testingproject/src/rtds/execution/state_assembler.py)
- incremental file tailing is handled by [`file_tail.py`](/home/ubuntu/testingproject/src/rtds/execution/file_tail.py)
  and is frozen to:
  - discover new JSONL files during runtime
  - track per-file offsets
  - tolerate cross-midnight `date=*` partition rollover for one session
  - skip incomplete trailing lines until they are complete
  - fail open on read/decode errors by logging and continuing

Emission cadence is frozen to:
- one state emission per newly-complete Polymarket quote sample
- freshest available Chainlink and exchange state merged into that decision-time row
- duplicate emissions suppressed by `state_fingerprint` and decision identity
- rows that cannot support tradability checks are skipped rather than emitted as partial executable states
- malformed appended rows remain fail-open and do not affect capture ownership, but they now surface as shadow-side soft-error counts for runtime visibility
- Polymarket rows now become visible to the live adapter only once `recv_ts <= decision_ts`; if
  `recv_ts` is absent, visibility falls back to the older timestamp rule

Current in-memory live-state cache surface:
- latest Chainlink tick
- latest exchange quote by venue
- latest exchange mid by venue
- latest Polymarket quote by market
- optional latest metadata row by market

Minimal derived state for one decision timestamp:
- current oracle tick
- latest exchange mids by venue
- latest Polymarket executable book for the selected market
- quote age relative to the decision timestamp

Live composite diagnostics now distinguish:
- venue present in cache
- venue quote valid for composite input
- venue eligible after freshness and dispersion checks
- per-venue `event_ts`, `recv_ts`, `event_age_ms`, and `recv_age_ms`
- per-venue normalization status and invalid/ineligible reason

This is intentionally stricter than simple presence. A venue can be present in the
live cache but still be excluded from composite construction because of:
- non-`normalized` status
- crossed-market state
- stale event timestamp
- outlier rejection

Current shadow-live composite policy is intentionally shadow-only:
- accept exchange quotes with `normalization_status` of:
  - `normalized`
  - `normalized_with_missing_event_ts`
- use `recv_ts` as the primary freshness clock for live composite participation
- keep `event_ts` as a secondary hard cap
- keep `minimum_venue_count = 3`

This does not change replay semantics, capture semantics, or policy semantics. It
exists only to make the live shadow sidecar diagnostic path honest and usable.

Current evidence status:
- Day 4 is the first clean live-forward shadow baseline session
- Day 5 shadow remains historically quarantined because `future_state_leak_detected` appeared
  before the Polymarket recv-time visibility patch
- current live composite bottleneck remains sparse `3`-trusted-venue formation, driven mainly by
  Binance outlier rejection rather than downstream tradability logic

Input-surface freeze for v0:
- no second truth source beyond those normalized session-scoped capture outputs
- price, spread, size, freshness, and timing come only from the three primary datasets above
- metadata must not override primary quote truth

Minimal runtime is frozen to:
- read normalized live state from a `live_state` adapter only
- evaluate frozen policy and tradability
- write append-only shadow decisions
- refresh atomic shadow summary
- log heartbeat without affecting capture
- isolate internal exceptions and continue
- shut down safely and close its own adapter resources

Current launcher path:
- [`src/rtds/cli/run_shadow_live.py`](/home/ubuntu/testingproject/src/rtds/cli/run_shadow_live.py)
- convenience wrapper: [`scripts/run_shadow_live.sh`](/home/ubuntu/testingproject/scripts/run_shadow_live.sh)

Launcher attach-time rule:
- for real live runs, the launcher owns the attach boundary
- if `--shadow-attach-ts` is omitted, the launcher stamps startup time in UTC
- explicit `--shadow-attach-ts` remains available for controlled replay, smoke, and fixture-driven runs

Out of scope in v0:
- live order submission
- authenticated CLOB trading
- maker logic
- queue modeling
- partial fills
- cancel/replace
- on-chain approval or funding flows
