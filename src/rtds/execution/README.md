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

Out of scope in v0:
- live order submission
- authenticated CLOB trading
- maker logic
- queue modeling
- partial fills
- cancel/replace
- on-chain approval or funding flows
