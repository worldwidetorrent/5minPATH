# Market Thesis

This document captures the short-form research thesis that originally lived in the README.

## Core question

The repo studies one narrow problem:

> At any moment inside a 5-minute BTC market, what is the best estimate of the oracle-relevant fair value, given only information knowable at that time?

The fair-value target is not generic BTC direction. It is the oracle-settled event:

\[
F_t = \Pr(P^{CL}_{settle} > P^{CL}_{open} \mid \mathcal{I}_t)
\]

Where:

- `P^CL_open` is the Chainlink opening print for the exact 5-minute window
- `P^CL_settle` is the Chainlink price that determines settlement
- `I_t` is the information available at time `t`

That makes the project:

- oracle-anchored
- timestamp-sensitive
- execution-aware

## Market thesis

The thesis is not “predict BTC better than everyone.”

The thesis is:

> price the contract more correctly than the market during lag windows.

The repo treats these markets as a lag-conversion problem:

1. exchange prices move
2. oracle-relevant state updates on its own cadence
3. Polymarket reprices
4. participants react with mixed speed and quality

The opportunity is in the mismatch between those clocks.

## Strategy in plain language

The initial research strategy was:

1. anchor to Chainlink for open and settlement definition
2. use multiple exchange venues as a fast nowcast
3. compute a baseline fair value from displacement, time remaining, and short-horizon volatility
4. trust or degrade that baseline using quality state
5. compare fair value to executable Polymarket prices, not hindsight or pretty mids
6. test whether the apparent edge survives fees, slippage, and model uncertainty

## Why the data system matters

The math is not the hard part. The evidence is.

Without the right data, the repo cannot answer basic questions cleanly:

- what was the correct Chainlink open anchor?
- what did the fastest trustworthy composite say then?
- how much time remained?
- what was the volatility regime?
- what was actually executable?
- was the data fresh enough to trust?

That is why the repo was built as a research and replay engine first, not as a bot.

## Methodology layers

The documented evidence set used four methodological layers:

### 1. Baseline analytic prior

Start with an interpretable prior using:

- displacement from the Chainlink open
- time remaining
- effective short-horizon volatility

### 2. Quality gating

Track whether the state is trustworthy:

- oracle age
- composite age
- venue count
- dispersion
- spread
- missing-anchor state

### 3. Empirical calibration

Correct the baseline against replay evidence rather than trusting raw model output directly.

### 4. Executable edge

Treat the real question as tradeable net edge after:

- taker fees
- slippage
- model buffer
- stale-data risk

## Current evidence conclusion

The current evidence set showed:

- real predictive structure exists under this framing
- calibration improved replay economics repeatedly
- some edge survived live-forward shadow conditions
- the current technique did not monetize that edge consistently enough across days

The dominant live drags were:

1. availability
2. directional disagreement
3. fill loss was minor

So the mature conclusion is:

> the signal is real, but the current harvesting technique is too regime-dependent.
