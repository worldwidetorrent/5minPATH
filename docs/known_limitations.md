# Known Limitations

`5minPATH` is a completed research/data pipeline, not a production trading product.

## Narrow Market Scope

The strongest validated use case is BTC 5-minute Polymarket markets that resolve on Chainlink.

The pipeline is reusable, but market binding, oracle anchoring, fair-value logic, and policy assumptions are not generic. Other market families should be treated as new experiments.

## No Authenticated Trading

The repo does not place real orders, manage capital, or provide a production execution stack.

Shadow measurement is for live-forward tradability analysis only.

## Technical User Required

The tool produces files, reports, and an optional local dashboard.

It assumes comfort with Python, CLI workflows, JSONL artifacts, and report inspection.

## Research Script Surface

The repository contains many experiment-specific scripts because the research trail is preserved.

The primary user-facing path is documented separately in:

- [How to use the repo](how_to_use.md)
- [Command map](command_map.md)
- [Scripts guide](../scripts/README.md)

## Limited Evidence Window

The final clean-shadow comparison set uses six clean shadow days.

The survival statistics are directional evidence, not a stable long-run estimate.

## Strategy Result

The tested strategy found real replay signal but was not deployment-effective enough under live conditions.

The dominant live drags were:

- availability
- directional disagreement / side mismatch
- fill loss, which was comparatively minor

## Future Cleanup

A future cleanup could consolidate experiment scripts into a smaller command surface.

That was intentionally not done before closeout to avoid changing validated workflows or creating new bugs in the research trail.
