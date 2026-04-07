## Block Summary: Days 4-7

Primary artifacts:
- `artifacts/diagnostics/block_day4_day7_summary/20260405T130000Z/summary.json`
- `artifacts/diagnostics/block_day4_day7_summary/20260405T130000Z/report.md`

Classification used for this block refresh:
- Capture/replay:
  - Day 4: valid
  - Day 5: valid
  - Day 6: failure specimen only
  - Day 7: valid
- Shadow:
  - Day 4: clean baseline
  - Day 5: quarantined
  - Day 6: quarantined
  - Day 7: clean baseline

Headline counts:
- clean capture days: `3`
- clean shadow baseline days: `2`
- quarantined shadow days: `2`

Calibration read:
- weak raw baseline days in this slice: `2` (`Day 5`, `Day 7`)
- rescued by calibration: `2 / 2`
- rescue rate on valid capture days: `66.7%`

Current cumulative calibration state:
- total sessions: `10`
- total good windows: `293`
- total good snapshots: `81106`
- all five coarse buckets remain `sufficient`

Current bucket gaps / CI widths:
- `far_down`: gap `0.3240`, CI width `0.1480`
- `lean_down`: gap `0.1204`, CI width `0.1422`
- `near_mid`: gap `0.0411`, CI width `0.1565`
- `lean_up`: gap `-0.0371`, CI width `0.1509`
- `far_up`: gap `-0.3054`, CI width `0.1501`

Clean shadow baseline comparison:
- Day 4: `1443 / 50920` actionable (`2.83%`), `1631 / 50920` rows with `3` trusted venues (`3.20%`)
- Day 7: `31356 / 48914` actionable (`64.10%`), `40435 / 48914` rows with `3` trusted venues (`82.67%`)

Read:
- the trustworthy capture block is Days 4, 5, and 7
- the trustworthy shadow baseline set is Day 4 plus Day 7
- the remaining execution-side quality issue is event-time skew characterization, not true recv-time visibility leakage
