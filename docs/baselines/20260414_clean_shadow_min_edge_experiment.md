# Clean Shadow Minimum-Edge Filter Experiment

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_min_edge_experiment/20260414T070000Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_min_edge_experiment/20260414T070000Z/report.md)

Scope:
- Day 4 `20260327T093850581Z`
- Day 7 `20260401T112554963Z`
- Day 8 `20260407T110750965Z`
- Day 9 `20260411T031416598Z`
- Day 10 `20260412T123517467Z`

This is an offline diagnostic only. It did not change live policy, the calibrator, the venue set, the 3-venue rule, capture behavior, or shadow-runtime behavior.

## Question

Can a stricter minimum-edge filter improve weak and middle clean-shadow days without destroying the rare strong-survival Day 7 case?

## Aggregate Result

The aggregate result improved only modestly.

| Experiment | Kept rows | Replay PnL | Shadow PnL | Survival | Side-match rate |
|---|---:|---:|---:|---:|---:|
| Current | `48743` | `8405.230` | `870.43` | `0.1035581417760132679296104925` | `0.662257019438445` |
| Min edge `0.01` | `46301` | `8394.372` | `868.99` | `0.1035205492441840795237571077` | `0.6668869795109055` |
| Min edge `0.025` | `42526` | `8411.107` | `912.20` | `0.1084518363635131499337720944` | `0.6744186046511628` |
| Min edge `0.05` | `37543` | `8195.500` | `896.43` | `0.1093807577329022024281617961` | `0.6750674012425273` |

The `0.025` and `0.05` filters improved weighted aggregate survival slightly, but the gain is not broad enough to justify a policy change.

## Per-Day Result

| Day | Current survival | Min edge `0.025` survival | Min edge `0.05` survival | Read |
|---|---:|---:|---:|---|
| Day 4 | `0.007507565613801537116011238129` | `0.007373368511173623608757185540` | `0.007736297055971105933762136734` | basically unchanged |
| Day 7 | `0.3371580735292590164938162539` | `0.3536854348550719498443273452` | `0.3616728588905865041466288556` | preserved and slightly improved |
| Day 8 | `0.009356814279938101074763486408` | `0.1409109592618582079214441273` | `0.1867876879899300728703234097` | materially improved |
| Day 9 | `0.05367285720355915687494300661` | `0.05268279280620013050496262292` | `0.05041531651716071340462706822` | slightly worse |
| Day 10 | `0.07092105896267184501084398971` | `0.04199807761624414273699387240` | `0.03506815160288324501877029563` | materially worse |

## Interpretation

The stricter minimum-edge filter does not pass as a universal policy refinement.

It passes two useful subchecks:
- it did not kill Day 7
- it materially improved Day 8

It fails the broader robustness check:
- Day 4 was unchanged because availability remained the bottleneck
- Day 9 was slightly worse
- Day 10 was materially worse
- aggregate improvement was small and partly driven by the Day 8 rescue

## Decision

Do not change policy yet.

Keep stricter minimum-edge filtering as a candidate diagnostic, not a live or default replay policy. The next useful version would need a more conditional trigger, because a blunt threshold helps the Day 8 failure mode but harms Day 10.

The current project read remains:
- runtime cleanliness is repeating
- calibration is still economically valuable
- fill loss is not the main bottleneck
- live edge survival is regime-dependent
- the next policy lever should be tested only if it handles Day 8-style side mismatch without damaging Day 10-style middle-survival days
- the next tranche is bounded clean-shadow collection plus conditional diagnosis, recorded in `docs/baselines/20260414_clean_shadow_conditional_diagnosis_tranche.md`
