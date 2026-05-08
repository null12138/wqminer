# WQMiner Worker Prompt Pack (Extracted + Merged)

Use this as operational guidance for generating BRAIN FASTEXPR candidates.

- Objective: prioritize stable PnL, economic sense, and robust out-of-sample behavior.
- Simulation settings discipline: confirm valid region, delay, universe, neutralization combinations before large runs.
- Core settings focus: region / delay / universe / neutralization materially affect outcomes; avoid blind carryover.
- Data hygiene: prefer single dataset coherence inside one expression; avoid random cross-dataset mixing.
- Preprocess chain preference: winsorize -> zscore/normalize -> group_neutralize or regression_neut where appropriate.
- Neutralization guidance: use group_neutralize(x, group) or regression_neut(y, x) to reduce structural exposure.
- Turnover control: use decay/hump/target_tvr operators when turnover is unstable.
- Correlation discipline: monitor self/prod correlation and avoid crowded overlaps.
- Diversity/pyramid mindset: increase coverage across region+delay+category, not only one crowded lane.
- Stage logic: if metrics are not good enough, prioritize structural changes over tiny parameter tuning.
- Negative signal reuse: strongly negative sharpe+fitness candidates can be considered for sign flip variants.
- Output discipline: only valid, balanced FASTEXPR expressions; no prose.
- Batch discipline: keep exact target batch size and reject invalid operators/arguments early.

Template style extraction (from local guides):
- Reuse robust skeletons from `temp.md` and keep expressions concise.
- Prefer operator-compatible templates; skip lines with unsupported operators.
- Ratio and pair ideas from `dt` can be used as candidate field relationships, then add robustness transforms.

