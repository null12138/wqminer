# FASTEXPR Syntax Manual

- Generated at: `2026-03-09 09:09:09`
- Operators covered: `66`
- Template samples: `170`

## Grammar Skeleton
```text
<expr> := <op>(<arg1>, <arg2>, ...) | <expr> <arith> <expr> | (<expr>) | if_else(<cond>, <a>, <b>)
<cond> := greater(x,y) | less(x,y) | equal(x,y) | (<expr> > <expr>) | (<expr> && <expr>)
<grouping> := market | sector | industry | subindustry | bucket(rank(cap), range="0,1,0.1")
```

## Generation Rules
- Output exactly one FASTEXPR expression per line.
- Use only listed operators and listed data fields.
- Expression must be single-line and parenthesis-balanced.
- Do not output placeholders such as {datafield}.
- Do not output assignments, markdown, or explanations.
- Prefer concise compositions: rank/ts_* plus optional group neutralization.

## Checker Rules
- Repair syntax only, do not change signal intent.
- Keep as much original structure as possible.
- Use known operators only.
- Keep expression single-line and balanced.
- If unrecoverable, return INVALID.

## Operator Cheat Sheet
### Arithmetic
- `abs`: `abs(x)`
- `add`: `add(x, y, filter = false), x + y`
- `densify`: `densify(x)`
- `divide`: `divide(x, y), x / y`
- `inverse`: `inverse(x)`
- `log`: `log(x)`
- `max`: `max(x, y, ..)`
- `min`: `min(x, y ..)`
- `multiply`: `multiply(x ,y, ... , filter=false), x * y`
- `power`: `power(x, y)`
- `reverse`: `reverse(x)`
- `sign`: `sign(x)`

### Cross Sectional
- `normalize`: `normalize(x, useStd = false, limit = 0.0)`
- `quantile`: `quantile(x, driver = gaussian, sigma = 1.0)`
- `rank`: `rank(x, rate=2)`
- `scale`: `scale(x, scale=1, longscale=1, shortscale=1)`
- `winsorize`: `winsorize(x, std=4)`
- `zscore`: `zscore(x)`

### Group
- `group_backfill`: `group_backfill(x, group, d, std = 4.0)`
- `group_mean`: `group_mean(x, weight, group)`
- `group_neutralize`: `group_neutralize(x, group)`
- `group_rank`: `group_rank(x, group)`
- `group_scale`: `group_scale(x, group)`
- `group_zscore`: `group_zscore(x, group)`

### Logical
- `and`: `and(input1, input2)`
- `equal`: `input1 == input2`
- `greater`: `input1 > input2`
- `greater_equal`: `input1 >= input2`
- `if_else`: `if_else(input1, input2, input 3)`
- `is_nan`: `is_nan(input)`
- `less`: `input1 < input2`
- `less_equal`: `input1 <= input2`
- `not`: `not(x)`
- `not_equal`: `input1!= input2`
- `or`: `or(input1, input2)`

### Time Series
- `days_from_last_change`: `days_from_last_change(x)`
- `hump`: `hump(x, hump = 0.01)`
- `kth_element`: `kth_element(x, d, k, ignore=“NaN”)`
- `last_diff_value`: `last_diff_value(x, d)`
- `ts_arg_max`: `ts_arg_max(x, d)`
- `ts_arg_min`: `ts_arg_min(x, d)`
- `ts_av_diff`: `ts_av_diff(x, d)`
- `ts_backfill`: `ts_backfill(x,lookback = d, k=1)`
- `ts_corr`: `ts_corr(x, y, d)`
- `ts_count_nans`: `ts_count_nans(x ,d)`
- `ts_covariance`: `ts_covariance(y, x, d)`
- `ts_decay_linear`: `ts_decay_linear(x, d, dense = false)`

### Transformational
- `bucket`: `bucket(rank(x), range="0, 1, 0.1" or buckets = "2,5,6,7,10")`
- `trade_when`: `trade_when(x, y, z)`

### Vector
- `vec_avg`: `vec_avg(x)`
- `vec_sum`: `vec_sum(x)`

## High-Frequency Operators
- `rank`: 271
- `ts_corr`: 72
- `group_neutralize`: 70
- `ts_rank`: 60
- `ts_sum`: 58
- `ts_delta`: 57
- `ts_mean`: 56
- `bucket`: 53
- `ts_delay`: 45
- `ts_decay_linear`: 42
- `add`: 17
- `scale`: 13
- `zscore`: 12
- `divide`: 11
- `ts_std_dev`: 9
- `sign`: 9
- `ts_zscore`: 8
- `abs`: 5
- `ts_arg_min`: 5
- `ts_arg_max`: 5
- `max`: 5
- `min`: 5
- `power`: 5
- `multiply`: 5

## Valid Template Examples
- `(((((ts_delay(close, 20) - ts_delay(close, 10)) / 10) - ((ts_delay(close, 10) - close) / 10)) < (-1 *0.05)) ? 1 : ((-1 * 1) * (close - ts_delay(close, 1))))`
- `(((((ts_delay(close, 20) - ts_delay(close, 10)) / 10) - ((ts_delay(close, 10) - close) / 10)) < (-1 *0.1)) ? 1 : ((-1 * 1) * (close - ts_delay(close, 1))))`
- `((((-1 * ts_min(low, 5)) + ts_delay(ts_min(low, 5), 5)) * rank(((ts_sum(returns, 240) -ts_sum(returns, 20)) / 220))) * ts_rank(volume, 5))`
- `((((rank((1 / close)) * volume) / ts_mean(volume,20)) * ((high * rank((high - close))) / (ts_sum(high, 5) /5))) - rank((vwap - ts_delay(vwap, 5))))`
- `((((ts_delta((ts_sum(close, 100) / 100), 100) / ts_delay(close, 100)) < 0.05) ||((ts_delta((ts_sum(close, 100) / 100), 100) / ts_delay(close, 100)) == 0.05)) ? (-1 * (close - ts_min(close,100))) : (-1 * ts_delta(close, 3)))`
- `(((-1 * rank((open - ts_delay(high, 1)))) * rank((open - ts_delay(close, 1)))) * rank((open -ts_delay(low, 1))))`
- `(((-1 * rank(ts_rank(close, 10))) * rank(ts_delta(ts_delta(close, 1), 1))) *rank(ts_rank((volume / ts_mean(volume,20)), 5)))`
- `(((1.0 - rank(((sign((close - ts_delay(close, 1))) + sign((ts_delay(close, 1) - ts_delay(close, 2)))) +sign((ts_delay(close, 2) - ts_delay(close, 3)))))) * ts_sum(volume, 5)) / ts_sum(volume, 20))`
- `(((ts_sum(high, 20) / 20) < high) ? (-1 * ts_delta(high, 2)) : 0)`
- `((-1 * rank((ts_delta(close, 7) * (1 - rank(ts_decay_linear((volume / ts_mean(volume,20)), 9)))))) * (1 +rank(ts_sum(returns, 250))))`
- `((-1 * rank(Ts_Rank(close, 10))) * rank((close / open)))`
- `((-1 * rank(ts_delta(returns, 3))) * ts_corr(open, volume, 10))`
- `((-1 * rank(ts_std_dev(high, 10))) * ts_corr(high, volume, 10))`
- `((-1 * sign(((close - ts_delay(close, 7)) + ts_delta(close, 7)))) * (1 + rank((1 + ts_sum(returns,250)))))`
- `((0 < ts_min(ts_delta(close, 1), 5)) ? ts_delta(close, 1) : ((ts_max(ts_delta(close, 1), 5) < 0) ? ts_delta(close, 1) : (-1 * ts_delta(close, 1))))`
- `((0.5 < rank((ts_sum(ts_corr(rank(volume), rank(vwap), 6), 2) / 2.0))) ? (-1 * 1) : 1)`
- `((Ts_Rank(ts_corr(close, ts_sum(ts_mean(volume,20), 14), 6), 20) < rank(((open+ close) - (vwap + open)))) * -1)`
- `((Ts_Rank(ts_corr(rank(high), rank(ts_mean(volume,15)), 9), 14) <rank(ts_delta(((close * 0.518371) + (low * (1 - 0.518371))), 1))) * -1)`

## Common Errors
- `placeholder`: Unresolved tokens like {data} or <field>
- `unknown_operator`: Functions outside allowed operator list
- `unbalanced_parentheses`: Missing ')' or extra ')'
- `multi_statement`: Multiple statements, assignments, or comments in one line
- `non_ascii_punctuation`: Chinese punctuation such as ，；（）
