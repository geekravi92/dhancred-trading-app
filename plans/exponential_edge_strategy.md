# Exponential Edge Strategy Implementation Plan

## 1. Purpose

Implement a new built-in strategy named `exponential_edge`.

The strategy is intentionally simple:

```text
healthy trend -> controlled retracement toward EMA -> entry at candle close -> managed exit
```

This is not a base-breakout-impulse state machine. The core edge is that a directional trend is already present, price has retraced by a measurable fraction of that trend, and price is now near the EMA without fully damaging the trend.

The strategy must be SSU-driven and backtestable. The strategy must not know whether candles are coming from live feed or historical replay.

Important boundary:

```text
strategy = deterministic setup/risk detection from chronological closed bars
runtime/execution = decides whether a bar is executable and what fill is authoritative
```

The strategy may distinguish internal bootstrap/warm-up bars from the current event bar. It must not branch on live vs backtest mode.

## 2. Design Principles

- Use only closed candles for signal decisions.
- Entry is emitted at the same candle close that validates the setup.
- Indicators and rolling windows are rebuilt from historical replay after restart.
- On first use after restart, bootstrap the strategy state from `TimeframeEngine::recent_bars`; do not emit entries for older catch-up bars.
- Position state is handled by the existing position book/trade context flow.
- Mode-specific execution behavior belongs in runtime/backtest execution layers, not inside strategy logic.
- Entry signal metadata must be flat. The existing backtest report already prefixes flattened entry metadata keys with `setup_`.
- No time-stop exit in V1.
- EMA touch uses ATR distance only. Do not add EMA percentage distance in V1.
- Do not use EMA slope at all in V1. Trend health comes from TH/RH, KER, and ADX.
- Avoid a large number of overlapping filters. V1 should prove whether the TH/RH model has edge.
- Keep per-candle processing incremental. Do not rescan large histories on every candle close.

## 3. Core Definitions

For each SSU, instrument, and timeframe, maintain a rolling window of the latest closed bars.

All calculations use closed bars only. The current closed bar is the trigger/entry bar. Swing extrema are selected from bars before the trigger bar so that a real retracement portion exists.

The trend leg must be an ordered pair, not independent global extrema. For each side, choose the best directional move inside the configured window:

```text
long  = max(high[j] - low[i])  where i < j < trigger_bar
short = max(high[i] - low[j])  where i < j < trigger_bar
```

If multiple pairs have the same height, prefer the most recent `j` so the retracement is tied to the latest completed swing.

The trend extreme must also be fresh. A large old move inside the lookback can otherwise make a sideways structure look like a valid pullback. `max_retracement_bars` prevents entries where price has spent too many candles after the trend extreme.

Terms:

```text
EMA = exponential moving average on close
ATR = average true range
KER = Kaufman efficiency ratio
ADX = average directional index
TH  = trend height
RH  = retracement height
RR  = retracement ratio = RH / TH
```

### 3.1 Long Setup

Within the configured trend lookback window before the trigger bar:

```text
trend_start_low = low[i] from the best long ordered pair
trend_high      = high[j] from the best long ordered pair
TH              = trend_high - trend_start_low
RH              = trend_high - current_close
RR              = RH / TH
```

Long setup is valid only if:

```text
trend_high occurs after trend_start_low
min_retracement_bars <= bars after trend_high through trigger bar <= max_retracement_bars
TH > 0
TH / ATR is inside configured trend_height_atr range
RR is inside configured retracement_ratio range
current_close >= EMA
current_close <= EMA + ema_touch_tolerance_atr * ATR
KER is inside configured range
ADX is inside configured range
```

Optional long invalidation:

```text
current_low < trend_start_low
```

If this happens, the trend is considered broken.

### 3.2 Short Setup

Within the configured trend lookback window before the trigger bar:

```text
trend_start_high = high[i] from the best short ordered pair
trend_low        = low[j] from the best short ordered pair
TH               = trend_start_high - trend_low
RH               = current_close - trend_low
RR               = RH / TH
```

Short setup is valid only if:

```text
trend_low occurs after trend_start_high
min_retracement_bars <= bars after trend_low through trigger bar <= max_retracement_bars
TH > 0
TH / ATR is inside configured trend_height_atr range
RR is inside configured retracement_ratio range
current_close <= EMA
current_close >= EMA - ema_touch_tolerance_atr * ATR
KER is inside configured range
ADX is inside configured range
```

Optional short invalidation:

```text
current_high > trend_start_high
```

If this happens, the trend is considered broken.

## 4. Why TH/RH Matters

The strategy should not enter merely because price is near EMA.

The retracement ratio gives a clean mathematical meaning:

```text
RR too low  => pullback is too shallow; entry may chase trend
RR healthy  => controlled pullback; continuation has reasonable structure
RR too high => trend may be damaged
```

Initial working bands for exploration:

```text
trend_height_atr: 1.0 .. wide
retracement_ratio: 0.20 .. 0.60
ema_touch_tolerance_atr: 0.20 .. 1.00
```

These are not final optimized values. They are only wide enough to generate trades and measure profitable ranges.

## 5. Trend Health Filters

V1 uses KER and ADX. EMA slope is intentionally not part of this strategy.

KER can be useful because it measures directional efficiency:

```text
KER = abs(close_now - close_n_bars_ago) / sum(abs(close_i - close_i-1))
```

ADX measures trend strength:

```text
ADX = Wilder-smoothed directional movement strength
```

KER validation:

```text
KER must be inside configured ker range
```

ADX validation:

```text
ADX must be inside configured adx range
```

## 6. SSU Contract

Example `params_json`:

```json
{
  "timeframe": "1h",
  "enabled_sides": ["long", "short"],

  "ema_period": 26,
  "atr_period": 14,
  "trend_lookback_bars": 48,
  "min_retracement_bars": 1,
  "max_retracement_bars": 12,
  "ker_period": 10,
  "adx_period": 14,

  "filters": {
    "trend_height_atr": { "min": 1.0, "max": 1000000.0 },
    "retracement_ratio": { "min": 0.20, "max": 0.60 },
    "ema_touch_tolerance_atr": { "min": 0.0, "max": 0.75 },
    "ker": { "min": 0.20, "max": 1.0 },
    "adx": { "min": 15.0, "max": 100.0 }
  },

  "stop_mode": "pullback_extreme",
  "stop_buffer_atr": 0.20,
  "target_enabled": false,
  "target_r_multiple": 2.0,
  "profit_trail_enabled": false,
  "profit_trail_activation_r": 3.0,
  "profit_trail_giveback_pct": 0.35,
  "profit_trail_min_lock_r": 1.0,
  "exit_on_ema_fail_bars": 2,

  "entry_policy": "single_position"
}
```

Required top-level SSU fields:

```text
strategy_key = exponential_edge
required_timeframes_json includes params_json.timeframe
trade_gap_secs
max_overlap
max_positions_per_day
```

### 6.1 Required Params

- `timeframe`
- `enabled_sides`
- `ema_period`
- `atr_period`
- `trend_lookback_bars`
- `min_retracement_bars`
- `max_retracement_bars`
- `ker_period`
- `adx_period`
- `filters.trend_height_atr`
- `filters.retracement_ratio`
- `filters.ema_touch_tolerance_atr`
- `filters.ker`
- `filters.adx`
- `stop_mode`
- `stop_buffer_atr`
- `target_enabled`
- `target_r_multiple`
- `profit_trail_enabled`
- `profit_trail_activation_r`
- `profit_trail_giveback_pct`
- `profit_trail_min_lock_r`
- `exit_on_ema_fail_bars`
- `entry_policy`

### 6.2 Validation

Reject SSU if:

- `ema_period <= 0`
- `atr_period <= 0`
- `trend_lookback_bars < 3`
- `min_retracement_bars <= 0`
- `max_retracement_bars < min_retracement_bars`
- `min_retracement_bars >= trend_lookback_bars`
- `max_retracement_bars >= trend_lookback_bars`
- `ker_period <= 0`
- `adx_period <= 0`
- range min/max is invalid
- `retracement_ratio.max > 1.0`
- `filters.ker.max > 1.0`
- `filters.adx.max > 100.0`
- `ema_touch_tolerance_atr.max < ema_touch_tolerance_atr.min`
- `stop_buffer_atr < 0`
- `target_enabled = true` and `target_r_multiple <= 0`
- `profit_trail_enabled = true` and `profit_trail_activation_r <= 0`
- `profit_trail_enabled = true` and `profit_trail_giveback_pct <= 0 or >= 1`
- `profit_trail_enabled = true` and `profit_trail_min_lock_r < 0`
- `profit_trail_enabled = true` and `profit_trail_min_lock_r > profit_trail_activation_r`
- `exit_on_ema_fail_bars <= 0`
- `enabled_sides` is empty
- timeframe in params is missing from `required_timeframes_json`

## 7. Runtime State

Add a new strategy file:

```text
src/strategy/strategies/exponential_edge.rs
```

Per `(ssu_id, instrument, timeframe)` keep:

```text
last_processed_closed_end
rolling bars for trend_lookback_bars
EMA state
ATR state
KER rolling close-diff state
ADX Wilder state
fixed-capacity ring segment tree for trend legs
```

Per `(ssu_id, instrument, timeframe, side)` keep:

```text
last_entry_bucket_start
recent_setup_ids bounded by small capacity
```

Use bounded memory. The window capacity should be:

```text
max(
  trend_lookback_bars,
  ema_period,
  atr_period + 1,
  ker_period + 1,
  (2 * adx_period) + 1
) + small_buffer
```

The ADX term is larger because ADX needs one previous bar for movement, `adx_period` bars to seed smoothed TR/+DM/-DM, and another `adx_period` DX values to seed ADX.

The runtime/backtest `recent_bars` setting must be at least the largest `state_capacity` across enabled `exponential_edge` SSUs. If it is smaller, restart catch-up will silently have too little history.

## 8. Incremental Computation

### 8.1 EMA

Seed EMA with SMA of first `ema_period` closes.

After seed:

```text
ema = previous_ema + multiplier * (close - previous_ema)
multiplier = 2 / (ema_period + 1)
```

### 8.2 ATR

Use true range:

```text
TR = max(
  high - low,
  abs(high - previous_close),
  abs(low - previous_close)
)
```

Seed ATR with SMA of first `atr_period` TR values.

After seed:

```text
atr = ((previous_atr * (atr_period - 1)) + tr) / atr_period
```

### 8.3 Segment Tree Trend-Leg Aggregator

Do not scan `trend_lookback_bars` on every candle close.

Use a fixed-capacity ring segment tree. Each leaf is one closed bar. Each internal node stores enough summary to answer the best ordered trend leg for both sides:

```text
logical_index
first_index
last_index
min_low
min_low_at
max_high
max_high_at
best_long_height
best_long_start_at
best_long_extreme_at
best_short_height
best_short_start_at
best_short_extreme_at
```

Each inserted closed bar receives a monotonic `logical_index`.

```text
ring_slot = logical_index % capacity
```

The same tree must support range queries for:

```text
min_low in [trend_extreme_index + 1, latest_index]
max_high in [trend_extreme_index + 1, latest_index]
```

This is needed for pullback stop placement without scanning the whole window.

Do not query the physical tree root as the chronological window after the ring wraps. Physical order and time order diverge after wrap.

Trend query:

```text
active_window = [oldest_logical_index, latest_logical_index]
```

Convert the logical window into one or two physical ring ranges. Query both ranges if wrapped, then merge the returned nodes in chronological order. The same split-and-merge rule applies to pullback range queries.

Empty pre-trigger pullback range is valid when the trend extreme is the latest prior bar. In that case, the current trigger bar is the first retracement bar:

```text
pre_trigger_pullback_range = empty
pullback_extreme = current_trigger_bar.low/high
retracement_bars = 1
```

This is allowed only when `min_retracement_bars <= 1`.

Merge two adjacent nodes `left` and `right`:

```text
best_long = max(
  left.best_long,
  right.best_long,
  right.max_high - left.min_low
)

best_short = max(
  left.best_short,
  right.best_short,
  left.max_high - right.min_low
)
```

Tie-breaks should prefer the most recent trend extreme, then the most recent trend start.

Per closed candle:

```text
1. Query the active chronological trend window before inserting the current trigger bar.
2. Query pullback range after trend extreme in O(log N).
3. Include current trigger bar in pullback extreme calculation without inserting it yet.
4. Validate current trigger bar as retracement/entry.
5. Insert current closed bar into the ring tree.
6. Update tree in O(log N).
```

Trend query, pullback range query, and update are O(log N). This preserves the same math as the ordered-pair definition without full-window rescans.

### 8.4 KER

Maintain a rolling close window and rolling sum of absolute close differences.

```text
ker = abs(close_now - close_n_bars_ago) / sum(abs(close_i - close_i-1))
```

If denominator is zero, `ker = 0`.

### 8.5 ADX

Use Wilder-style incremental ADX.

For each closed bar:

```text
up_move   = current_high - previous_high
down_move = previous_low - current_low

+DM = up_move   if up_move > down_move and up_move > 0 else 0
-DM = down_move if down_move > up_move and down_move > 0 else 0
TR  = true range
```

Smooth `TR`, `+DM`, and `-DM` with Wilder smoothing over `adx_period`.

```text
+DI = 100 * smoothed(+DM) / smoothed(TR)
-DI = 100 * smoothed(-DM) / smoothed(TR)
DX  = 100 * abs(+DI - -DI) / (+DI + -DI)
ADX = Wilder-smoothed DX
```

Readiness:

```text
1. Need previous bar before first TR/+DM/-DM can be computed.
2. Seed smoothed TR/+DM/-DM from the first adx_period TR/+DM/-DM values.
3. Compute DX for the next adx_period bars.
4. Seed ADX as the SMA of those adx_period DX values.
5. ADX becomes ready only after that seed exists.
```

For a period of `N`, ADX is ready after `2N` closed bars plus the previous bar needed for the first movement calculation. Before readiness, no entry may use ADX.

If denominators are zero, use `0`. ADX is used only as strength filter; direction still comes from TH/RH and EMA-side validation.

## 9. Setup Detection

Closed-bar processing order:

```text
1. Continue only if tf_update.closed_timeframes contains params_json.timeframe.
2. Ignore the event if closed_bar.end_at <= last_processed_closed_end.
3. If state is empty, bootstrap from recent_bars first.
4. Warm up indicators and the segment tree using only bars with end_at < closed_bar.end_at.
5. Query the active chronological segment-tree window before inserting the current trigger bar.
6. Update EMA/ATR/KER/ADX with the current trigger bar.
7. Manage open-position exits.
8. If any exit is emitted, insert the current bar into the segment tree, set last_processed_closed_end, and return exits. Do not re-enter on the same candle.
9. If EMA/ATR/KER/ADX/trend window are not ready for entries, insert the current bar into the segment tree, set last_processed_closed_end, and return.
10. Run entry detection.
11. Insert the current bar into the segment tree.
12. Set last_processed_closed_end = closed_bar.end_at.
```

If state is empty for `(ssu_id, instrument, timeframe)`, load enough closed bars through:

```text
ctx.timeframes.recent_bars(instrument, timeframe, state_capacity + 1)
```

The `+ 1` accounts for implementations where `recent_bars` includes the current trigger bar, which must be skipped during warm-up.

Feed returned bars into the rolling state in chronological order, but only if:

```text
bar.end_at < closed_bar.end_at
```

If `recent_bars` includes the current trigger bar, skip that bar during warm-up. The current trigger bar must be evaluated exactly once, then inserted after evaluation.

Warm-up/catch-up bars are non-executable. They must not emit entries or exits.

For recovered open positions, warm-up bars may inspect risk breaches after the last checked bar:

```text
bar.end_at > trade_context.last_exit_check_bar_end_at
```

If a warm-up bar shows that stop, target, or profit trail would have been hit while the strategy was down, do not backdate-close the position and do not fill at the historical exit price. Instead:

```text
recovery_breach_detected = true
recovery_breach_reason = stop | target | profit_trail
recovery_breach_bar_end_at = breached_bar.end_at
```

This detection is feed-agnostic. The strategy records that a recovery breach was observed; it does not decide whether the historical bar was executable in live trading.

Runtime/execution policy:

```text
backtest normal replay bar  -> executable simulated bar
strategy bootstrap bar      -> non-executable detection-only bar
live broker-confirmed fill  -> broker fill is authoritative
live missed historical stop -> do not backdate; close/reconcile from current executable bar
```

On the first current executable closed bar, emit an exit for the recovered position with reason `recovery_breach`. The runtime/execution layer decides the authoritative fill. If no warm-up breach is found, update `last_exit_check_bar_end_at` through the latest inspected warm-up bar.

Entry detection runs only after bootstrap and current-bar indicator update have completed.

For each enabled side:

1. Compute TH/RH/RR.
2. Validate trend height.
3. Validate retracement ratio.
4. Validate retracement freshness using `min_retracement_bars` and `max_retracement_bars`.
5. Validate EMA touch zone.
6. Validate KER.
7. Validate ADX.
8. Build a deterministic `setup_id` from `(ssu_id, instrument, timeframe, side, trend_start_at, trend_extreme_at, entry_bar_end_at)`.
9. Skip if the `setup_id` was already emitted.
10. Enforce strategy-level `entry_policy`.
11. Call the existing position book so `trade_gap_secs`, `max_overlap`, and `max_positions_per_day` are still enforced in live and backtest.
12. Emit entry at closed candle close.

Signal identity must be collision-resistant. Do not rely on the default `StrategySignal::single_leg_*` ID shape alone because it does not include instrument or position identity.

Required:

```text
entry signal_id includes ssu_id, instrument, side, entry_bar_end_at, setup_id
entry leg_id includes setup_id
exit signal_id includes ssu_id, instrument, side, closed_bar_end_at, position_id, exit_reason
exit leg_id = position_id
```

V1 supports only:

```text
entry_policy = single_position
```

For `single_position`, skip entry when an open position already exists for the same SSU and trigger instrument. Reject other `entry_policy` values until explicitly implemented.

No base state, no impulse state, no separate continuation trigger in V1.

## 10. Entry Price

Backtest and live should both treat entry signal price as:

```text
entry_price = closed_bar.close
```

Backtest slippage/brokerage can still adjust fill price at the backtest engine level.

This close-fill model is an explicit approximation. Since live execution can only react after the candle close is known, the configured slippage must be pessimistic enough to cover close-to-execution drift. Do not treat zero-slippage close fills as realistic.

All strategy evaluation must use net PnL after configured:

```text
slippage_pct
brokerage_pct
fixed_fee_per_order
```

Gross PnL is only diagnostic. It must not be used to decide whether the strategy works.

## 11. Stop And Target

### 11.1 Stop Mode: Pullback Extreme

For long:

```text
stop_price = min(low from bars after trend_high through trigger bar) - stop_buffer_atr * ATR
```

For short:

```text
stop_price = max(high from bars after trend_low through trigger bar) + stop_buffer_atr * ATR
```

The retracement portion is:

```text
long  = bars after trend_high through trigger bar
short = bars after trend_low through trigger bar
```

Reject entry if this portion has fewer than `min_retracement_bars` or more than `max_retracement_bars`.

### 11.2 Target

If `target_enabled = true`:

```text
risk = abs(entry_price - stop_price)
target = entry_price + target_r_multiple * risk for long
target = entry_price - target_r_multiple * risk for short
```

If `target_enabled = false`, target is absent.

### 11.3 Profit Trail

Profit trail is separate from the fixed target. It protects open profit without capping upside.

If `profit_trail_enabled = true`, persist the following in trade context:

```text
profit_trail_activated
profit_trail_best_price
profit_trail_stop_price
```

For long:

```text
risk = entry_price - stop_price
peak_profit = best_high_after_entry - entry_price
activate when peak_profit >= profit_trail_activation_r * risk
trail = entry_price + max(
  peak_profit * (1 - profit_trail_giveback_pct),
  profit_trail_min_lock_r * risk
)
```

For short:

```text
risk = stop_price - entry_price
peak_profit = entry_price - best_low_after_entry
activate when peak_profit >= profit_trail_activation_r * risk
trail = entry_price - max(
  peak_profit * (1 - profit_trail_giveback_pct),
  profit_trail_min_lock_r * risk
)
```

Only a trail that was already active before the current closed bar can trigger an exit on that bar. If activation and giveback both happen inside the same OHLC bar, update the trail for the next bar instead of assuming intrabar order.

Reject entry if:

```text
risk <= 0
```

## 12. Exit Rules

Open-position checks run before new entries.

Stop and target are risk exits and do not depend on indicator readiness. They must run for the current executable bar even when EMA/ATR/KER/ADX/trend window are not ready for new entries.

EMA-fail depends on EMA and is checked only when EMA is ready.

Warm-up bars are non-executable strategy bootstrap bars. They may detect a missed risk breach for a recovered open position, but they must not emit a backdated exit signal. A detected warm-up breach forces a `recovery_breach` exit intent on the first current executable closed bar. Runtime/execution decides the authoritative fill.

Exit priority:

```text
recovery_breach -> stop -> target -> profit_trail -> ema_fail
```

`recovery_breach` applies only to recovered positions where warm-up detected a missed risk breach. For normal positions without `recovery_breach_detected`, priority is:

```text
stop -> target -> profit_trail -> ema_fail
```

Same-candle ambiguity rule:

```text
if stop and target are both touched inside the same candle:
    exit stop
```

This is intentionally pessimistic. With candle data we do not know the intrabar path, so assuming target first would overstate results.

Exit reference price:

```text
stop            -> stop_price
target          -> target_price
profit_trail    -> profit_trail_stop_price
ema_fail        -> closed_bar.close
recovery_breach -> current executable close/LTP
```

Backtest slippage is applied after this reference price. Live broker fill is authoritative. Historical recovery breach detection is not itself proof of a live fill unless there was an actual exchange-side protective order or broker execution record.

Long:

```text
if closed_bar.low <= stop_price -> exit stop
else if target enabled and closed_bar.high >= target -> exit target
else if active profit trail and closed_bar.low <= profit_trail_stop_price -> exit profit_trail
else if close < EMA for exit_on_ema_fail_bars consecutive bars -> exit ema_fail
```

Short:

```text
if closed_bar.high >= stop_price -> exit stop
else if target enabled and closed_bar.low <= target -> exit target
else if active profit trail and closed_bar.high >= profit_trail_stop_price -> exit profit_trail
else if close > EMA for exit_on_ema_fail_bars consecutive bars -> exit ema_fail
```

No time stop.

## 13. Trade Context Metadata

There are two different metadata surfaces. Keep them separate.

### 13.1 Entry Signal Metadata

This is what the backtest `orderbook.csv` uses. Keep it flat because the report already prefixes every flattened entry metadata key with `setup_`.

Required:

```json
{
  "setup_id": "...",
  "timeframe": "1h",
  "side": "long",
  "entry_bar_end_at": 1710005400000,
  "entry_price": 100.0,
  "stop_price": 97.5,
  "target_enabled": false,
  "target_price": null,
  "profit_trail_enabled": false,
  "profit_trail_activation_r": 3.0,
  "profit_trail_giveback_pct": 0.35,
  "profit_trail_min_lock_r": 1.0,
  "ema": 99.8,
  "atr": 1.2,
  "trend_start_at": 1710000000000,
  "trend_extreme_at": 1710003600000,
  "trend_height": 8.0,
  "trend_height_atr": 6.67,
  "retracement_height": 3.0,
  "retracement_ratio": 0.375,
  "retracement_bars": 4,
  "ema_touch_distance_atr": 0.12,
  "ker": 0.62,
  "adx": 28.4,
  "plus_di": 31.2,
  "minus_di": 18.7
}
```

Do not put these under a nested `setup` object. Nested setup metadata would become columns like `setup_setup_trend_height`.

### 13.2 Trade Context Metadata

Save only the open-position management state needed by the live runtime. Do not persist indicator windows and do not duplicate all setup metrics here.

Required:

```json
{
  "strategy_key": "exponential_edge",
  "position_id": "...",
  "setup_id": "...",
  "side": "long",
  "entry_bar_end_at": 1710005400000,
  "entry_price": 100.0,
  "stop_price": 97.5,
  "target_enabled": false,
  "target_price": null,
  "profit_trail_enabled": false,
  "profit_trail_activation_r": 3.0,
  "profit_trail_giveback_pct": 0.35,
  "profit_trail_min_lock_r": 1.0,
  "profit_trail_activated": false,
  "profit_trail_best_price": 100.0,
  "profit_trail_stop_price": null,
  "ema_fail_bars": 0,
  "last_exit_check_bar_end_at": 1710005400000,
  "recovery_breach_detected": false,
  "recovery_breach_reason": null,
  "recovery_breach_bar_end_at": null
}
```

If an open position exists but trade context is missing or malformed, the strategy must not silently ignore that position. It should:

```text
1. Return/log a recovery error with position_id, ssu_id, and instrument.
2. Skip all new entries for the same SSU and trigger instrument.
3. Leave the position open for manual or explicit recovery.
```

If we later make live recovery fully replay-derived from persisted signals, `stop_price` and `target_price` can be removed from trade context. For V1, keeping them matches the existing strategy flow and avoids recomputing open-position exits from historical setup reconstruction.

## 14. Backtest Reporting

The existing backtest report already includes flattened SSU params and flattened entry metadata.

No report change is needed if `exponential_edge` emits the flat entry metadata from section 13.1.

Add setup metrics:

```text
setup_timeframe
setup_side
setup_ema
setup_atr
setup_trend_start_at
setup_trend_extreme_at
setup_trend_height
setup_trend_height_atr
setup_retracement_height
setup_retracement_ratio
setup_retracement_bars
setup_ema_touch_distance_atr
setup_ker
setup_adx
setup_plus_di
setup_minus_di
setup_entry_price
setup_stop_price
setup_target_price
```

These fields are necessary to filter profitable ranges after wide-open runs.

Backtest conclusions must be based on summary net metrics, not gross metrics:

```text
net_pnl
profit_factor after costs
expectancy after costs
max_drawdown after costs
win_rate after costs
```

Each report should preserve the cost assumptions used for that run.

## 15. Files To Change

Expected changes:

```text
src/strategy/strategies/exponential_edge.rs
src/strategy/strategies/mod.rs
runtime/strategy.sqlite SSUs
```

Only touch `src/backtest/report.rs` if the existing flattening does not produce the required columns.

Optional if shared helpers are useful:

```text
src/strategy/strategies/common/ranges.rs
src/strategy/strategies/common/rolling.rs
```

Avoid adding common abstractions until duplication actually appears.

## 16. Initial SSUs

For BTC backtest exploration:

```text
timeframes: 5m, 15m, 1h, 1d
ema_period: 26
atr_period: 14
trend_lookback_bars: start with 48 for intraday, smaller for 1d if needed
min_retracement_bars: 1
max_retracement_bars: 12
retracement_ratio: 0.20 .. 0.70 wide run
trend_height_atr: 1.0 .. wide
ema_touch_tolerance_atr: 0.0 .. 1.0
ker_period: 10
ker: 0.0 .. 1.0 wide run
adx_period: 14
adx: 0.0 .. 100.0 wide run
target_enabled: false for first run
profit_trail_enabled: false for baseline; then test activation_r 3/5, giveback_pct 0.30/0.40/0.50, min_lock_r 1/2
exit_on_ema_fail_bars: 2
```

After wide run, tighten based on actual profitable distributions.

## 17. Tests

Unit tests:

- Parses valid SSU.
- Rejects invalid ranges.
- EMA seeds and updates correctly.
- ATR seeds and updates correctly.
- KER computes expected directional efficiency.
- ADX seeds and updates correctly.
- Segment tree returns the expected trend leg and pullback range for hand-built long/short fixtures.
- Segment tree still returns the correct chronological trend leg after ring wrap.
- Empty pre-trigger pullback range is valid when current trigger bar is the first retracement bar and `min_retracement_bars <= 1`.
- Long TH/RH/RR calculation is correct.
- Short TH/RH/RR calculation is correct.
- Entry is blocked when there are no retracement bars after the swing extreme.
- Entry is blocked when retracement bars exceed `max_retracement_bars`.
- Long entry fires only inside EMA touch zone.
- Short entry fires only inside EMA touch zone.
- Entry is blocked when TH is too small.
- Entry is blocked when RR is too shallow.
- Entry is blocked when RR is too deep.
- Entry is blocked when KER fails.
- Entry is blocked when ADX fails.
- Stop exit works.
- Target exit works when enabled.
- Stop/target/EMA-fail/recovery-breach use the correct exit reference price.
- Stop/target exits run even when entry indicators are not ready.
- If an exit is emitted, same-candle re-entry is blocked.
- Warm-up bars after `last_exit_check_bar_end_at` can detect recovery breach but cannot emit backdated exits.
- Recovery breach creates an exit intent on the first current executable bar; runtime/execution owns the authoritative fill.
- Recovery breach has priority over current-bar stop/target/EMA-fail for recovered positions.
- Strategy does not branch on live vs backtest mode.
- Missing or malformed trade context blocks new entries for the same SSU/instrument and surfaces a recovery error.
- Entry and exit signal IDs are unique across instruments, sides, positions, and same-timestamp events.
- Stop wins when stop and target are both touched in the same candle.
- EMA fail exit works.
- No time-stop behavior exists.
- First-use catch-up requests `state_capacity + 1` recent bars and warms state without emitting old entries.
- Bootstrap happens before readiness checks on empty state.
- Current trigger bar from `recent_bars` is skipped during warm-up and evaluated exactly once.
- Duplicate `setup_id` is not emitted twice.

Backtest smoke:

- Run a 2-day BTC window.
- Confirm report generation.
- Confirm orderbook has flat setup metric columns such as `setup_trend_height_atr`, not `setup_setup_trend_height_atr`.
- Confirm summary includes slippage/brokerage/fixed fee assumptions.
- Confirm net PnL differs from gross PnL when costs are non-zero.
- Confirm strategy can run on 5m/15m/1h from 1m source candles.

## 18. Implementation Order

1. Add `exponential_edge.rs` with SSU parser and range types.
2. Implement rolling EMA, ATR, KER, and ADX.
3. Implement fixed-capacity ring segment tree for trend legs.
4. Implement TH/RH/RR calculation for long and short from aggregator output.
5. Implement entry signal creation with setup metadata.
6. Implement position management exits.
7. Register strategy in `strategies/mod.rs`.
8. Verify report columns from flat entry metadata.
9. Add focused unit tests.
10. Add SSUs in `runtime/strategy.sqlite`.
11. Run smoke backtest.
12. Run full test suite.

## 19. Non-Goals For V1

- No base detection.
- No impulse state.
- No continuation trigger.
- No pyramiding.
- No time stop.
- No ML/optimizer.
- No EMA slope filter.

## 20. First Backtest Question

The first backtest should answer only this:

```text
Does trend_height_atr + retracement_ratio + ATR-based EMA touch + KER + ADX produce a positive subset on any timeframe?
```

If not, the idea is weak and should not be patched with more filters.
