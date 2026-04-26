# EMA Pullback Scalp Strategy Implementation Plan

## 1. Purpose

Implement a new built-in strategy named `ema_pullback_scalp`.

The strategy detects this price-action sequence inside a bounded candle lookback:

```text
base formation -> breakout -> impulse -> pullback to EMA zone -> continuation trigger -> managed scalp exit
```

The strategy must be fully SSU-driven. No strategy threshold, period, window, buffer, exit rule, or enabled side may be hardcoded inside the strategy implementation. If any required value is missing or invalid, the SSU must fail with `StrategyError::Config` or `StrategyError::Parse`.

V1 is for OHLC-only execution. Volume is not available and must not be required.

## 2. Existing Repo Context

Current strategy contracts:

- SSUs are loaded from `strategy_ssu` in `runtime/strategy.sqlite`.
- `SsuConfig.params_json` carries strategy-specific parameters.
- `SsuConfig.required_timeframes` controls which bars the `TimeframeEngine` maintains.
- `StrategyRuntime` calls each active strategy on `PriceUpdated`.
- Strategies live in `src/strategy/strategies/`.
- Built-ins are registered in `src/strategy/strategies/mod.rs`.
- `TimeframeEngine::recent_bars(instrument, timeframe, count)` returns the latest closed bars.
- Current indicator runtime supports EMA, but only exposes latest indicator value, not historical EMA slope. This strategy should compute local EMA/ATR series from recent bars.

Files expected to change:

- `src/strategy/strategies/ema_pullback_scalp.rs`
- `src/strategy/strategies/mod.rs`
- `src/storage/strategy.rs`
- `src/strategy/runtime.rs` or a new small strategy context store module, if persistent trade context is added
- tests under existing strategy test modules or a new dedicated test module

## 3. SSU Contract

Example SSU:

```json
{
  "strategy_key": "ema_pullback_scalp",
  "enabled": 1,
  "trade_gap_secs": 60,
  "max_overlap": 1,
  "max_positions_per_day": 20,
  "required_timeframes_json": ["1m"],
  "indicator_specs_json": [],
  "params_json": {
    "timeframe": "1m",
    "enabled_sides": ["long", "short"],
    "entry_policy": "pyramid",
    "lookback_bars": 200,

    "ema_fast_period": 9,
    "ema_slow_period": 20,
    "atr_period": 14,

    "regime_ema_slope_lookback_bars": 5,
    "regime_min_fast_slope_atr": 0.08,
    "regime_min_ema_separation_atr": 0.05,

    "base_window_bars": 20,
    "base_max_range_atr": 1.2,
    "base_max_close_spread_atr": 0.8,
    "base_max_single_bar_range_atr": 0.7,
    "base_max_directional_efficiency": 0.35,

    "breakout_buffer_atr": 0.05,
    "breakout_min_bar_range_atr": 0.5,
    "breakout_min_close_location": 0.65,

    "impulse_min_height_atr": 1.25,
    "impulse_max_bars": 8,
    "impulse_min_efficiency": 0.55,

    "pullback_min_ratio": 0.2,
    "pullback_max_ratio": 0.45,
    "pullback_min_bars": 1,
    "pullback_max_bars": 8,
    "pullback_max_counter_efficiency": 0.75,
    "ema_zone_buffer_atr": 0.15,
    "max_breakout_level_penetration_atr": 0.15,

    "trigger_break_lookback_bars": 2,
    "trigger_buffer_atr": 0.02,
    "trigger_min_close_location": 0.6,
    "max_entry_extension_atr": 0.35,

    "stop_buffer_atr": 0.1,
    "target_enabled": false,
    "target_r_multiple": 1.5,
    "exit_on_ema_fail_bars": 2,

    "pyramid_min_profit_r_before_add": 0.8,
    "pyramid_stop_adjustment": "better_of_breakeven_or_latest_entry_sl",
    "pyramid_require_fresh_base_after_last_entry": true,
    "pyramid_min_breakout_level_distance_atr": 0.25,
    "pyramid_max_active_legs": 0
  }
}
```

All values above are examples for the SSU row, not defaults in code.

`entry_policy` is required and must be parsed into this enum:

```rust
enum EntryPolicy {
    SinglePosition,
    Independent,
    Pyramid,
}
```

Allowed SSU values:

```json
"single_position"
"independent"
"pyramid"
```

Entry policy behavior:

- `single_position`: while an open position exists for the same SSU, instrument, and side, the strategy only manages exits and does not scan for new entries.
- `independent`: the strategy may generate a new signal for every fresh complete setup, subject to runtime `max_overlap`, `trade_gap_secs`, and duplicate-setup protection.
- `pyramid`: the strategy may add a new signal only when existing campaign legs are already profitable by an SSU-defined margin and the new setup is structurally fresh.

`pyramid_max_active_legs = 0` means uncapped by strategy. This field still must exist so an SSU can choose to cap pyramiding without changing code.

Strategy must not decide position size. Pyramiding controls only whether an additional signal is structurally valid. Order sizing belongs to the later order module.

## 4. Parameter Validation Rules

The strategy must parse `params_json` into a strongly typed settings struct. Do not use `unwrap_or(...)` defaults for strategy behavior.

Required validations:

- `timeframe` must parse to a supported `Timeframe`.
- `timeframe` must be present in `ssu.required_timeframes`.
- `enabled_sides` must contain at least one of `long` or `short`.
- `entry_policy` must be one of `single_position`, `independent`, or `pyramid`.
- `lookback_bars` must be large enough to compute EMA, ATR, base, impulse, and pullback.
- `ema_fast_period > 0`.
- `ema_slow_period > ema_fast_period`.
- `atr_period > 0`.
- `regime_ema_slope_lookback_bars > 0`.
- `base_window_bars > 1`.
- `impulse_max_bars > 0`.
- `pullback_min_bars > 0`.
- `pullback_max_bars >= pullback_min_bars`.
- `pullback_min_ratio >= 0`.
- `pullback_max_ratio > pullback_min_ratio`.
- `target_enabled` must be explicitly present.
- `target_r_multiple > 0` when `target_enabled = true`.
- If `entry_policy = pyramid`, all pyramid parameters listed in the SSU contract must be present.
- `pyramid_min_profit_r_before_add > 0` when `entry_policy = pyramid`.
- `pyramid_stop_adjustment` must parse to the stop-adjustment enum when `entry_policy = pyramid`.
- `pyramid_min_breakout_level_distance_atr >= 0` when `entry_policy = pyramid`.
- `pyramid_max_active_legs >= 0` when `entry_policy = pyramid`; `0` means uncapped.
- All ATR multiplier thresholds must be finite and non-negative.
- All close-location thresholds must be between `0.0` and `1.0`.

Pyramid stop adjustment enum:

```rust
enum PyramidStopAdjustment {
    None,
    Breakeven,
    LatestEntrySl,
    BetterOfBreakevenOrLatestEntrySl,
}
```

Allowed SSU values:

```json
"none"
"breakeven"
"latest_entry_sl"
"better_of_breakeven_or_latest_entry_sl"
```

Minimum bar requirement:

```text
min_required_bars =
  max(ema_slow_period, atr_period)
  + regime_ema_slope_lookback_bars
  + base_window_bars
  + impulse_max_bars
  + pullback_max_bars
  + trigger_break_lookback_bars
  + 5
```

Reject the SSU if `lookback_bars < min_required_bars`.

## 5. Strategy State

Maintain in-memory state per `(ssu_id, trigger_instrument)`.

Recommended structs:

```rust
struct EmaPullbackScalpStrategy {
    settings: Mutex<BTreeMap<i64, EmaPullbackSettings>>,
    states: Mutex<BTreeMap<StateKey, SetupState>>,
}

struct StateKey {
    ssu_id: i64,
    instrument: String,
}

enum SetupPhase {
    Idle,
    BaseFound,
    BreakoutFound,
    ImpulseTracking,
    PullbackTracking,
    ReadyForEntry,
}

struct SetupState {
    last_processed_closed_end: Option<u64>,
    phase: SetupPhase,
    side: Option<SignalSide>,
    base_start_at: Option<u64>,
    base_end_at: Option<u64>,
    base_high: Option<f64>,
    base_low: Option<f64>,
    breakout_bar_end_at: Option<u64>,
    breakout_level: Option<f64>,
    swing_start_price: Option<f64>,
    swing_extreme_price: Option<f64>,
    swing_extreme_bar_end_at: Option<u64>,
    impulse_bars: u32,
    pullback_extreme_price: Option<f64>,
    pullback_extreme_bar_end_at: Option<u64>,
    pullback_bars: u32,
    ema_fail_bars: u32,
    last_entry_bar_end_at: Option<u64>,
    last_setup_id: Option<String>,
    last_breakout_level: Option<f64>,
}
```

The implementation may recompute the setup from `recent_bars` on each closed candle instead of relying heavily on state, but it must still guard against duplicate processing with `last_processed_closed_end`.

Every complete setup must have a deterministic `setup_id` so `independent` and `pyramid` modes cannot re-enter the same structure repeatedly.

Recommended setup id:

```text
setup_id =
  ssu_id
  + "|"
  + instrument
  + "|"
  + side
  + "|"
  + base_start_at
  + "|"
  + base_end_at
  + "|"
  + breakout_bar_end_at
  + "|"
  + swing_extreme_bar_end_at
  + "|"
  + pullback_extreme_bar_end_at
```

`last_setup_id` must be updated only after an entry signal is accepted by `open_position`.

For `pyramid`, the strategy must treat all open positions for `(ssu_id, instrument, side)` as one campaign. Each accepted entry is one campaign leg with its own virtual position and own persisted trade context.

## 6. Indicator Calculations

Compute local indicator series from recent closed bars.

True range:

```text
TR[i] = max(
  high[i] - low[i],
  abs(high[i] - close[i - 1]),
  abs(low[i] - close[i - 1])
)
```

ATR:

```text
ATR[i] = SMA(TR over atr_period)
```

EMA:

```text
alpha = 2 / (period + 1)
seed EMA = SMA(first period closes)
EMA[i] = alpha * close[i] + (1 - alpha) * EMA[i - 1]
```

Close location for long:

```text
close_location = (close - low) / (high - low)
```

Close location for short:

```text
close_location = (high - close) / (high - low)
```

If `high == low`, close location is invalid and the bar should fail the close-location predicate.

Directional efficiency:

```text
efficiency = abs(end_close - start_close) / sum(abs(close[i] - close[i - 1]))
```

If denominator is zero, efficiency is invalid and should fail the predicate.

## 7. Detection Procedure

Only process when the configured timeframe has closed:

```rust
if !tf_update.closed_timeframes.contains(&settings.timeframe) {
    return Ok(Vec::new());
}
```

Read:

```rust
let bars = ctx.timeframes.recent_bars(
    &event.trigger_instrument,
    settings.timeframe,
    settings.lookback_bars,
);
```

Return no signal if there are fewer than `min_required_bars`.

### 7.1 Regime Filter

Long regime is valid when:

```text
ema_fast_last > ema_slow_last
ema_fast_slope_atr >= regime_min_fast_slope_atr
ema_separation_atr >= regime_min_ema_separation_atr
```

Short regime mirrors this:

```text
ema_fast_last < ema_slow_last
ema_fast_slope_atr <= -regime_min_fast_slope_atr
ema_separation_atr >= regime_min_ema_separation_atr
```

Formulas:

```text
ema_fast_slope_atr =
  (ema_fast_last - ema_fast_n_bars_ago) / atr_last

ema_separation_atr =
  abs(ema_fast_last - ema_slow_last) / atr_last
```

Evaluate only sides enabled in SSU.

### 7.2 Base Formation

Use the `base_window_bars` immediately before the breakout candidate.

Long and short share the same base-quality tests:

```text
base_high = max(high over base window)
base_low = min(low over base window)
base_range = base_high - base_low
base_close_high = max(close over base window)
base_close_low = min(close over base window)
base_close_spread = base_close_high - base_close_low
max_single_bar_range = max(high - low over base window)
base_efficiency = abs(last_close - first_close) / sum(abs(close[i] - close[i - 1]))
```

Base is valid when:

```text
base_range / atr <= base_max_range_atr
base_close_spread / atr <= base_max_close_spread_atr
max_single_bar_range / atr <= base_max_single_bar_range_atr
base_efficiency <= base_max_directional_efficiency
```

Use ATR from the last base bar or breakout candidate bar, but choose one consistently. Recommended: use ATR from the breakout candidate bar.

### 7.3 Breakout

The breakout candidate is the first bar after a valid base that closes outside the base boundary.

Long breakout:

```text
close > base_high + breakout_buffer_atr * atr
bar_range / atr >= breakout_min_bar_range_atr
close_location_long >= breakout_min_close_location
```

Short breakout:

```text
close < base_low - breakout_buffer_atr * atr
bar_range / atr >= breakout_min_bar_range_atr
close_location_short >= breakout_min_close_location
```

Wick-only breakouts must not pass.

When breakout is found:

```text
long swing_start_price = breakout_bar.low
long breakout_level = base_high
short swing_start_price = breakout_bar.high
short breakout_level = base_low
```

### 7.4 Impulse Validation

After breakout, inspect up to `impulse_max_bars`.

Long:

```text
swing_extreme_price = max(high from breakout bar through impulse bars)
impulse_height = swing_extreme_price - swing_start_price
```

Short:

```text
swing_extreme_price = min(low from breakout bar through impulse bars)
impulse_height = swing_start_price - swing_extreme_price
```

Impulse is valid when:

```text
impulse_height / atr_at_breakout >= impulse_min_height_atr
impulse_bars <= impulse_max_bars
impulse_efficiency >= impulse_min_efficiency
```

Impulse efficiency uses closes from breakout bar to swing-extreme bar.

### 7.5 Pullback Validation

After the impulse extreme, inspect up to `pullback_max_bars`.

Long:

```text
pullback_extreme = min(low after swing high)
pullback_depth = swing_high - pullback_extreme
pullback_ratio = pullback_depth / impulse_height
```

Short:

```text
pullback_extreme = max(high after swing low)
pullback_depth = pullback_extreme - swing_low
pullback_ratio = pullback_depth / impulse_height
```

Pullback is valid when:

```text
pullback_min_ratio <= pullback_ratio <= pullback_max_ratio
pullback_min_bars <= pullback_bars <= pullback_max_bars
pullback_counter_efficiency <= pullback_max_counter_efficiency
```

EMA-zone condition:

Long pullback should touch or approach the EMA zone:

```text
pullback_low <= max(ema_fast, ema_slow) + ema_zone_buffer_atr * atr
pullback_low >= min(ema_fast, ema_slow) - ema_zone_buffer_atr * atr
```

Short pullback:

```text
pullback_high >= min(ema_fast, ema_slow) - ema_zone_buffer_atr * atr
pullback_high <= max(ema_fast, ema_slow) + ema_zone_buffer_atr * atr
```

Breakout-level respect:

Long:

```text
pullback_low >= breakout_level - max_breakout_level_penetration_atr * atr
```

Short:

```text
pullback_high <= breakout_level + max_breakout_level_penetration_atr * atr
```

### 7.6 Entry Trigger

The strategy must not enter just because price reached EMA. It enters only after continuation restarts.

Long trigger:

```text
trigger_reference = max(high over last trigger_break_lookback_bars pullback bars)
close > trigger_reference + trigger_buffer_atr * atr
close_location_long >= trigger_min_close_location
entry_extension = close - max(ema_fast, ema_slow)
entry_extension / atr <= max_entry_extension_atr
```

Short trigger:

```text
trigger_reference = min(low over last trigger_break_lookback_bars pullback bars)
close < trigger_reference - trigger_buffer_atr * atr
close_location_short >= trigger_min_close_location
entry_extension = min(ema_fast, ema_slow) - close
entry_extension / atr <= max_entry_extension_atr
```

Entry signal:

```rust
EntrySignal {
    ssu_id,
    trigger_instrument: event.trigger_instrument.clone(),
    trade_instrument: event.trigger_instrument.clone(),
    side,
    price: current_ltp_or_closed_close,
    reason,
    at: event.at,
}
```

Reason must include enough debugging context:

```text
ema_pullback_entry|tf=1m|side=long|base_range_atr=...|impulse_atr=...|pullback_ratio=...|closed_bar_end=...
```

Use `ctx.strategy_positions.open_position(&signal, ssu)`. If it returns `StrategyError::Rule(_)`, return no signal.

## 8. Exit Management

V1 exits:

- stop loss
- target, only when `target_enabled = true`
- time stop
- EMA failure

At entry, compute:

Long:

```text
stop_price = pullback_low - stop_buffer_atr * atr
risk = entry_price - stop_price
target_price = entry_price + target_r_multiple * risk if target_enabled = true
```

Short:

```text
stop_price = pullback_high + stop_buffer_atr * atr
risk = stop_price - entry_price
target_price = entry_price - target_r_multiple * risk if target_enabled = true
```

Reject entry if `risk <= 0`.

Open-position exit checks should run before new-entry detection.

Long exit:

```text
if closed_bar.low <= stop_price -> exit stop
else if target_enabled and closed_bar.high >= target_price -> exit target
else if close < ema_slow for exit_on_ema_fail_bars consecutive bars -> exit ema_fail
```

Short exit:

```text
if closed_bar.high >= stop_price -> exit stop
else if target_enabled and closed_bar.low <= target_price -> exit target
else if close > ema_slow for exit_on_ema_fail_bars consecutive bars -> exit ema_fail
```

If stop and target are both touched in the same closed candle, use conservative ordering:

- Long: stop first.
- Short: stop first.

Reason examples:

```text
ema_pullback_exit|reason=stop|tf=1m|closed_bar_end=...
ema_pullback_exit|reason=target|tf=1m|r_multiple=1.5|closed_bar_end=...
ema_pullback_exit|reason=ema_fail|fail_bars=2|closed_bar_end=...
```

Use `ctx.strategy_positions.close_position(&signal)`. If it returns `StrategyError::Rule(_)` or `StrategyError::NotFound(_)`, return no signal.

## 9. Entry Policy and Pyramiding

Entry policy must be evaluated after existing open positions have been checked for exits and before detecting a new entry.

Open-position lookup:

```text
open_legs = open virtual positions where:
  position.ssu_id == ssu.ssu_id
  position.trade_instrument == event.trigger_instrument
  position.side == candidate side
  position.status == Open
```

### 9.1 Single Position

If `entry_policy = single_position`:

```text
if open_legs is not empty:
    manage exits only
    do not detect new entries
```

### 9.2 Independent

If `entry_policy = independent`:

```text
detect fresh setup even when open_legs is not empty
reject if setup_id was already entered
let StrategyPositionBook enforce max_overlap, trade_gap_secs, and max_positions_per_day
```

This mode treats each setup as a separate trade. It does not require the existing open position to be profitable.

### 9.3 Pyramid

If `entry_policy = pyramid`, a new entry is allowed only when all pyramid guards pass.

Pyramid guards:

```text
open_legs is empty -> allow normal first entry
open_legs is not empty -> require add-entry checks
```

Add-entry checks:

```text
1. Every existing open leg must have valid persisted trade context.
2. The latest existing open leg by `entry_at` must have unrealized_r >= pyramid_min_profit_r_before_add.
3. No existing leg may have unrealized_r < 0.
4. Candidate setup_id must not equal any previously entered setup_id.
5. Candidate breakout level must be far enough from last accepted breakout level.
6. Candidate base must be fresh after the latest accepted entry when pyramid_require_fresh_base_after_last_entry = true.
7. Candidate trigger must still pass max_entry_extension_atr.
8. Runtime trade_gap_secs must pass through StrategyPositionBook.
9. If pyramid_max_active_legs > 0, open_legs.len() must be below that cap.
```

Unrealized R:

Long:

```text
initial_risk = entry_price - original_stop_price
unrealized_r = (current_price - entry_price) / initial_risk
```

Short:

```text
initial_risk = original_stop_price - entry_price
unrealized_r = (entry_price - current_price) / initial_risk
```

Reject pyramiding if `initial_risk <= 0`.

Breakout-level freshness:

Long:

```text
abs(candidate_breakout_level - last_breakout_level) / atr >= pyramid_min_breakout_level_distance_atr
candidate_breakout_level > last_breakout_level
```

Short:

```text
abs(candidate_breakout_level - last_breakout_level) / atr >= pyramid_min_breakout_level_distance_atr
candidate_breakout_level < last_breakout_level
```

Fresh base check:

```text
candidate_base_start_at > latest_open_leg.entry_bar_end_at
```

This prevents adding again on the same level or same structure.

### 9.4 Pyramid Stop Adjustment

When a pyramid add-entry is accepted, adjust older open-leg virtual stops according to `pyramid_stop_adjustment`.

This is a strategy-state update only in V1. The strategy must update `strategy_trade_context.metadata.stop_price`; it must not emit an order-facing adjustment signal because the current order module only understands entry/exit execution through broker-side `BUY` / `SELL`.

Long stop adjustment:

```text
none -> keep old stop
breakeven -> max(old_stop, old_entry_price)
latest_entry_sl -> max(old_stop, latest_entry_stop)
better_of_breakeven_or_latest_entry_sl -> max(old_stop, old_entry_price, latest_entry_stop)
```

Short stop adjustment:

```text
none -> keep old stop
breakeven -> min(old_stop, old_entry_price)
latest_entry_sl -> min(old_stop, latest_entry_stop)
better_of_breakeven_or_latest_entry_sl -> min(old_stop, old_entry_price, latest_entry_stop)
```

The adjustment must update persisted trade context for every older open leg before creating/saving the new leg. If the context update fails, do not open or emit the new entry signal.

This strategy still must not decide quantity or position size. The add-entry signal is only a quality signal.

## 10. Persistent Trade Context

The current `virtual_position` table does not store stop/target/setup context. This strategy needs that context to manage exits after restart.

Add one of these approaches.

Preferred approach:

```sql
CREATE TABLE IF NOT EXISTS strategy_trade_context (
    position_id TEXT PRIMARY KEY,
    ssu_id INTEGER NOT NULL,
    strategy_key TEXT NOT NULL,
    trigger_instrument TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_strategy_trade_context_ssu
    ON strategy_trade_context (ssu_id, trigger_instrument);
```

Metadata example:

```json
{
  "strategy_key": "ema_pullback_scalp",
  "side": "long",
  "timeframe": "1m",
  "entry_price": 100.0,
  "original_stop_price": 98.5,
  "stop_price": 98.5,
  "target_enabled": false,
  "target_price": null,
  "entry_bar_end_at": 1710000000000,
  "setup_id": "7|BTCUSD|long|...",
  "pullback_extreme": 98.7,
  "swing_extreme": 101.0,
  "breakout_level": 99.2,
  "last_breakout_level": 99.2,
  "ema_fail_bars": 0
}
```

Expose a small store to strategies through `StrategyContext`, for example:

```rust
pub trait StrategyTradeContextStore: Send + Sync {
    fn save_context(&self, position_id: &str, ssu_id: i64, strategy_key: &str, trigger_instrument: &str, metadata_json: &str, updated_at: u64) -> Result<(), StrategyError>;
    fn load_context(&self, position_id: &str) -> Result<Option<String>, StrategyError>;
    fn load_open_contexts(&self, ssu_id: i64, trigger_instrument: &str) -> Result<Vec<(String, String)>, StrategyError>;
    fn update_context(&self, position_id: &str, metadata_json: &str, updated_at: u64) -> Result<(), StrategyError>;
    fn delete_context(&self, position_id: &str) -> Result<(), StrategyError>;
}
```

If this store is too much for the first patch, do not fake persistence in memory. Instead, explicitly mark the first implementation as backtest/simulation-only. For live use, persistent context is required.

## 11. Processing Order

On each closed candle:

```text
1. Parse or fetch cached SSU settings.
2. Ignore duplicate closed candle using last_processed_closed_end.
3. Fetch recent bars.
4. Compute EMA and ATR series.
5. Manage existing open positions for this SSU/instrument/side.
6. If any exit signal is generated, return exits and do not open a new position on the same candle.
7. Evaluate entry_policy against open positions.
8. If policy blocks new entry, update managed context and return no entry.
9. Detect fresh setup.
10. Reject duplicate setup_id.
11. If entry_policy = pyramid and open positions exist, run pyramid guards.
12. If trigger passes and policy allows, compute stop/optional target.
13. For pyramid add-entry, adjust older virtual stops in persisted strategy context according to SSU policy before opening the new leg.
14. Create entry signal and save trade context.
15. Otherwise update in-memory setup state and return no signal.
```

## 12. Registration

Update `src/strategy/strategies/mod.rs`:

```rust
mod adaptive_supertrend;
mod dhanrekha;
mod ema_pullback_scalp;
mod exponential_edge;
mod noop;

pub(crate) fn strategy_by_key(strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError> {
    match strategy_key.trim().to_ascii_lowercase().as_str() {
        "adaptive_supertrend" => Ok(Arc::new(adaptive_supertrend::AdaptiveSupertrendStrategy::default())),
        "dhanrekha" => Ok(Arc::new(dhanrekha::DhanrekhaStrategy::default())),
        "ema_pullback_scalp" => Ok(Arc::new(ema_pullback_scalp::EmaPullbackScalpStrategy::default())),
        "exponential_edge" => Ok(Arc::new(exponential_edge::ExponentialEdgeStrategy::default())),
        "noop" => Ok(Arc::new(noop::NoopStrategy)),
        value => Err(StrategyError::Unsupported(format!(
            "unsupported strategy_key {value}; available builtins: adaptive_supertrend, dhanrekha, ema_pullback_scalp, exponential_edge, noop"
        ))),
    }
}
```

## 13. Tests

Required tests:

- Settings parser rejects empty `params_json`.
- Settings parser rejects missing fields.
- Settings parser rejects invalid `lookback_bars`.
- Settings parser rejects timeframe not registered in `required_timeframes`.
- Settings parser rejects invalid `entry_policy`.
- Settings parser rejects missing pyramid params when `entry_policy = pyramid`.
- Settings parser rejects invalid `pyramid_stop_adjustment`.
- Local ATR calculation matches hand-calculated true range sample.
- Local EMA calculation matches hand-calculated seeded EMA sample.
- Base detector accepts a tight base.
- Base detector rejects a wide/choppy base.
- Breakout detector rejects wick-only breakout.
- Breakout detector accepts close outside base with configured buffer.
- Impulse detector rejects weak displacement.
- Impulse detector rejects inefficient movement.
- Pullback detector accepts valid retracement ratio.
- Pullback detector rejects too-shallow pullback.
- Pullback detector rejects too-deep pullback.
- Pullback detector rejects pullback that breaks breakout level beyond configured penetration.
- Trigger detector rejects EMA touch without continuation break.
- Trigger detector accepts continuation break.
- End-to-end long entry is emitted on synthetic bars.
- End-to-end short entry is emitted on synthetic bars.
- `single_position` blocks a second entry while an open leg exists.
- `independent` allows a fresh second setup when runtime overlap/gap rules allow it.
- `pyramid` rejects add-entry when latest open leg has not reached `pyramid_min_profit_r_before_add`.
- `pyramid` rejects add-entry when any open leg has negative unrealized R.
- `pyramid` rejects duplicate `setup_id`.
- `pyramid` rejects same-level add-entry using `pyramid_min_breakout_level_distance_atr`.
- `pyramid` accepts fresh add-entry after latest open leg reaches the configured profit margin.
- `pyramid_stop_adjustment = breakeven` updates older persisted virtual stops correctly without emitting an adjustment signal.
- `pyramid_stop_adjustment = latest_entry_sl` updates older persisted virtual stops correctly without emitting an adjustment signal.
- `pyramid_stop_adjustment = better_of_breakeven_or_latest_entry_sl` chooses the more protective stop.
- `pyramid_max_active_legs = 0` allows uncapped legs subject to the other guards.
- `pyramid_max_active_legs > 0` blocks add-entry at the configured cap.
- Stop exit is emitted.
- Target exit is emitted.
- Target is ignored when `target_enabled = false`.
- Time-stop exit is emitted.
- EMA-fail exit is emitted.
- Same candle stop and target uses stop-first conservative handling.
- Runtime still dispatches all active built-in strategies.

## 14. Acceptance Criteria

Implementation is complete when:

- A row with `strategy_key = "ema_pullback_scalp"` loads through existing SSU runtime.
- The strategy produces no signal until the full sequence is present.
- All thresholds are read from SSU `params_json`.
- `entry_policy` is a parsed enum and supports `single_position`, `independent`, and `pyramid`.
- Pyramiding can only add after the latest open leg reaches the SSU-defined profit margin.
- Pyramiding rejects duplicate or same-level structures.
- Pyramiding stop adjustment updates persisted context before creating/emitting the add-entry signal.
- Strategy emits quality signals only and does not decide quantity or position size.
- No strategy-specific magic numbers exist except harmless structural constants, such as string labels.
- Every entry reason includes enough metrics to debug why the setup passed.
- Exits work from persisted context after restart.
- `cargo test` passes.

## 15. Important Design Notes

- EMA is only a location filter. The strategy edge is the sequence: trend regime, base, breakout, impulse, controlled pullback, renewed aggression.
- Pullback must be measured as a percentage of impulse height, not as a percentage of BTC price.
- ATR is used to normalize thresholds across volatility regimes.
- Volume must not be added to V1 because the current data path does not guarantee it.
- BTC trades 24/7, so do not add session-close or overnight logic to this strategy.
- `max_overlap` is a runtime safety limit. It must not be the only pyramiding rule.
- `pyramid_max_active_legs = 0` intentionally means uncapped by strategy, but other pyramid quality guards still apply.
- Position sizing belongs to the future order module, not this strategy.
- Promotion from scalp to intraday or carry strategy should be a separate later strategy/state layer, not part of this V1 scalp implementation.
