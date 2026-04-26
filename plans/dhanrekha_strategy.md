# Dhanrekha Strategy

`strategy_key = "dhanrekha"`

Purpose:

```text
streaming support/resistance zones -> breach-pending state
  -> sweep reclaim reversal
  -> breakout acceptance + retest continuation
```

The implementation is closed-candle driven. It bootstraps once from `recent_bars`, then updates ATR, pivots, Donchian queues, period levels, bounded zones, and pending setups incrementally.

## SSU Example

```sql
INSERT OR REPLACE INTO strategy_ssu (
  ssu_id,
  strategy_key,
  enabled,
  trade_gap_secs,
  max_overlap,
  max_positions_per_day,
  required_timeframes_json,
  indicator_specs_json,
  params_json
) VALUES (
  101,
  'dhanrekha',
  0,
  300,
  1,
  12,
  '["5m"]',
  '[]',
  '{
    "timeframe": "5m",
    "enabled_modes": ["sweep_reversal", "breakout_retest"],
    "enabled_sides": ["long", "short"],
    "level_sources": ["pivot", "donchian", "prev_day", "prev_week"],

    "period_timezone": "utc",
    "entry_policy": "single_position",

    "atr_period": 14,
    "pivot_left_bars": 3,
    "pivot_right_bars": 3,
    "donchian_lookback_bars": 48,

    "zone_atr_mult": 0.15,
    "max_active_zones": 24,
    "max_zone_age_bars": 600,
    "max_broken_closes": 8,
    "min_zone_score": 2.0,

    "touch_tolerance_atr": 0.10,
    "break_close_beyond_atr": 0.15,
    "pivot_level_score": 2.0,
    "prev_day_level_score": 3.0,
    "prev_week_level_score": 4.0,
    "donchian_level_score": 1.0,
    "touch_score": 0.25,
    "broken_close_penalty": 0.75,

    "sweep_min_penetration_atr": 0.10,
    "sweep_max_depth_atr": 1.50,
    "max_reclaim_bars": 5,

    "breakout_min_close_beyond_atr": 0.30,
    "breakout_accept_closes": 2,
    "max_retest_bars": 10,
    "retest_tolerance_atr": 0.25,
    "retest_max_penetration_atr": 0.40,
    "min_retest_close_location": 0.60,

    "stop_buffer_atr": 0.20,
    "target_enabled": true,
    "target_r_multiple": 2.0,
    "max_hold_bars": 96
  }'
);
```

## Bias Guards

- Pivot levels are created only after `pivot_right_bars` future candles have already closed.
- Previous day/week levels are emitted only after period rollover.
- Donchian levels are read before the current candle is inserted.
- A level must have `created_at < signal_bar.start_at` to be eligible for entry.
- Retest entry cannot happen on the breakout candle.

## Complexity

Per closed candle:

```text
ATR: O(1)
Pivot confirmation: O(pivot_left_bars + pivot_right_bars), bounded by SSU
Donchian: amortized O(1)
Zone/pending updates: O(max_active_zones), bounded by SSU
```
