# Strategy V1 Contracts

## Summary
- Feeder owns tick ingestion only.
- Strategy runtime owns `PriceStore`, `TimeframeEngine`, SSU loading, and virtual positions.
- The only wake-up event is `PriceUpdated(trigger_instrument)`.
- Historical replay seeds timeframe/indicator state before live execution.
- All active SSUs receive feeder spot triggers in v1.

## Runtime Shape
- `PriceStore` keeps the latest LTP by normalized instrument key.
- `TimeframeEngine` maintains live forming bars plus recent closed bars for SSU-required timeframes.
- SSUs read both raw bars and indicator values from the shared engine.
- `strategy_ssu` and `virtual_position` live in `runtime/strategy.sqlite`.
- Accepted ENTRY/EXIT virtual-position changes are persisted and notified through Telegram.

## Current Built-ins
- Built-in strategy factory currently supports `strategy_key = "noop"`.
- Concrete strategy implementations live under `src/strategy/strategies/`, one strategy per file.
- SSU reload is available through `POST /admin/strategy/reload`.

## SQLite Tables
- `strategy_ssu`
  - `ssu_id`, `strategy_key`, `enabled`
  - `trade_gap_secs`, `max_overlap`, `max_positions_per_day`
  - `required_timeframes_json`
  - `indicator_specs_json`
  - `params_json`
- `virtual_position`
  - `position_id`, `ssu_id`
  - `trigger_instrument`, `trade_instrument`
  - `side`, `status`
  - `entry_price`, `entry_at`
  - `exit_price`, `exit_at`, `exit_reason`, `pnl`

## Current Assumptions
- Feeder spot symbols are normalized to canonical spot instrument names before triggering SSUs.
- Non-spot ticks still update `PriceStore` and `TimeframeEngine`, but only feeder spot ticks fan out to SSUs in v1.
- Higher timeframes are derived inside the strategy module, not pushed from feeder.
