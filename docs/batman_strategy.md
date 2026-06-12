# Batman Strategy

`strategy_key = "batman"`

## Purpose

Test whether a positional ratioed long iron fly / Batman structure has edge when the underlying's projected remaining range is safely inside the structure's danger zones.

Core structure:

```text
BUY  ATM CE
BUY  ATM PE
SELL OTM CE in higher ratio
SELL OTM PE in higher ratio
optional: BUY far OTM CE/PE hedges
```

This is not an intraday strategy. The engine scans every minute only to find the best positional entry and to manage open campaigns.

## Design Principles

- Do not hardcode BANKNIFTY. The strategy works on `underlying + its option chain`.
- Do not use DTE as a fixed entry filter. DTE is an input to the remaining-range model.
- Do not enter just because premium is high. Enter only when payoff geometry survives the projected remaining range and stress tests.
- Keep all strategy-specific values inside `params_json.strategy`.
- Every optional model/filter must be flag-based so V1 can stay small.
- Strategy emits one campaign containing all option legs.
- Campaign management is as important as entry selection.

## SSU Shape

Use `single_underlying_with_derivatives`.

```json
{
  "schema_version": "ssu.v1",
  "decision": {
    "scope": "single_underlying_with_derivatives",
    "primary_timeframe": "1m",
    "trigger": {
      "type": "instrument_update",
      "instrument_ref": "primary"
    },
    "universe": [
      {
        "ref": "primary",
        "role": "primary",
        "binding": "runtime_trigger",
        "kind": "SPOT",
        "timeframes": ["1m", "5m", "15m", "1h", "1d"],
        "features": ["candles", "ltp"]
      },
      {
        "ref": "primary_options",
        "role": "derivative_context",
        "kind": "OPTION_CHAIN",
        "underlying_ref": "primary",
        "features": ["ltp", "ohlc", "volume", "oi"]
      }
    ]
  },
  "trade_plan": {
    "mode": "strategy_emits_instructions",
    "instruction_sets": []
  },
  "position": {
    "entry_policy": "spread_campaign",
    "max_open_positions": 1,
    "max_active_legs": 6
  },
  "risk": {
    "enabled": true
  },
  "strategy": {}
}
```

Implementation note: the contract allows derivative decision context, but current `StandardSsu` runtime does not yet resolve dynamic option selectors. Batman should either use `strategy_emits_instructions` support or a completed option selector resolver before live execution.

## Engine Flow

Every minute:

1. Read current underlying price.
2. Load available option chains and expiries for the underlying.
3. Build candidate structures across configured expiry, strike, ratio, and hedge grids.
4. Calculate payoff geometry for each candidate.
5. Estimate the underlying's remaining range till each candidate expiry.
6. Reject candidates that fail range, liquidity, cost, margin, or stress checks.
7. Score surviving candidates.
8. Emit the best campaign only if score is above threshold and position policy allows it.
9. Manage any open campaign for profit capture, danger expansion, breach risk, shift, or exit.

## Candidate Construction

Config-driven grids:

```json
{
  "structure": {
    "long_atm_lots": [1],
    "short_ratios": [3, 5, 7, 10],
    "strike_selection_modes": ["premium_target", "expected_move_buffer", "distance_pct"],
    "hedges": {
      "enabled": false,
      "modes": ["fixed_distance", "premium_budget"]
    }
  }
}
```

Candidate legs:

```text
LONG_CE       = nearest ATM CE
LONG_PE       = nearest ATM PE
SHORT_CE      = selected OTM CE
SHORT_PE      = selected OTM PE
LONG_CE_HEDGE = optional far OTM CE
LONG_PE_HEDGE = optional far OTM PE
```

## Range Engine

The main question:

```text
For the time remaining to expiry, what range can the underlying realistically travel?
```

The range engine must be modular. Each component is enabled by config.

```json
{
  "range_engine": {
    "enabled": true,
    "quantiles": [0.80, 0.90, 0.95],
    "models": {
      "atm_straddle_implied_move": true,
      "black_scholes_iv": true,
      "realized_volatility": true,
      "vix_context": false,
      "trend_regime": true,
      "gap_stress": true
    }
  }
}
```

Model inputs:

- ATM straddle implied move from current option prices.
- IV derived using Black-Scholes from option price, strike, underlying price, time to expiry, risk-free rate, and dividend yield.
- RV from underlying returns over configurable rolling windows.
- Optional VIX context when available.
- Trend/range regime from underlying candles.
- Gap stress from historical gaps for the same underlying.

DTE is not a gate. It only changes remaining time and therefore projected range.

## Rejection Gates

Reject a candidate if any enabled gate fails:

```json
{
  "gates": {
    "payoff_geometry": true,
    "range_breakeven_buffer": true,
    "short_strike_buffer": true,
    "tail_slope_cap": true,
    "liquidity": true,
    "costs_and_slippage": true,
    "margin_estimate": false,
    "stress_test": true
  }
}
```

Required checks:

- Projected range must stay inside breakevens with configured buffer.
- Short strikes must sit outside projected range with configured buffer.
- Tail slope must be within capital risk limits.
- Net premium and payoff must survive costs and slippage.
- Stale or illiquid option legs must be rejected.

## Scoring

Do not optimize only for max visible green area.

Candidate score should combine:

```text
premium richness
+ breakeven buffer
+ short strike buffer
+ probability of profit-capture exit
- tail slope risk
- stress loss
- margin usage
- liquidity penalty
- regime risk penalty
```

The exact weights are config-driven and should be tested through SSUs.

## Entry Signal

Entry signal type: `ENTRY_SHORT`.

The signal must contain all legs in one campaign:

```text
BUY  LONG_CE
BUY  LONG_PE
SELL SHORT_CE
SELL SHORT_PE
BUY  LONG_CE_HEDGE, if enabled
BUY  LONG_PE_HEDGE, if enabled
```

Signal metadata should include:

- selected expiry
- selected strikes
- ratio
- net premium
- breakevens
- projected range
- range model components
- candidate score
- stress loss
- expected profit capture threshold

## Campaign Management

Every minute for open campaigns:

```json
{
  "exit": {
    "profit_capture_pct": [0.25, 0.40, 0.60],
    "max_loss_enabled": true,
    "range_expansion_exit": true,
    "short_strike_risk_exit": true,
    "time_decay_exit": false,
    "shift_enabled": false
  }
}
```

Exit or reduce when:

- configured profit capture is achieved
- range model expands enough that breakeven/short-strike buffer is gone
- stress loss crosses configured limit
- short strike touch/breach probability rises beyond threshold
- liquidity becomes unsafe

Shift/roll is optional. It must be enabled explicitly and must beat a simple exit after costs.

## Backtest Labels

For every scanned candidate, record labels even when no trade is taken:

- final PnL till planned exit or expiry
- max adverse PnL
- max favorable PnL
- short strike touched
- breakeven breached
- stop hit
- profit capture hit
- time spent in drawdown

This lets us learn why losers failed instead of guessing filters upfront.

## V1 Boundary

V1 should be intentionally small:

- underlying + option chain
- every-minute scan
- ATM straddle buy + OTM strangle sell
- ratio grid
- ATM straddle implied move
- realized volatility
- payoff geometry gates
- profit capture exit
- max loss exit

Keep these disabled initially unless needed:

- VIX context
- Black-Scholes IV surface/skew filters
- hedge wings
- margin approximation
- shift/roll
- ML scoring

If the simple V1 cannot survive costs, slippage, and stress tests, adding more filters is likely curve fitting.
