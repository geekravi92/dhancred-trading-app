# Strategy Signal and Trade Instruction Contract Plan

## 1. Purpose

Refactor strategy output from single-instrument `EntrySignal` / `ExitSignal` objects into an immutable strategy signal envelope containing one or more trade instructions.

This is strategy-independent and applies to all current and future strategies:

- single-leg BTC spot entries
- futures strategies
- option spreads
- iron condor / iron fly / multi-leg hedged structures
- pyramiding add signals
- stop or trail adjustment instructions

Core rule:

```text
Strategy owns logical signal state.
Order module reacts to immutable signals and their instructions.
Order module must not infer strategy intent from hidden state.
```

## 2. Current Code Assessment

Current implementation is not aligned with the required model.

Current signal model in `src/strategy/runtime.rs`:

```rust
pub struct EntrySignal {
    pub ssu_id: i64,
    pub trigger_instrument: String,
    pub trade_instrument: String,
    pub side: SignalSide,
    pub price: f64,
    pub reason: String,
    pub at: u64,
}

pub enum StrategySignal {
    Entry(EntrySignal),
    Exit(ExitSignal),
    Shift(ShiftSignal),
    Rollover(RolloverSignal),
}
```

Limitations:

- A signal can represent only one `trade_instrument`.
- No `signal_id`.
- No `campaign_id`.
- No list of trade instructions.
- No `metadata` field.
- No explicit `generated_at`; existing `at` is strategy/event-specific but not a general signal timestamp.
- `SignalSide` is mixed into individual entry/exit objects instead of signal intent.
- No way to represent a four-leg spread in one atomic signal.
- No way to represent stop/trail modification without abusing entry/exit semantics.
- `virtual_position` table persists only one instrument per virtual position.

## 3. Target Model

Replace the current single-instrument strategy signal shape with this general model:

```rust
pub struct StrategySignal {
    pub signal_id: String,
    pub ssu_id: i64,
    pub strategy_key: String,
    pub campaign_id: String,
    pub signal_type: StrategySignalType,
    pub generated_at: u64,
    pub reason: String,
    pub metadata: serde_json::Value,
    pub instructions: Vec<TradeInstruction>,
}
```

Signal type:

```rust
pub enum StrategySignalType {
    EntryLong,
    EntryShort,
    ExitLong,
    ExitShort,
    Shift,
    Rollover,
}
```

Meaning:

- `EntryLong` / `EntryShort`: open a new logical strategy position or campaign leg.
- `ExitLong` / `ExitShort`: close existing logical position or campaign leg.
- `Shift`: replace one or more legs with another set of legs.
- `Rollover`: move existing exposure to next expiry/contract.

`LONG/SHORT` belongs to `signal_type`, not to `TradeInstruction`.

## 4. Trade Instruction Model

```rust
pub struct TradeInstruction {
    pub instruction_id: String,
    pub leg_id: String,
    pub action: TradeAction,
    pub instrument_id: String,
    pub instrument_name: String,
    pub instrument_kind: InstrumentKind,
    pub leg_role: String,
    pub quantity_ratio: f64,
    pub price_policy: PricePolicy,
    pub metadata: serde_json::Value,
}
```

Trade action:

```rust
pub enum TradeAction {
    Buy,
    Sell,
}
```

Instrument kind:

```rust
pub enum InstrumentKind {
    Spot,
    Cash,
    Fut,
    Ce,
    Pe,
}
```

Important naming rule:

- Do not call `CE`, `PE`, `FUT`, `CASH`, or `SPOT` a `side`.
- Use `instrument_kind`.
- Broker-side order direction is represented only by `TradeAction::Buy` or `TradeAction::Sell`.
- Strategy intent such as entry, exit, stop adjustment, trail adjustment, shift, or rollover is represented by `StrategySignalType` plus signal metadata.
- For exits, `TradeAction` is still only the broker action needed to flatten the leg: `ExitLong` uses `Sell`, and `ExitShort` uses `Buy`.

## 5. Price Policy

`price_policy` tells the order module how the instruction should be executed.

```rust
pub struct PricePolicy {
    pub policy_type: PricePolicyType,
    pub reference_price: Option<f64>,
    pub limit_price: Option<f64>,
    pub max_slippage_bps: Option<f64>,
}
```

```rust
pub enum PricePolicyType {
    Market,
    Limit,
    LimitWithSlippage,
}
```

Examples:

- `Market`: execute immediately at best available price.
- `Limit`: use explicit `limit_price`.
- `LimitWithSlippage`: use `reference_price` and `max_slippage_bps` to compute allowed execution boundary.

Scalping strategies should usually include `reference_price` even for market orders so post-trade slippage can be audited.

## 6. Signal Examples

### 6.1 BTC Spot Long Entry

```json
{
  "signal_id": "SIG-1001",
  "ssu_id": 7,
  "strategy_key": "ema_pullback_scalp",
  "campaign_id": "CMP-7-BTCUSD-20260425-0001",
  "signal_type": "ENTRY_LONG",
  "generated_at": 1777100000000,
  "reason": "ema_pullback_entry",
  "metadata": {
    "timeframe": "1m",
    "setup_id": "7|BTCUSD|long|...",
    "pullback_ratio": 0.34
  },
  "instructions": [
    {
      "instruction_id": "SIG-1001-I1",
      "leg_id": "CMP-7-BTCUSD-20260425-0001-L1",
      "action": "BUY",
      "instrument_id": "BTCUSD",
      "instrument_name": "BTCUSD",
      "instrument_kind": "SPOT",
      "leg_role": "MAIN",
      "quantity_ratio": 1.0,
      "price_policy": {
        "policy_type": "MARKET",
        "reference_price": 100000.0,
        "limit_price": null,
        "max_slippage_bps": 5.0
      },
      "metadata": {
        "stop_price": 99500.0
      }
    }
  ]
}
```

### 6.2 Iron Condor Entry

One strategy signal, four trade instructions:

```text
signal_type = ENTRY_SHORT
instructions:
  SELL CE, leg_role = SHORT_CE
  BUY CE, leg_role = LONG_CE_HEDGE
  SELL PE, leg_role = SHORT_PE
  BUY PE, leg_role = LONG_PE_HEDGE
```

### 6.3 Pyramid Add With Stop Adjustment

In the current architecture, stop movement is strategy-state only. The strategy updates the persisted virtual stop in `strategy_trade_context.metadata.stop_price`. The order module receives no stop-adjustment signal unless a later broker-side protection-order module is explicitly designed.

```text
signal_type = ENTRY_LONG
instructions:
  BUY new long leg L2

strategy internal state:
  old long leg L1 metadata.stop_price = new_stop_price
```

Stop adjustment must not be encoded as another `ENTRY_LONG` for the old leg. In V1 it must also not be emitted as an order-facing signal.

## 7. Immutability and IDs

Signals must be immutable.

Do not modify an old signal to adjust stop or trail. Generate a new signal that references the same `campaign_id` and target `leg_id`.

ID responsibilities:

- `signal_id`: unique per generated signal.
- `instruction_id`: unique per instruction.
- `campaign_id`: groups related legs from the same logical strategy campaign.
- `leg_id`: stable logical leg identity used by strategy, persistence, and order module.

The strategy should generate deterministic or monotonic IDs. The order module must not invent strategy `leg_id`.

## 8. Storage Plan

Current `virtual_position` is not enough for multi-instruction signals.

Add signal tables:

```sql
CREATE TABLE IF NOT EXISTS strategy_signal (
    signal_id TEXT PRIMARY KEY,
    ssu_id INTEGER NOT NULL,
    strategy_key TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    generated_at INTEGER NOT NULL,
    reason TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_signal_instruction (
    instruction_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    leg_id TEXT NOT NULL,
    action TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    instrument_kind TEXT NOT NULL,
    leg_role TEXT NOT NULL,
    quantity_ratio REAL NOT NULL,
    price_policy_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY(signal_id) REFERENCES strategy_signal(signal_id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_campaign
    ON strategy_signal (campaign_id, generated_at);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_instruction_leg
    ON strategy_signal_instruction (leg_id);
```

Storage column can be named `metadata_json` because that is the persisted format. Rust/API structs should use `metadata`.

## 9. Position and Campaign Persistence

Current `virtual_position` can stay temporarily for backward compatibility, but the target model needs campaign/leg persistence.

Recommended tables:

```sql
CREATE TABLE IF NOT EXISTS strategy_campaign (
    campaign_id TEXT PRIMARY KEY,
    ssu_id INTEGER NOT NULL,
    strategy_key TEXT NOT NULL,
    trigger_instrument TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at INTEGER NOT NULL,
    closed_at INTEGER,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_campaign_leg (
    leg_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    ssu_id INTEGER NOT NULL,
    instrument_id TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    instrument_kind TEXT NOT NULL,
    leg_role TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_signal_id TEXT,
    exit_signal_id TEXT,
    entry_price REAL,
    exit_price REAL,
    opened_at INTEGER,
    closed_at INTEGER,
    metadata_json TEXT NOT NULL,
    FOREIGN KEY(campaign_id) REFERENCES strategy_campaign(campaign_id)
);
```

This lets strategy track logical state without assuming one signal equals one instrument.

## 10. Runtime Interface Changes

Replace old enum-based signal return:

```rust
Result<Vec<StrategySignal>, StrategyError>
```

Keep the same return shape, but redefine `StrategySignal` as the envelope struct.

Change `SignalSink` to consume the new envelope:

```rust
pub trait SignalSink: Send + Sync {
    fn consume(&self, signal: &StrategySignal) -> Result<(), StrategyError>;
}
```

The router already works with `StrategySignal`; the main change is type shape and message rendering.

Telegram notification should summarize:

```text
SIGNAL | type=ENTRY_LONG | ssu=7 | strategy=ema_pullback_scalp | campaign=CMP... | instructions=1 | reason=...
```

## 11. Backward Compatibility Strategy

Implement this in phases.

Phase 1:

- Add new structs/enums.
- Add persistence tables.
- Add helper constructors for single-leg entry/exit so existing strategies can migrate easily.
- Keep old `virtual_position` only if needed by current tests.

Phase 2:

- Migrate `candle_cycle` to emit new envelope signals with one instruction.
- Update `StrategyPositionBook` or replace it with campaign/leg store APIs.
- Update tests.

Phase 3:

- Remove old `EntrySignal`, `ExitSignal`, `ShiftSignal`, `RolloverSignal` variants once no strategy uses them.

## 12. Required Helper APIs

Add helper constructors:

```rust
impl StrategySignal {
    pub fn single_leg_entry_long(... ) -> Self;
    pub fn single_leg_entry_short(... ) -> Self;
    pub fn single_leg_exit_long(... ) -> Self;
    pub fn single_leg_exit_short(... ) -> Self;
}
```

Add parser/label helpers:

- `parse_signal_type`
- `signal_type_label`
- `parse_trade_action`
- `trade_action_label`
- `parse_instrument_kind`
- `instrument_kind_label`
- `parse_price_policy`
- `serialize_price_policy`

## 13. Order Module Contract

Order module should react to each entry/exit `StrategySignal` and each `TradeInstruction`.

Order module may be strategy-stateless, but it cannot be execution-stateless. If a future design adds broker-side protection orders, it must maintain execution mapping:

```text
leg_id -> broker_order_id / broker_position_id / current_stop_order_id
```

Strategy owns:

- campaign state
- setup state
- signal generation
- when to add/exit/adjust

Order module owns:

- broker order placement
- broker order modification
- broker order cancellation
- mapping strategy `leg_id` to broker execution IDs
- actual fill status

Because V1 order module does not maintain broker-side stop/trail order mapping, strategy must manage virtual stops and send only entry/exit signals.

## 14. Tests

Required tests:

- Signal envelope serializes/deserializes with multiple instructions.
- Single-leg helper creates one instruction with expected IDs and metadata.
- Iron-condor example creates four instructions under one signal.
- `ENTRY_LONG` signal has `BUY` trade instruction for BTC spot.
- `ENTRY_SHORT` spread signal can contain both `BUY` and `SELL` instructions.
- Stop movement updates strategy trade context and does not emit an order-facing signal.
- Metadata field is named `metadata` in Rust/API structs.
- DB persists signal and instructions atomically.
- DB loads signal with instructions in stable order.
- Telegram sink summarizes new envelope correctly.
- `candle_cycle` migration still emits entry and exit signals.
- Existing runtime dispatch still sends all strategy signals to sinks.

## 15. Acceptance Criteria

Implementation is complete when:

- `StrategySignal` is an immutable envelope with `instructions: Vec<TradeInstruction>`.
- A signal can represent one-leg and multi-leg strategies.
- `LONG/SHORT` lives in `signal_type`.
- Only `BUY/SELL` lives in instruction `action`.
- Entry, exit, shift, and rollover intent lives in `StrategySignalType` plus signal metadata.
- Stop/trail movement is internal strategy state in V1 unless broker-side protection orders are explicitly introduced later.
- `CE/PE/FUT/CASH/SPOT` lives in `instrument_kind`.
- Code/API structs use `metadata`, not `metadata_json`.
- Storage may use `metadata_json`.
- Stop/trail changes are represented by persisted strategy context updates, not by mutating old signals or sending broker-facing adjustment signals.
- `cargo test` passes after migrating existing strategy tests.
