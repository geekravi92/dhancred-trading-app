use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalSide {
    Long,
    Short,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategySignalType {
    EntryLong,
    EntryShort,
    ExitLong,
    ExitShort,
    ExitLongPartial,
    ExitShortPartial,
    Shift,
    Rollover,
}

impl StrategySignalType {
    pub fn side(self) -> Option<SignalSide> {
        match self {
            Self::EntryLong | Self::ExitLong | Self::ExitLongPartial => Some(SignalSide::Long),
            Self::EntryShort | Self::ExitShort | Self::ExitShortPartial => {
                Some(SignalSide::Short)
            }
            Self::Shift | Self::Rollover => None,
        }
    }

    pub fn is_entry(self) -> bool {
        matches!(self, Self::EntryLong | Self::EntryShort)
    }

    pub fn is_exit(self) -> bool {
        self.is_full_exit() || self.is_partial_exit()
    }

    pub fn is_full_exit(self) -> bool {
        matches!(self, Self::ExitLong | Self::ExitShort)
    }

    pub fn is_partial_exit(self) -> bool {
        matches!(self, Self::ExitLongPartial | Self::ExitShortPartial)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeAction {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InstrumentKind {
    Spot,
    Cash,
    Fut,
    Ce,
    Pe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PricePolicyType {
    Market,
    Limit,
    LimitWithSlippage,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PricePolicy {
    pub policy_type: PricePolicyType,
    pub reference_price: Option<f64>,
    pub limit_price: Option<f64>,
    pub max_slippage_bps: Option<f64>,
}

impl PricePolicy {
    pub fn market(reference_price: f64) -> Self {
        Self {
            policy_type: PricePolicyType::Market,
            reference_price: Some(reference_price),
            limit_price: None,
            max_slippage_bps: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategySignal {
    pub signal_id: String,
    pub ssu_id: i64,
    pub strategy_key: String,
    pub campaign_id: String,
    pub trigger_instrument: String,
    pub signal_type: StrategySignalType,
    pub generated_at: u64,
    pub reason: String,
    pub metadata: serde_json::Value,
    pub instructions: Vec<TradeInstruction>,
}

impl StrategySignal {
    pub fn single_leg_entry(
        ssu_id: i64,
        strategy_key: &str,
        trigger_instrument: &str,
        side: SignalSide,
        price: f64,
        reason: String,
        generated_at: u64,
    ) -> Self {
        let signal_type = match side {
            SignalSide::Long => StrategySignalType::EntryLong,
            SignalSide::Short => StrategySignalType::EntryShort,
        };
        let action = match side {
            SignalSide::Long => TradeAction::Buy,
            SignalSide::Short => TradeAction::Sell,
        };
        single_leg_signal(
            ssu_id,
            strategy_key,
            trigger_instrument,
            signal_type,
            action,
            price,
            reason,
            generated_at,
        )
    }

    pub fn single_leg_exit(
        ssu_id: i64,
        strategy_key: &str,
        trigger_instrument: &str,
        side: SignalSide,
        price: f64,
        reason: String,
        generated_at: u64,
    ) -> Self {
        let signal_type = match side {
            SignalSide::Long => StrategySignalType::ExitLong,
            SignalSide::Short => StrategySignalType::ExitShort,
        };
        let action = match side {
            SignalSide::Long => TradeAction::Sell,
            SignalSide::Short => TradeAction::Buy,
        };
        single_leg_signal(
            ssu_id,
            strategy_key,
            trigger_instrument,
            signal_type,
            action,
            price,
            reason,
            generated_at,
        )
    }

    pub fn single_leg_partial_exit(
        ssu_id: i64,
        strategy_key: &str,
        trigger_instrument: &str,
        side: SignalSide,
        price: f64,
        quantity_ratio: f64,
        reason: String,
        generated_at: u64,
    ) -> Self {
        let signal_type = match side {
            SignalSide::Long => StrategySignalType::ExitLongPartial,
            SignalSide::Short => StrategySignalType::ExitShortPartial,
        };
        let action = match side {
            SignalSide::Long => TradeAction::Sell,
            SignalSide::Short => TradeAction::Buy,
        };
        let mut signal = single_leg_signal(
            ssu_id,
            strategy_key,
            trigger_instrument,
            signal_type,
            action,
            price,
            reason,
            generated_at,
        );
        signal.instructions[0].quantity_ratio = quantity_ratio;
        signal
    }

    pub fn side(&self) -> Option<SignalSide> {
        self.signal_type.side()
    }

    pub fn primary_instruction(&self) -> Option<&TradeInstruction> {
        self.instructions.first()
    }
}

fn single_leg_signal(
    ssu_id: i64,
    strategy_key: &str,
    trigger_instrument: &str,
    signal_type: StrategySignalType,
    action: TradeAction,
    price: f64,
    reason: String,
    generated_at: u64,
) -> StrategySignal {
    let type_label = signal_type_label(signal_type);
    let signal_id = format!("SIG-{ssu_id}-{generated_at}-{type_label}");
    let campaign_id = format!("CMP-{ssu_id}-{trigger_instrument}-{generated_at}");
    let leg_id = format!("{campaign_id}-L1");
    StrategySignal {
        signal_id: signal_id.clone(),
        ssu_id,
        strategy_key: strategy_key.to_string(),
        campaign_id,
        trigger_instrument: trigger_instrument.to_string(),
        signal_type,
        generated_at,
        reason,
        metadata: serde_json::json!({}),
        instructions: vec![TradeInstruction {
            instruction_id: format!("{signal_id}-I1"),
            leg_id,
            action,
            instrument_id: trigger_instrument.to_string(),
            instrument_name: trigger_instrument.to_string(),
            instrument_kind: InstrumentKind::Spot,
            leg_role: "MAIN".to_string(),
            quantity_ratio: 1.0,
            price_policy: PricePolicy::market(price),
            metadata: serde_json::json!({}),
        }],
    }
}

pub fn signal_type_label(value: StrategySignalType) -> &'static str {
    match value {
        StrategySignalType::EntryLong => "ENTRY_LONG",
        StrategySignalType::EntryShort => "ENTRY_SHORT",
        StrategySignalType::ExitLong => "EXIT_LONG",
        StrategySignalType::ExitShort => "EXIT_SHORT",
        StrategySignalType::ExitLongPartial => "EXIT_LONG_PARTIAL",
        StrategySignalType::ExitShortPartial => "EXIT_SHORT_PARTIAL",
        StrategySignalType::Shift => "SHIFT",
        StrategySignalType::Rollover => "ROLLOVER",
    }
}

pub fn parse_signal_type(value: &str) -> Option<StrategySignalType> {
    match normalize_label(value).as_str() {
        "ENTRY_LONG" => Some(StrategySignalType::EntryLong),
        "ENTRY_SHORT" => Some(StrategySignalType::EntryShort),
        "EXIT_LONG" => Some(StrategySignalType::ExitLong),
        "EXIT_SHORT" => Some(StrategySignalType::ExitShort),
        "EXIT_LONG_PARTIAL" => Some(StrategySignalType::ExitLongPartial),
        "EXIT_SHORT_PARTIAL" => Some(StrategySignalType::ExitShortPartial),
        "SHIFT" => Some(StrategySignalType::Shift),
        "ROLLOVER" => Some(StrategySignalType::Rollover),
        _ => None,
    }
}

pub fn trade_action_label(value: TradeAction) -> &'static str {
    match value {
        TradeAction::Buy => "BUY",
        TradeAction::Sell => "SELL",
    }
}

pub fn parse_trade_action(value: &str) -> Option<TradeAction> {
    match normalize_label(value).as_str() {
        "BUY" => Some(TradeAction::Buy),
        "SELL" => Some(TradeAction::Sell),
        _ => None,
    }
}

pub fn instrument_kind_label(value: InstrumentKind) -> &'static str {
    match value {
        InstrumentKind::Spot => "SPOT",
        InstrumentKind::Cash => "CASH",
        InstrumentKind::Fut => "FUT",
        InstrumentKind::Ce => "CE",
        InstrumentKind::Pe => "PE",
    }
}

pub fn parse_instrument_kind(value: &str) -> Option<InstrumentKind> {
    match normalize_label(value).as_str() {
        "SPOT" => Some(InstrumentKind::Spot),
        "CASH" => Some(InstrumentKind::Cash),
        "FUT" => Some(InstrumentKind::Fut),
        "CE" => Some(InstrumentKind::Ce),
        "PE" => Some(InstrumentKind::Pe),
        _ => None,
    }
}

pub fn price_policy_type_label(value: PricePolicyType) -> &'static str {
    match value {
        PricePolicyType::Market => "MARKET",
        PricePolicyType::Limit => "LIMIT",
        PricePolicyType::LimitWithSlippage => "LIMIT_WITH_SLIPPAGE",
    }
}

pub fn parse_price_policy_type(value: &str) -> Option<PricePolicyType> {
    match normalize_label(value).as_str() {
        "MARKET" => Some(PricePolicyType::Market),
        "LIMIT" => Some(PricePolicyType::Limit),
        "LIMIT_WITH_SLIPPAGE" => Some(PricePolicyType::LimitWithSlippage),
        _ => None,
    }
}

pub fn parse_price_policy(value: &str) -> Result<PricePolicy, serde_json::Error> {
    serde_json::from_str(value)
}

pub fn serialize_price_policy(value: &PricePolicy) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

fn normalize_label(value: &str) -> String {
    value.trim().replace('-', "_").to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_leg_entry_keeps_long_short_at_signal_level() {
        let signal = StrategySignal::single_leg_entry(
            7,
            "ema_pullback_scalp",
            "BTCUSD",
            SignalSide::Long,
            100_000.0,
            "entry".to_string(),
            1_777_100_000_000,
        );

        assert_eq!(signal.signal_type, StrategySignalType::EntryLong);
        assert_eq!(signal.instructions.len(), 1);
        assert_eq!(signal.instructions[0].action, TradeAction::Buy);
        assert_eq!(signal.instructions[0].instrument_kind, InstrumentKind::Spot);
    }

    #[test]
    fn single_leg_exit_uses_opposite_broker_action() {
        let long_exit = StrategySignal::single_leg_exit(
            7,
            "ema_pullback_scalp",
            "BTCUSD",
            SignalSide::Long,
            101.0,
            "exit long".to_string(),
            1_777_100_001_000,
        );
        let short_exit = StrategySignal::single_leg_exit(
            7,
            "ema_pullback_scalp",
            "BTCUSD",
            SignalSide::Short,
            99.0,
            "exit short".to_string(),
            1_777_100_002_000,
        );

        assert_eq!(long_exit.signal_type, StrategySignalType::ExitLong);
        assert_eq!(long_exit.instructions[0].action, TradeAction::Sell);
        assert_eq!(short_exit.signal_type, StrategySignalType::ExitShort);
        assert_eq!(short_exit.instructions[0].action, TradeAction::Buy);
    }

    #[test]
    fn single_leg_partial_exit_marks_partial_type_and_quantity() {
        let long_partial = StrategySignal::single_leg_partial_exit(
            7,
            "adaptive_supertrend",
            "BTCUSD",
            SignalSide::Long,
            101.0,
            1.0 / 3.0,
            "tp1 partial".to_string(),
            1_777_100_001_000,
        );

        assert_eq!(long_partial.signal_type, StrategySignalType::ExitLongPartial);
        assert_eq!(long_partial.instructions[0].action, TradeAction::Sell);
        assert!((long_partial.instructions[0].quantity_ratio - 1.0 / 3.0).abs() < 0.000001);
        assert!(long_partial.signal_type.is_exit());
        assert!(long_partial.signal_type.is_partial_exit());
        assert!(!long_partial.signal_type.is_full_exit());
    }

    #[test]
    fn signal_envelope_serializes_multiple_trade_instructions() {
        let signal = StrategySignal {
            signal_id: "SIG-IRON-CONDOR".to_string(),
            ssu_id: 21,
            strategy_key: "iron_condor".to_string(),
            campaign_id: "CMP-21-BTCUSD".to_string(),
            trigger_instrument: "BTCUSD".to_string(),
            signal_type: StrategySignalType::EntryShort,
            generated_at: 1_777_100_000_000,
            reason: "open_iron_condor".to_string(),
            metadata: serde_json::json!({"structure": "iron_condor"}),
            instructions: vec![
                option_instruction(
                    "I1",
                    "L1",
                    TradeAction::Sell,
                    InstrumentKind::Ce,
                    "SHORT_CE",
                ),
                option_instruction(
                    "I2",
                    "L2",
                    TradeAction::Buy,
                    InstrumentKind::Ce,
                    "LONG_CE_HEDGE",
                ),
                option_instruction(
                    "I3",
                    "L3",
                    TradeAction::Sell,
                    InstrumentKind::Pe,
                    "SHORT_PE",
                ),
                option_instruction(
                    "I4",
                    "L4",
                    TradeAction::Buy,
                    InstrumentKind::Pe,
                    "LONG_PE_HEDGE",
                ),
            ],
        };

        let encoded = serde_json::to_string(&signal).expect("serialize");
        assert!(encoded.contains("\"signal_type\":\"ENTRY_SHORT\""));
        assert!(encoded.contains("\"action\":\"SELL\""));
        assert!(encoded.contains("\"instrument_kind\":\"CE\""));
        assert!(encoded.contains("\"policy_type\":\"MARKET\""));

        let decoded: StrategySignal = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded.signal_type, StrategySignalType::EntryShort);
        assert_eq!(decoded.instructions.len(), 4);
        assert_eq!(decoded.instructions[0].leg_role, "SHORT_CE");
        assert_eq!(decoded.instructions[3].instrument_kind, InstrumentKind::Pe);
    }

    #[test]
    fn parses_contract_labels() {
        assert_eq!(
            parse_signal_type("ENTRY_LONG"),
            Some(StrategySignalType::EntryLong)
        );
        assert_eq!(
            parse_signal_type("exit-long-partial"),
            Some(StrategySignalType::ExitLongPartial)
        );
        assert_eq!(parse_trade_action("sell"), Some(TradeAction::Sell));
        assert_eq!(parse_instrument_kind("fut"), Some(InstrumentKind::Fut));
        assert_eq!(
            parse_price_policy_type("limit-with-slippage"),
            Some(PricePolicyType::LimitWithSlippage)
        );
        assert_eq!(parse_trade_action("MODIFY_STOP"), None);
    }

    #[test]
    fn serializes_and_parses_price_policy() {
        let policy = PricePolicy {
            policy_type: PricePolicyType::LimitWithSlippage,
            reference_price: Some(100.0),
            limit_price: Some(101.0),
            max_slippage_bps: Some(10.0),
        };

        let encoded = serialize_price_policy(&policy).expect("serialize price policy");
        let decoded = parse_price_policy(&encoded).expect("parse price policy");

        assert_eq!(decoded, policy);
    }

    fn option_instruction(
        instruction_id: &str,
        leg_id: &str,
        action: TradeAction,
        instrument_kind: InstrumentKind,
        leg_role: &str,
    ) -> TradeInstruction {
        TradeInstruction {
            instruction_id: instruction_id.to_string(),
            leg_id: leg_id.to_string(),
            action,
            instrument_id: format!("BTCUSD-{leg_role}"),
            instrument_name: format!("BTCUSD-{leg_role}"),
            instrument_kind,
            leg_role: leg_role.to_string(),
            quantity_ratio: 1.0,
            price_policy: PricePolicy::market(100.0),
            metadata: serde_json::json!({}),
        }
    }
}
