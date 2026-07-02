use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq)]
pub enum RefreshDecision {
    Initialized {
        anchor_price: f64,
    },
    Hold {
        anchor_price: f64,
        current_price: f64,
        movement_pct: f64,
    },
    Refresh {
        previous_anchor_price: f64,
        new_anchor_price: f64,
        movement_pct: f64,
    },
}

#[derive(Clone, Debug)]
pub struct UniverseRefreshState {
    refresh_trigger_pct: f64,
    anchor_price: Option<f64>,
    active_symbols: BTreeSet<String>,
}

impl UniverseRefreshState {
    pub fn new(refresh_trigger_pct: f64) -> Self {
        Self {
            refresh_trigger_pct,
            anchor_price: None,
            active_symbols: BTreeSet::new(),
        }
    }

    pub fn on_underlying_price(&mut self, current_price: f64) -> RefreshDecision {
        let Some(anchor_price) = self.anchor_price else {
            self.anchor_price = Some(current_price);
            return RefreshDecision::Initialized {
                anchor_price: current_price,
            };
        };

        let movement_pct = movement_pct(anchor_price, current_price);
        if movement_pct >= self.refresh_trigger_pct {
            self.anchor_price = Some(current_price);
            RefreshDecision::Refresh {
                previous_anchor_price: anchor_price,
                new_anchor_price: current_price,
                movement_pct,
            }
        } else {
            RefreshDecision::Hold {
                anchor_price,
                current_price,
                movement_pct,
            }
        }
    }

    pub fn apply_symbols(&mut self, next_symbols: BTreeSet<String>) -> SubscriptionDiff {
        let subscribe = next_symbols
            .difference(&self.active_symbols)
            .cloned()
            .collect();
        let unsubscribe = self
            .active_symbols
            .difference(&next_symbols)
            .cloned()
            .collect();

        self.active_symbols = next_symbols;

        SubscriptionDiff {
            subscribe,
            unsubscribe,
        }
    }

    pub fn reset(&mut self) -> SubscriptionDiff {
        let unsubscribe = self.active_symbols.iter().cloned().collect();
        self.anchor_price = None;
        self.active_symbols.clear();

        SubscriptionDiff {
            subscribe: Vec::new(),
            unsubscribe,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionDiff {
    pub subscribe: Vec<String>,
    pub unsubscribe: Vec<String>,
}

fn movement_pct(anchor_price: f64, current_price: f64) -> f64 {
    ((current_price - anchor_price).abs() / anchor_price) * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refreshes_only_after_trigger_pct() {
        let mut state = UniverseRefreshState::new(4.5);

        assert_eq!(
            state.on_underlying_price(100.0),
            RefreshDecision::Initialized {
                anchor_price: 100.0
            }
        );
        assert!(matches!(
            state.on_underlying_price(104.0),
            RefreshDecision::Hold { .. }
        ));
        assert!(matches!(
            state.on_underlying_price(105.0),
            RefreshDecision::Refresh { .. }
        ));
        assert!(matches!(
            state.on_underlying_price(109.0),
            RefreshDecision::Hold { .. }
        ));
    }

    #[test]
    fn computes_subscription_diff() {
        let mut state = UniverseRefreshState::new(4.5);
        let first = state.apply_symbols(BTreeSet::from([
            "BTCUSD".to_string(),
            "C-BTC-73000-120426".to_string(),
        ]));

        assert_eq!(first.subscribe.len(), 2);
        assert_eq!(first.unsubscribe.len(), 0);

        let second = state.apply_symbols(BTreeSet::from([
            "BTCUSD".to_string(),
            "C-BTC-74000-120426".to_string(),
        ]));

        assert_eq!(second.subscribe, vec!["C-BTC-74000-120426".to_string()]);
        assert_eq!(second.unsubscribe, vec!["C-BTC-73000-120426".to_string()]);
    }

    #[test]
    fn reset_unsubscribes_active_symbols_and_clears_anchor() {
        let mut state = UniverseRefreshState::new(4.5);
        state.on_underlying_price(100.0);
        state.apply_symbols(BTreeSet::from([
            "BTCUSD".to_string(),
            "C-BTC-73000-120426".to_string(),
        ]));

        let diff = state.reset();

        assert_eq!(diff.subscribe.len(), 0);
        assert_eq!(
            diff.unsubscribe,
            vec!["BTCUSD".to_string(), "C-BTC-73000-120426".to_string()]
        );
        assert!(matches!(
            state.on_underlying_price(101.0),
            RefreshDecision::Initialized { .. }
        ));
    }
}
