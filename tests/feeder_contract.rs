use dhancred_trading_app::adapters::delta::DeltaFeederAdapter;
use dhancred_trading_app::adapters::historical::HistoricalReplayFeeder;
use dhancred_trading_app::feeder::{
    Candle, FeedChannel, FeedError, FeedSubscription, Feeder, InstrumentName, Price, PriceEvent,
    PriceTick, Timeframe, UnixMillis,
};

#[test]
fn historical_replay_feeder_filters_by_subscription() {
    let btc = InstrumentName::new("BTCUSD");
    let eth = InstrumentName::new("ETHUSD");
    let mut feeder = HistoricalReplayFeeder::new(vec![
        PriceEvent::Tick(PriceTick::new(
            btc.clone(),
            Price::new(10.0).unwrap(),
            UnixMillis::new(1),
        )),
        PriceEvent::Tick(PriceTick::new(
            eth,
            Price::new(20.0).unwrap(),
            UnixMillis::new(2),
        )),
        PriceEvent::Candle(Candle::new(
            btc.clone(),
            Timeframe::OneMinute,
            UnixMillis::new(3),
            UnixMillis::new(4),
            Price::new(11.0).unwrap(),
            Price::new(12.0).unwrap(),
            Price::new(10.0).unwrap(),
            Price::new(11.5).unwrap(),
            5.0,
        )),
    ]);

    feeder
        .subscribe(FeedSubscription::new(
            vec![btc],
            vec![FeedChannel::PriceCandle(Timeframe::OneMinute)],
        ))
        .unwrap();

    assert!(matches!(
        feeder.next_event().unwrap(),
        Some(PriceEvent::Candle(_))
    ));
    assert_eq!(feeder.next_event().unwrap(), None);
}

#[test]
fn delta_adapter_accepts_our_neutral_channels() {
    let mut feeder = DeltaFeederAdapter::new();
    let result = feeder.subscribe(FeedSubscription::new(
        vec![InstrumentName::new("BTCUSD")],
        vec![FeedChannel::PriceTick],
    ));

    assert_eq!(result, Ok(()));
    assert!(matches!(
        feeder.next_event(),
        Err(FeedError::Disconnected(_))
    ));
}
