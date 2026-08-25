//! Store-backed signal runs: collect completed candle slices per coin and evaluate.
//!
//! The hot cache includes the still-forming trailing bar. Runs share its `Arc`
//! buffers and expose only the completed prefix, avoiding both repainting and a
//! clone of every `Candle`.

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use hl_feed::MarketStore;

use crate::definition::SignalDefinition;
use crate::eval::{CoinData, EvalReport, evaluate};

/// Length of the completed prefix in an oldest-first candle series.
fn completed_len(candles: &[Candle], timeframe: Timeframe, now: DateTime<Utc>) -> usize {
    let now_ms = now.timestamp_millis();
    candles
        .partition_point(|candle| candle.open_time.timestamp_millis().saturating_add(timeframe.duration_ms()) <= now_ms)
}

/// Distinct timeframes referenced by a definition, ascending by duration.
#[must_use]
pub fn referenced_timeframes(definition: &SignalDefinition) -> Vec<Timeframe> {
    let mut referenced = Vec::with_capacity(Timeframe::ALL.len());
    for timeframe in Timeframe::ALL {
        if definition.conditions.values().any(|condition| condition.timeframe == timeframe) {
            referenced.push(timeframe);
        }
    }
    referenced
}

/// Collect completed candle prefixes for every timeframe a signal references.
/// Missing cache entries remain absent and surface as
/// [`crate::eval::OutcomeStatus::NoData`].
#[must_use]
pub fn collect_coin_data(
    store: &MarketStore,
    coin: &str,
    definition: &SignalDefinition,
    now: DateTime<Utc>,
) -> CoinData {
    let mut data = CoinData::default();
    for timeframe in referenced_timeframes(definition) {
        if let Some(candles) = store.candles(coin, timeframe) {
            let completed = completed_len(&candles, timeframe, now);
            data.insert_shared(timeframe, candles, completed);
        }
    }
    data
}

/// Evaluate a signal on one coin using hot-cache completed bars.
#[must_use]
pub fn evaluate_coin(store: &MarketStore, definition: &SignalDefinition, coin: &str, now: DateTime<Utc>) -> EvalReport {
    let data = collect_coin_data(store, coin, definition, now);
    evaluate(definition, coin, &data)
}

/// Scope of coins one signal run covers.
#[derive(Debug, Clone)]
pub enum RunScope {
    /// Every coin in the cached universe.
    Universe,
    /// An explicit set of coins (for example, a saved watchlist).
    Coins(Vec<String>),
}

/// Evaluate a signal across a scope, one report per coin.
#[must_use]
pub fn run_signal(
    store: &MarketStore,
    definition: &SignalDefinition,
    scope: &RunScope,
    now: DateTime<Utc>,
) -> Vec<EvalReport> {
    match scope {
        RunScope::Universe => store
            .universe()
            .iter()
            .map(|coin| evaluate_coin(store, definition, &coin.coin, now))
            .collect(),
        RunScope::Coins(coins) => coins.iter().map(|coin| evaluate_coin(store, definition, coin, now)).collect(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use rust_decimal::Decimal;

    use super::completed_len;

    fn candle(hour: i64) -> Candle {
        Candle {
            open_time: Utc.timestamp_opt(hour * 3_600, 0).single().expect("valid timestamp"),
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ZERO,
        }
    }

    #[test]
    fn excludes_forming_trailing_bar() {
        let candles = vec![candle(0), candle(1), candle(2)];
        let now = Utc.timestamp_opt(2 * 3_600 + 1_800, 0).single().expect("valid timestamp");
        assert_eq!(completed_len(&candles, Timeframe::Hour1, now), 2);
    }

    #[test]
    fn includes_bar_at_exact_close_time() {
        let candles = vec![candle(0), candle(1)];
        let now = Utc.timestamp_opt(2 * 3_600, 0).single().expect("valid timestamp");
        assert_eq!(completed_len(&candles, Timeframe::Hour1, now), 2);
    }
}
