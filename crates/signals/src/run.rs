//! Store-backed signal runs: collect completed candle slices per coin and evaluate.
//!
//! The hot cache includes the still-forming trailing bar. Runs expose only its
//! completed prefix, sharing the `Arc` when it is deep enough and loading only a
//! missing older prefix otherwise. A series is omitted while its expected current
//! bar is missing or a gap is being reconciled, avoiding repainting and stale matches.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use hl_feed::{CandleHistory, MarketStore};

use crate::definition::{DefinitionError, SignalDefinition};
use crate::eval::{CoinData, EvalReport, evaluate_validated};

/// Length of the completed prefix in an oldest-first candle series.
fn completed_len(candles: &[Candle], timeframe: Timeframe, now: DateTime<Utc>) -> usize {
    let now_ms = now.timestamp_millis();
    candles
        .partition_point(|candle| candle.open_time.timestamp_millis().saturating_add(timeframe.duration_ms()) <= now_ms)
}

async fn hydrate_timeframe(
    data: &mut CoinData,
    history: &CandleHistory,
    coin: &str,
    timeframe: Timeframe,
    required: usize,
    candles: Arc<Vec<Candle>>,
    now: DateTime<Utc>,
) {
    let completed = completed_len(&candles, timeframe, now);
    if completed >= required {
        data.insert_shared(timeframe, candles, completed);
        return;
    }
    let Some(first) = candles.first() else {
        return;
    };

    let missing = required - completed;
    match history
        .load_before(coin, timeframe, first.open_time.timestamp_millis(), missing)
        .await
    {
        Ok(mut older) => {
            // History ends at the first hot bar; `current_candles` guarantees
            // that the remaining hot prefix is aligned and contiguous.
            let (completed_candles, _) = candles.split_at(completed);
            older.extend_from_slice(completed_candles);
            data.insert(timeframe, older);
        }
        Err(error) => data.mark_unavailable(timeframe, error.to_string()),
    }
}

async fn hydrate_coin_data(
    store: &MarketStore,
    history: &CandleHistory,
    coin: &str,
    requirements: &BTreeMap<Timeframe, usize>,
    now: DateTime<Utc>,
) -> CoinData {
    let mut data = CoinData::default();
    let now_ms = now.timestamp_millis();
    for (&timeframe, &required) in requirements {
        if let Some(candles) = store.current_candles(coin, timeframe, now_ms) {
            hydrate_timeframe(&mut data, history, coin, timeframe, required, candles, now).await;
        }
    }
    data
}

/// Validate and evaluate a signal on one coin using lazily hydrated completed bars.
pub async fn evaluate_coin(
    store: &MarketStore,
    history: &CandleHistory,
    definition: &SignalDefinition,
    coin: &str,
    now: DateTime<Utc>,
) -> Result<EvalReport, DefinitionError> {
    let requirements = definition.required_history()?;
    let data = hydrate_coin_data(store, history, coin, &requirements, now).await;
    evaluate_validated(definition, coin, &data)
}

/// One completed coin evaluation within a batch run.
#[derive(Debug, Clone)]
pub struct RunProgress {
    /// Number of coins evaluated, including this report.
    pub completed: usize,
    /// Fixed number of coins in the run.
    pub total: usize,
    /// Evaluation result for the coin just completed.
    pub report: EvalReport,
}

/// Terminal accounting for a batch run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSummary {
    /// Number of coins evaluated.
    pub completed: usize,
    /// Fixed number of coins in the run.
    pub total: usize,
    /// Whether the progress consumer requested an early stop.
    pub stopped: bool,
}

/// Validate and evaluate a fixed coin snapshot sequentially.
///
/// The callback receives each report immediately and can stop the remaining
/// work by returning [`ControlFlow::Break`].
pub async fn run_signal<F>(
    store: &MarketStore,
    history: &CandleHistory,
    definition: &SignalDefinition,
    coins: &[String],
    now: DateTime<Utc>,
    mut on_progress: F,
) -> Result<RunSummary, DefinitionError>
where
    F: FnMut(RunProgress) -> ControlFlow<()>,
{
    let requirements = definition.required_history()?;
    let total = coins.len();
    let mut completed = 0;
    for coin in coins {
        let data = hydrate_coin_data(store, history, coin, &requirements, now).await;
        let report = evaluate_validated(definition, coin, &data)?;
        completed += 1;
        if on_progress(RunProgress {
            completed,
            total,
            report,
        })
        .is_break()
        {
            return Ok(RunSummary {
                completed,
                total,
                stopped: true,
            });
        }
    }
    Ok(RunSummary {
        completed,
        total,
        stopped: false,
    })
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;
    use std::sync::{Arc, Mutex, PoisonError};

    use async_trait::async_trait;
    use chrono::{DateTime, TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use hl_feed::{CandleHistory, HyperliquidRest, MarketStore};
    use indicators::Instance;
    use rust_decimal::Decimal;
    use storage::{CandleSeries, CandleStore, StoreError};

    use super::{RunSummary, completed_len, hydrate_timeframe, run_signal};
    use crate::definition::{
        ComparisonOp, Condition, DEFINITION_VERSION, Direction, Node, Operand, Predicate, SignalDefinition,
    };
    use crate::eval::{CoinData, OutcomeStatus, evaluate};

    struct CachedStore {
        candles: Vec<Candle>,
        fail: bool,
        requests: Mutex<Vec<(i64, usize)>>,
    }

    impl CachedStore {
        fn new(candles: Vec<Candle>, fail: bool) -> Self {
            Self {
                candles,
                fail,
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl CandleStore for CachedStore {
        async fn load_recent(&self, _limit: usize) -> Result<Vec<CandleSeries>, StoreError> {
            Ok(Vec::new())
        }

        async fn save(&self, _coin: &str, _timeframe: Timeframe, _candles: &[Candle]) -> Result<(), StoreError> {
            panic!("complete cached history must not be written")
        }

        async fn candles_before(
            &self,
            _coin: &str,
            _timeframe: Timeframe,
            before_ms: i64,
            limit: usize,
        ) -> Result<Vec<Candle>, StoreError> {
            self.requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((before_ms, limit));
            if self.fail {
                return Err(StoreError::InvalidCandleRow(before_ms));
            }
            let mut candles: Vec<Candle> = self
                .candles
                .iter()
                .rev()
                .filter(|candle| candle.open_time.timestamp_millis() < before_ms)
                .take(limit)
                .cloned()
                .collect();
            candles.reverse();
            Ok(candles)
        }
    }

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

    fn definition(period: f64) -> SignalDefinition {
        SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bullish,
            root: Node::Condition { id: "trend".to_owned() },
            conditions: [(
                "trend".to_owned(),
                Condition {
                    timeframe: Timeframe::Hour1,
                    predicate: Predicate::Threshold {
                        left: Operand::Indicator {
                            instance: Instance {
                                id: "sma".to_owned(),
                                params: [("period".to_owned(), period)].into_iter().collect(),
                            },
                            output: "sma".to_owned(),
                            adjustment: None,
                        },
                        op: ComparisonOp::GreaterThan,
                        right: Operand::Constant { value: 0.0 },
                    },
                    within_bars: 0,
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    fn history(store: Arc<CachedStore>) -> CandleHistory {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let rest = Arc::new(HyperliquidRest::new(reqwest::Client::new(), 1100));
        CandleHistory::new(store, rest)
    }

    fn hot_candles() -> Arc<Vec<Candle>> {
        Arc::new((5..=10).map(candle).collect())
    }

    fn during_forming_bar() -> DateTime<Utc> {
        Utc.timestamp_opt(10 * 3_600 + 1_800, 0).single().expect("valid timestamp")
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

    #[tokio::test]
    async fn deep_enough_hot_history_does_not_query_storage() {
        let store = Arc::new(CachedStore::new(Vec::new(), true));
        let history = history(store.clone());
        let signal = definition(3.0);
        let mut data = CoinData::default();

        hydrate_timeframe(
            &mut data,
            &history,
            "BTC",
            Timeframe::Hour1,
            3,
            hot_candles(),
            during_forming_bar(),
        )
        .await;
        let report = evaluate(&signal, "BTC", &data).expect("valid shared-hot signal");

        assert_eq!(report.conditions["trend"].status, OutcomeStatus::Satisfied);
        assert!(store.requests.lock().unwrap_or_else(PoisonError::into_inner).is_empty());
    }

    #[tokio::test]
    async fn hydrates_only_the_missing_prefix_before_the_hot_buffer() {
        let store = Arc::new(CachedStore::new((0..5).map(candle).collect(), false));
        let history = history(store.clone());
        let signal = definition(8.0);
        let mut data = CoinData::default();

        hydrate_timeframe(
            &mut data,
            &history,
            "BTC",
            Timeframe::Hour1,
            8,
            hot_candles(),
            during_forming_bar(),
        )
        .await;
        let report = evaluate(&signal, "BTC", &data).expect("valid hydrated signal");

        assert_eq!(report.conditions["trend"].status, OutcomeStatus::Satisfied);
        let requests = store.requests.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(requests.as_slice(), &[(5 * Timeframe::Hour1.duration_ms(), 3)]);
    }

    #[tokio::test]
    async fn batch_reports_progress_and_stops_without_evaluating_the_rest() {
        let store = Arc::new(CachedStore::new(Vec::new(), true));
        let history = history(store.clone());
        let market = MarketStore::new();
        let signal = definition(1.0);
        let coins = vec!["BTC".to_owned(), "ETH".to_owned()];
        let mut seen = Vec::new();

        let summary = run_signal(&market, &history, &signal, &coins, during_forming_bar(), |progress| {
            seen.push((progress.completed, progress.total, progress.report.coin));
            ControlFlow::Break(())
        })
        .await
        .expect("valid batch signal");

        assert_eq!(
            summary,
            RunSummary {
                completed: 1,
                total: 2,
                stopped: true,
            }
        );
        assert_eq!(seen, vec![(1, 2, "BTC".to_owned())]);
        assert!(store.requests.lock().unwrap_or_else(PoisonError::into_inner).is_empty());
    }

    #[tokio::test]
    async fn history_failures_become_unavailable() {
        let store = Arc::new(CachedStore::new(Vec::new(), true));
        let history = history(store);
        let signal = definition(8.0);
        let mut data = CoinData::default();

        hydrate_timeframe(
            &mut data,
            &history,
            "BTC",
            Timeframe::Hour1,
            8,
            hot_candles(),
            during_forming_bar(),
        )
        .await;
        let report = evaluate(&signal, "BTC", &data).expect("valid unavailable-history signal");

        assert_eq!(report.conditions["trend"].status, OutcomeStatus::Unavailable);
        assert!(
            report.unavailable_data[&Timeframe::Hour1].contains("cached candle row"),
            "the storage failure should remain visible"
        );
    }
}
