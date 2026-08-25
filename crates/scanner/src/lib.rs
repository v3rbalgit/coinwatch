//! Divergence-based setup scanner over the cached Hyperliquid universe.
//!
//! Reads candles from the [`MarketStore`] hot cache and runs
//! RSI/MACD divergence detection — pure CPU, no network on the scan path. The market
//! sync engine keeps the cache warm; this crate only computes. Pure detection lives
//! in [`signals_from_candles`]; [`scan_coin`] and [`scan_universe`] fan it over the
//! cache.

use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use hl_feed::MarketStore;
use indicators::{DivergenceDirection, DivergenceKind, detect_divergences, macd, rsi};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

/// Which oscillator produced a divergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SignalSource {
    /// RSI divergence.
    Rsi,
    /// MACD-line divergence.
    Macd,
}

/// A detected setup: one divergence on one coin and timeframe.
#[derive(Debug, Clone)]
pub struct Signal {
    /// Coin symbol.
    pub coin: String,
    /// Timeframe the divergence was found on.
    pub timeframe: Timeframe,
    /// Oscillator that diverged.
    pub source: SignalSource,
    /// Regular (reversal) or hidden (continuation).
    pub kind: DivergenceKind,
    /// Bullish or bearish.
    pub direction: DivergenceDirection,
    /// Open time of the confirming (recent) pivot.
    pub at: DateTime<Utc>,
}

/// Scan parameters. [`ScanConfig::default`] uses the classic RSI(14) /
/// MACD(12,26,9) periods over 1h/4h/1d/1w with 5-bar swing pivots.
#[derive(Debug, Clone)]
pub struct ScanConfig {
    /// Timeframes to scan.
    pub timeframes: Vec<Timeframe>,
    /// RSI period.
    pub rsi_period: usize,
    /// MACD fast/slow/signal periods.
    pub macd_periods: (usize, usize, usize),
    /// Bars of confirmation before a swing pivot.
    pub pivot_left: usize,
    /// Bars of confirmation after a swing pivot.
    pub pivot_right: usize,
    /// Maximum bars between the two pivots of a divergence.
    pub max_gap: usize,
    /// Only report divergences whose confirming pivot is within this many bars of
    /// the most recent confirmable pivot (freshness).
    pub recent_within: usize,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            timeframes: vec![Timeframe::Hour1, Timeframe::Hour4, Timeframe::Day1, Timeframe::Week1],
            rsi_period: 14,
            macd_periods: (12, 26, 9),
            pivot_left: 5,
            pivot_right: 5,
            max_gap: 60,
            recent_within: 3,
        }
    }
}

/// Detect fresh RSI and MACD divergences in one coin/timeframe's candles.
#[must_use]
pub fn signals_from_candles(coin: &str, timeframe: Timeframe, candles: &[Candle], config: &ScanConfig) -> Vec<Signal> {
    let closes: Vec<f64> = candles.iter().filter_map(|candle| candle.close.to_f64()).collect();
    if closes.len() != candles.len() {
        return Vec::new();
    }

    let (fast, slow, signal) = config.macd_periods;
    let rsi_series = rsi(&closes, config.rsi_period);
    let macd_line = macd(&closes, fast, slow, signal).macd;

    // The freshest confirmable pivot sits `pivot_right` bars back from the end.
    let latest_confirmable = closes.len().saturating_sub(1 + config.pivot_right);

    let mut signals = Vec::new();
    for (source, oscillator) in [(SignalSource::Rsi, &rsi_series), (SignalSource::Macd, &macd_line)] {
        for divergence in detect_divergences(&closes, oscillator, config.pivot_left, config.pivot_right, config.max_gap)
        {
            if latest_confirmable.saturating_sub(divergence.recent_index) > config.recent_within {
                continue;
            }
            let Some(candle) = candles.get(divergence.recent_index) else {
                continue;
            };
            signals.push(Signal {
                coin: coin.to_owned(),
                timeframe,
                source,
                kind: divergence.kind,
                direction: divergence.direction,
                at: candle.open_time,
            });
        }
    }
    signals
}

/// Scan one coin across all configured timeframes using cached candles — pure CPU,
/// no network. Timeframes with nothing cached yet are skipped.
#[must_use]
pub fn scan_coin(store: &MarketStore, coin: &str, config: &ScanConfig) -> Vec<Signal> {
    let mut signals = Vec::new();
    for &timeframe in &config.timeframes {
        if let Some(candles) = store.candles(coin, timeframe) {
            signals.extend(signals_from_candles(coin, timeframe, &candles, config));
        }
    }
    signals
}

/// Scan the whole cached universe. Reads only the cache, so it runs in
/// milliseconds regardless of universe size.
#[must_use]
pub fn scan_universe(store: &MarketStore, config: &ScanConfig) -> Vec<Signal> {
    store
        .universe()
        .iter()
        .flat_map(|coin| scan_coin(store, &coin.coin, config))
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use hl_feed::MarketStore;
    use rust_decimal::Decimal;

    use super::{ScanConfig, scan_universe, signals_from_candles};

    fn candles_from(closes: &[f64]) -> Vec<Candle> {
        closes
            .iter()
            .enumerate()
            .map(|(index, &close)| {
                let price = Decimal::try_from(close).expect("finite close");
                let secs = i64::try_from(index).unwrap_or(0) * 3600;
                Candle {
                    open_time: Utc.timestamp_opt(secs, 0).single().expect("valid timestamp"),
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: Decimal::ZERO,
                }
            })
            .collect()
    }

    #[test]
    fn too_few_candles_yield_no_signals() {
        let candles = candles_from(&[1.0, 2.0, 3.0]);
        assert!(signals_from_candles("TEST", Timeframe::Hour1, &candles, &ScanConfig::default()).is_empty());
    }

    #[test]
    fn clean_uptrend_has_no_divergence() {
        let closes: Vec<f64> = (1..=200).map(f64::from).collect();
        let candles = candles_from(&closes);
        let signals = signals_from_candles("TEST", Timeframe::Hour1, &candles, &ScanConfig::default());
        assert!(signals.is_empty(), "a monotonic uptrend must not diverge: {signals:?}");
    }

    #[test]
    fn empty_store_yields_no_signals() {
        let store = MarketStore::new();
        assert!(scan_universe(&store, &ScanConfig::default()).is_empty());
    }
}
