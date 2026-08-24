//! Divergence-based setup scanner over the Hyperliquid universe.
//!
//! Ties [`hl_feed`] candle backfill to the [`indicators`] divergence detector: for
//! each coin and timeframe it fetches candles, computes RSI and MACD, and reports
//! fresh regular/hidden divergences as [`Signal`]s. Pure detection lives in
//! [`signals_from_candles`]; the async entry points fetch, then delegate to it.

use std::time::Duration;

use chrono::{DateTime, Utc};
use hl_feed::{Candle, Timeframe, fetch_candles};
use indicators::{DivergenceDirection, DivergenceKind, detect_divergences, macd, rsi};
use rust_decimal::prelude::ToPrimitive;

/// Which oscillator produced a divergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Candles fetched per coin/timeframe.
    pub lookback: usize,
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
            lookback: 300,
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
        for divergence in detect_divergences(&closes, oscillator, config.pivot_left, config.pivot_right, config.max_gap) {
            if latest_confirmable.saturating_sub(divergence.recent_index) > config.recent_within {
                continue;
            }
            let Some(candle) = candles.get(divergence.recent_index) else { continue };
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

type ScanError = Box<dyn std::error::Error + Send + Sync>;

/// Scan one coin across all configured timeframes, fetching candles then running
/// [`signals_from_candles`].
pub async fn scan_coin(client: &reqwest::Client, coin: &str, config: &ScanConfig) -> Result<Vec<Signal>, ScanError> {
    let mut signals = Vec::new();
    for &timeframe in &config.timeframes {
        let candles = fetch_candles(client, coin, timeframe, config.lookback).await?;
        signals.extend(signals_from_candles(coin, timeframe, &candles, config));
    }
    Ok(signals)
}

/// A coin whose scan failed, kept rather than silently dropped.
#[derive(Debug, Clone)]
pub struct ScanFailure {
    /// Coin that failed.
    pub coin: String,
    /// Human-readable error.
    pub error: String,
}

/// The outcome of scanning a set of coins.
#[derive(Debug, Clone, Default)]
pub struct ScanReport {
    /// All detected signals.
    pub signals: Vec<Signal>,
    /// Coins whose scan failed (rate limits, transient network).
    pub failures: Vec<ScanFailure>,
}

/// Scan every coin in `coins`, pacing requests by `delay` to respect Hyperliquid
/// rate limits. Per-coin failures are collected into the report rather than
/// aborting the whole sweep.
pub async fn scan_universe(
    client: &reqwest::Client,
    coins: &[String],
    config: &ScanConfig,
    delay: Duration,
) -> ScanReport {
    let mut report = ScanReport::default();
    for coin in coins {
        match scan_coin(client, coin, config).await {
            Ok(signals) => report.signals.extend(signals),
            Err(error) => report.failures.push(ScanFailure { coin: coin.clone(), error: error.to_string() }),
        }
        tokio::time::sleep(delay).await;
    }
    report
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use hl_feed::{Candle, Timeframe};
    use rust_decimal::Decimal;

    use super::{ScanConfig, scan_coin, signals_from_candles};

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

    /// Live wiring check: scanning a real coin succeeds and every signal is tagged
    /// with the coin and a requested timeframe. Requires network access.
    #[tokio::test]
    async fn scan_coin_runs_against_live_data() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::new();
        let config = ScanConfig { timeframes: vec![Timeframe::Day1], ..ScanConfig::default() };
        let signals = scan_coin(&client, "BTC", &config).await.expect("scan BTC daily");
        assert!(signals.iter().all(|signal| signal.coin == "BTC" && signal.timeframe == Timeframe::Day1));
    }
}
