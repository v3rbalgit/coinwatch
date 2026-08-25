//! WebAssembly bindings for [`indicators`]. The chart computes RSI/MACD and
//! divergences from the exact same Rust code the scanner uses, so the two can
//! never drift. The pure [`analyze`] entry point is shared by the native test and
//! the wasm export.

use indicators::{DivergenceDirection, DivergenceKind, detect_divergences, macd, rsi};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

mod engine;

/// Indicator parameters (camelCase to match the JS caller).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    /// RSI period.
    pub rsi_period: usize,
    /// MACD fast EMA period.
    pub macd_fast: usize,
    /// MACD slow EMA period.
    pub macd_slow: usize,
    /// MACD signal EMA period.
    pub macd_signal: usize,
    /// Bars of confirmation before a swing pivot.
    pub pivot_left: usize,
    /// Bars of confirmation after a swing pivot.
    pub pivot_right: usize,
    /// Maximum bars between the two pivots of a divergence.
    pub max_gap: usize,
}

/// The oscillator a divergence was found on (serialized as `RSI` / `MACD`).
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Source {
    /// Relative Strength Index.
    Rsi,
    /// Moving-average convergence/divergence.
    Macd,
}

/// A divergence with pivot indices into the input series; the caller maps the
/// indices to bar times and prices. `source`/`kind`/`direction` serialize to their
/// variant names (`RSI`/`MACD`, `Regular`/`Hidden`, `Bullish`/`Bearish`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DivergenceOut {
    /// Oscillator that diverged.
    pub source: Source,
    /// Reversal (`Regular`) or continuation (`Hidden`).
    pub kind: DivergenceKind,
    /// Bullish or bearish.
    pub direction: DivergenceDirection,
    /// Index of the earlier pivot.
    pub prior_index: usize,
    /// Index of the later pivot.
    pub recent_index: usize,
}

/// Indicator series (`NaN` where undefined) plus detected divergences.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Analysis {
    /// RSI series.
    pub rsi: Vec<f64>,
    /// MACD line.
    pub macd: Vec<f64>,
    /// Signal line.
    pub signal: Vec<f64>,
    /// MACD histogram.
    pub histogram: Vec<f64>,
    /// Divergences detected on RSI and MACD.
    pub divergences: Vec<DivergenceOut>,
}

fn nan_vec(series: &[Option<f64>]) -> Vec<f64> {
    series.iter().map(|value| value.unwrap_or(f64::NAN)).collect()
}

/// Compute RSI/MACD series and RSI+MACD divergences over `closes`. Pure and
/// framework-free — shared by the native test and the wasm export.
#[must_use]
pub fn analyze(closes: &[f64], config: &Config) -> Analysis {
    let rsi_series = rsi(closes, config.rsi_period);
    let macd_out = macd(closes, config.macd_fast, config.macd_slow, config.macd_signal);

    let mut divergences = Vec::new();
    for (source, oscillator) in [(Source::Rsi, &rsi_series), (Source::Macd, &macd_out.macd)] {
        for divergence in detect_divergences(closes, oscillator, config.pivot_left, config.pivot_right, config.max_gap)
        {
            divergences.push(DivergenceOut {
                source,
                kind: divergence.kind,
                direction: divergence.direction,
                prior_index: divergence.prior_index,
                recent_index: divergence.recent_index,
            });
        }
    }

    Analysis {
        rsi: nan_vec(&rsi_series),
        macd: nan_vec(&macd_out.macd),
        signal: nan_vec(&macd_out.signal),
        histogram: nan_vec(&macd_out.histogram),
        divergences,
    }
}

/// Wasm entry point: `analyzeSeries(closes, config)` returns the [`Analysis`] as a
/// plain JS object (indicator arrays use `NaN` for undefined bars).
#[wasm_bindgen(js_name = analyzeSeries)]
pub fn analyze_series(closes: &[f64], config: JsValue) -> Result<JsValue, JsValue> {
    let config: Config =
        serde_wasm_bindgen::from_value(config).map_err(|error| JsValue::from_str(&error.to_string()))?;
    let analysis = analyze(closes, &config);
    serde_wasm_bindgen::to_value(&analysis).map_err(|error| JsValue::from_str(&error.to_string()))
}

/// Wasm entry point: the indicator registry (metadata for the chart's toggle UI).
#[wasm_bindgen(js_name = indicatorDescriptors)]
pub fn indicator_descriptors() -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(&engine::descriptors()).map_err(|error| JsValue::from_str(&error.to_string()))
}

/// Wasm entry point: compute configured indicator instances over an OHLCV series.
#[wasm_bindgen(js_name = computeIndicators)]
pub fn compute_indicators(
    instances: JsValue,
    high: &[f64],
    low: &[f64],
    close: &[f64],
    volume: &[f64],
) -> Result<JsValue, JsValue> {
    let instances: Vec<engine::Instance> =
        serde_wasm_bindgen::from_value(instances).map_err(|error| JsValue::from_str(&error.to_string()))?;
    let data = engine::Ohlcv {
        high,
        low,
        close,
        volume,
    };
    let outputs = engine::compute(&instances, &data);
    serde_wasm_bindgen::to_value(&outputs).map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{Config, analyze};

    fn config() -> Config {
        Config {
            rsi_period: 14,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            pivot_left: 5,
            pivot_right: 5,
            max_gap: 60,
        }
    }

    #[test]
    fn rsi_saturates_on_a_monotonic_rise() {
        let closes: Vec<f64> = (1..=60).map(f64::from).collect();
        let analysis = analyze(&closes, &config());
        assert_eq!(analysis.rsi.len(), closes.len());
        let last = analysis.rsi.into_iter().rev().find(|value| !value.is_nan()).expect("some rsi");
        assert!((last - 100.0).abs() < 1e-9, "monotonic rise should give RSI 100, got {last}");
    }

    #[test]
    fn clean_trend_has_no_divergence() {
        let closes: Vec<f64> = (1..=200).map(f64::from).collect();
        assert!(
            analyze(&closes, &config()).divergences.is_empty(),
            "a monotonic trend must not diverge"
        );
    }
}
