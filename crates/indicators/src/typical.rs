/// Typical price `(high + low + close) / 3` for aligned OHLC columns.
pub(crate) fn typical_prices(high: &[f64], low: &[f64], close: &[f64]) -> Vec<f64> {
    high.iter()
        .zip(low)
        .zip(close)
        .map(|((high, low), close)| (high + low + close) / 3.0)
        .collect()
}
