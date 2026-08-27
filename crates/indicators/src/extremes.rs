//! Rolling-window extreme helpers shared by range-based indicators.

/// `(highest, lowest)` over one aligned high/low window.
pub(crate) fn window_extremes(highs: &[f64], lows: &[f64]) -> (f64, f64) {
    let highest = highs.iter().copied().fold(f64::MIN, f64::max);
    let lowest = lows.iter().copied().fold(f64::MAX, f64::min);
    (highest, lowest)
}
