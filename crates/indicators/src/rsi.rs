//! Wilder's Relative Strength Index.

/// Wilder's RSI over `closes` for `period`.
///
/// Returns one entry per close, aligned index-for-index with the input: `None`
/// until `period` price changes are available (so the first value lands at index
/// `period`), then the smoothed RSI in `0.0..=100.0`. A flat market yields 50, a
/// pure advance 100, a pure decline 0.
#[must_use]
pub fn rsi(closes: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out: Vec<Option<f64>> = vec![None; closes.len()];
    if period == 0 || closes.len() <= period {
        return out;
    }

    // Consecutive price changes; `deltas[i]` is the move into `closes[i + 1]`.
    let deltas: Vec<f64> = closes
        .iter()
        .zip(closes.iter().skip(1))
        .map(|(prev, next)| next - prev)
        .collect();

    #[expect(clippy::cast_precision_loss, reason = "indicator periods are tiny, well within f64")]
    let period_f = period as f64;

    // Seed the averages with the simple mean of the first `period` gains/losses.
    let mut avg_gain = 0.0;
    let mut avg_loss = 0.0;
    for delta in deltas.iter().take(period) {
        avg_gain += delta.max(0.0);
        avg_loss += (-delta).max(0.0);
    }
    avg_gain /= period_f;
    avg_loss /= period_f;

    if let Some(slot) = out.get_mut(period) {
        *slot = Some(rsi_from(avg_gain, avg_loss));
    }

    // Wilder smoothing for every later bar.
    for (offset, delta) in deltas.iter().enumerate().skip(period) {
        avg_gain = (avg_gain * (period_f - 1.0) + delta.max(0.0)) / period_f;
        avg_loss = (avg_loss * (period_f - 1.0) + (-delta).max(0.0)) / period_f;
        if let Some(slot) = out.get_mut(offset + 1) {
            *slot = Some(rsi_from(avg_gain, avg_loss));
        }
    }

    out
}

/// RSI from smoothed average gain/loss. Guards the divide-by-zero endpoints
/// (`avg_loss <= 0.0`, since both averages are non-negative) rather than an exact
/// float compare.
fn rsi_from(avg_gain: f64, avg_loss: f64) -> f64 {
    if avg_loss <= 0.0 {
        return if avg_gain <= 0.0 { 50.0 } else { 100.0 };
    }
    100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
}

#[cfg(test)]
mod tests {
    use super::rsi;

    #[test]
    fn none_until_period_satisfied() {
        let closes: Vec<f64> = (1..=20).map(f64::from).collect();
        let out = rsi(&closes, 14);
        assert_eq!(out.len(), closes.len());
        assert!(out.iter().take(14).all(Option::is_none));
        assert!(out.get(14).copied().flatten().is_some());
    }

    #[test]
    fn all_gains_saturate_to_100() {
        let closes: Vec<f64> = (1..=30).map(f64::from).collect();
        let last = rsi(&closes, 14).into_iter().flatten().last().expect("some rsi");
        assert!((last - 100.0).abs() < 1e-9, "monotonic rise should give RSI 100, got {last}");
    }

    #[test]
    fn all_losses_sink_to_zero() {
        let closes: Vec<f64> = (1..=30).rev().map(f64::from).collect();
        let last = rsi(&closes, 14).into_iter().flatten().last().expect("some rsi");
        assert!(last.abs() < 1e-9, "monotonic fall should give RSI 0, got {last}");
    }

    #[test]
    fn mixed_series_stays_in_range() {
        let closes = [
            44.0, 44.3, 44.1, 44.2, 44.5, 43.9, 44.6, 45.0, 44.8, 45.2, 45.1, 45.4, 45.0, 45.5, 45.2, 45.6,
        ];
        for value in rsi(&closes, 14).into_iter().flatten() {
            assert!((0.0..=100.0).contains(&value), "RSI out of range: {value}");
        }
    }
}
