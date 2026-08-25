//! Average True Range (a volatility measure), Wilder-smoothed.

/// Average True Range over `period`, aligned index-for-index with the input.
///
/// True range per bar is `max(high-low, |high-prev_close|, |low-prev_close|)`
/// (the first bar has no previous close, so it is `high-low`). ATR seeds with the
/// mean of the first `period` true ranges (landing at index `period-1`), then uses
/// Wilder smoothing. `high`/`low`/`close` are expected to be equal length.
#[must_use]
pub fn atr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; close.len()];
    if period == 0 {
        return out;
    }
    let mut ranges: Vec<f64> = Vec::with_capacity(close.len());
    let mut prev_close: Option<f64> = None;
    for ((high, low), close) in high.iter().zip(low).zip(close) {
        let range = match prev_close {
            Some(prev) => (high - low).max((high - prev).abs()).max((low - prev).abs()),
            None => high - low,
        };
        ranges.push(range);
        prev_close = Some(*close);
    }
    if ranges.len() < period {
        return out;
    }

    let period_f = period as f64;
    let mut atr = ranges.iter().take(period).sum::<f64>() / period_f;
    if let Some(slot) = out.get_mut(period - 1) {
        *slot = Some(atr);
    }
    for (index, range) in ranges.iter().enumerate().skip(period) {
        atr = (atr * (period_f - 1.0) + range) / period_f;
        if let Some(slot) = out.get_mut(index) {
            *slot = Some(atr);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::atr;

    #[test]
    fn atr_of_constant_range_is_that_range() {
        let high = [11.0; 10];
        let low = [10.0; 10];
        let close = [10.5; 10];
        // Every true range is 1.0 (high-low dominates the flat closes), so ATR -> 1.0.
        for value in atr(&high, &low, &close, 3).into_iter().flatten() {
            assert!((value - 1.0).abs() < 1e-9, "expected ATR 1.0, got {value}");
        }
    }

    #[test]
    fn atr_is_none_until_the_period() {
        let high: Vec<f64> = (1..=10).map(|n| f64::from(n) + 0.5).collect();
        let low: Vec<f64> = (1..=10).map(f64::from).collect();
        let close: Vec<f64> = (1..=10).map(f64::from).collect();
        let out = atr(&high, &low, &close, 5);
        assert!(out.iter().take(4).all(Option::is_none));
        assert!(out.get(4).copied().flatten().is_some());
    }
}
