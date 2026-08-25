//! Stochastic oscillator (%K / %D), bounded 0–100.

/// Stochastic oscillator aligned index-for-index with the input.
///
/// `%K = 100 · (close − lowest_low) / (highest_high − lowest_low)` over the
/// `period` window; `%D` is the `smooth_d`-period simple moving average of `%K`.
/// A flat window (zero range) yields 50. `high`/`low`/`close` are expected to be
/// equal length.
#[derive(Debug, Clone)]
pub struct Stochastic {
    /// The fast %K line.
    pub k: Vec<Option<f64>>,
    /// The slow %D line (SMA of %K).
    pub d: Vec<Option<f64>>,
}

/// Stochastic oscillator over `period` with a `smooth_d`-period %D.
#[must_use]
pub fn stochastic(high: &[f64], low: &[f64], close: &[f64], period: usize, smooth_d: usize) -> Stochastic {
    let len = close.len();
    let mut result = Stochastic {
        k: vec![None; len],
        d: vec![None; len],
    };
    if period == 0 {
        return result;
    }
    let mut k_dense: Vec<f64> = Vec::new();
    for (index, (highs, lows)) in high.windows(period).zip(low.windows(period)).enumerate() {
        let slot = index + period - 1;
        let Some(&close) = close.get(slot) else {
            break;
        };
        let highest = highs.iter().copied().fold(f64::MIN, f64::max);
        let lowest = lows.iter().copied().fold(f64::MAX, f64::min);
        let range = highest - lowest;
        let value = if range > 0.0 {
            100.0 * (close - lowest) / range
        } else {
            50.0
        };
        k_dense.push(value);
        if let Some(cell) = result.k.get_mut(slot) {
            *cell = Some(value);
        }
    }

    if smooth_d > 0 {
        let smooth_f = smooth_d as f64;
        let d_start = period + smooth_d - 2;
        for (offset, window) in k_dense.windows(smooth_d).enumerate() {
            if let Some(cell) = result.d.get_mut(d_start + offset) {
                *cell = Some(window.iter().sum::<f64>() / smooth_f);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::stochastic;

    #[test]
    fn k_is_100_at_the_top_of_the_range() {
        let high = [2.0, 3.0, 4.0];
        let low = [1.0, 1.0, 1.0];
        let close = [1.5, 2.0, 4.0];
        // Window [.,.,.] range 1..4 -> %K = 100*(4-1)/(4-1) = 100 at index 2.
        let out = stochastic(&high, &low, &close, 3, 2);
        assert_eq!(out.k.get(2).copied().flatten(), Some(100.0));
    }

    #[test]
    fn k_is_none_until_the_period() {
        let out = stochastic(&[3.0, 4.0, 5.0], &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 3, 2);
        assert!(out.k.iter().take(2).all(Option::is_none));
        assert!(out.k.get(2).copied().flatten().is_some());
    }
}
