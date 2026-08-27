//! Williams %R (a momentum oscillator), bounded −100–0.

use crate::extremes::window_extremes;

/// Williams %R over `period`, aligned index-for-index with the input:
/// `%R = -100 · (highest_high − close) / (highest_high − lowest_low)`. A flat
/// window (zero range) yields −50. `high`/`low`/`close` are expected to be equal
/// length.
#[must_use]
pub fn williams_r(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; close.len()];
    if period == 0 {
        return out;
    }
    for (index, (highs, lows)) in high.windows(period).zip(low.windows(period)).enumerate() {
        let slot = index + period - 1;
        let Some(&close) = close.get(slot) else {
            break;
        };
        let (highest, lowest) = window_extremes(highs, lows);
        let range = highest - lowest;
        let value = if range > 0.0 {
            -100.0 * (highest - close) / range
        } else {
            -50.0
        };
        if let Some(cell) = out.get_mut(slot) {
            *cell = Some(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::williams_r;

    #[test]
    fn r_is_zero_at_the_top_of_the_range() {
        let out = williams_r(&[2.0, 3.0, 4.0], &[1.0, 1.0, 1.0], &[1.5, 2.0, 4.0], 3);
        // close == highest -> %R = 0 at index 2.
        assert_eq!(out.get(2).copied().flatten(), Some(0.0));
    }

    #[test]
    fn r_stays_in_range() {
        let high: Vec<f64> = (1..=20).map(|n| f64::from(n) + 1.0).collect();
        let low: Vec<f64> = (1..=20).map(f64::from).collect();
        let close: Vec<f64> = (1..=20).map(|n| f64::from(n) + 0.5).collect();
        for value in williams_r(&high, &low, &close, 5).into_iter().flatten() {
            assert!((-100.0..=0.0).contains(&value), "%R out of range: {value}");
        }
    }
}
