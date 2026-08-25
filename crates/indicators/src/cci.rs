//! Commodity Channel Index (a momentum oscillator around zero).

/// Commodity Channel Index over `period`, aligned index-for-index with the input.
///
/// Uses the typical price `(high + low + close) / 3`:
/// `CCI = (TP − SMA(TP)) / (0.015 · mean_deviation(TP))` over the window. A window
/// with zero mean deviation yields 0. `high`/`low`/`close` are expected to be
/// equal length.
#[must_use]
pub fn cci(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; close.len()];
    if period == 0 {
        return out;
    }
    let typical: Vec<f64> = high
        .iter()
        .zip(low)
        .zip(close)
        .map(|((high, low), close)| (high + low + close) / 3.0)
        .collect();
    let period_f = period as f64;
    for (index, window) in typical.windows(period).enumerate() {
        let mean = window.iter().sum::<f64>() / period_f;
        let mean_deviation = window.iter().map(|value| (value - mean).abs()).sum::<f64>() / period_f;
        let slot = index + period - 1;
        let Some(&price) = typical.get(slot) else {
            break;
        };
        let value = if mean_deviation > 0.0 {
            (price - mean) / (0.015 * mean_deviation)
        } else {
            0.0
        };
        if let Some(cell) = out.get_mut(slot) {
            *cell = Some(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::cci;

    #[test]
    fn cci_is_zero_on_a_flat_series() {
        let flat = [10.0; 12];
        for value in cci(&flat, &flat, &flat, 5).into_iter().flatten() {
            assert!(value.abs() < 1e-9, "flat series should give CCI 0, got {value}");
        }
    }

    #[test]
    fn cci_is_positive_at_a_new_high() {
        let high: Vec<f64> = (1..=20).map(f64::from).collect();
        let last = cci(&high, &high, &high, 5).into_iter().flatten().last().expect("cci");
        assert!(last > 0.0, "a steady rise should give positive CCI, got {last}");
    }
}
