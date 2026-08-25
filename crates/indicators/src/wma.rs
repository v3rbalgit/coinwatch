//! Weighted moving average (a price-pane overlay): linear weights favouring
//! recent bars.

/// Weighted moving average of `values` over `period` with linear weights — the
/// most recent bar in each window weighted `period`, the oldest `1`.
#[must_use]
pub fn wma(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    if period == 0 {
        return out;
    }
    let weight_total = (period * (period + 1) / 2) as f64;
    for (index, window) in values.windows(period).enumerate() {
        let weighted: f64 = window
            .iter()
            .enumerate()
            .map(|(offset, value)| (offset + 1) as f64 * value)
            .sum();
        if let Some(slot) = out.get_mut(index + period - 1) {
            *slot = Some(weighted / weight_total);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::wma;

    #[test]
    fn wma_of_a_constant_is_the_constant() {
        for value in wma(&[3.0; 8], 4).into_iter().flatten() {
            assert!((value - 3.0).abs() < 1e-9);
        }
    }

    #[test]
    fn wma_leans_toward_recent_values() {
        // Weights 1,2,3 over [1,2,3] = (1+4+9)/6 = 14/6, above the simple mean 2.0.
        let out = wma(&[1.0, 2.0, 3.0], 3);
        assert!((out.get(2).copied().flatten().expect("wma") - 14.0 / 6.0).abs() < 1e-9);
    }
}
