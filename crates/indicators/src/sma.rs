//! Simple moving average (a price-pane overlay).

/// Simple moving average of `values` over `period`: `None` until `period` samples,
/// then the rolling mean, aligned index-for-index with the input.
#[must_use]
pub fn sma(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    if period == 0 {
        return out;
    }
    let period_f = period as f64;
    for (index, window) in values.windows(period).enumerate() {
        if let Some(slot) = out.get_mut(index + period - 1) {
            *slot = Some(window.iter().sum::<f64>() / period_f);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::sma;

    #[test]
    fn sma_of_a_constant_is_the_constant() {
        for value in sma(&[5.0; 10], 4).into_iter().flatten() {
            assert!((value - 5.0).abs() < 1e-9);
        }
    }

    #[test]
    fn sma_is_none_until_the_period_is_reached() {
        let out = sma(&[1.0, 2.0, 3.0, 4.0], 3);
        assert!(out.iter().take(2).all(Option::is_none));
        assert_eq!(out.get(2).copied().flatten(), Some(2.0));
        assert_eq!(out.get(3).copied().flatten(), Some(3.0));
    }

    #[test]
    fn sma_is_all_none_when_period_exceeds_input() {
        let out = sma(&[1.0, 2.0], 5);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(Option::is_none));
    }
}
