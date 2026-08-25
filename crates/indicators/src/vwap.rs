//! Volume-Weighted Average Price, cumulative from the start of the series.

/// Cumulative VWAP aligned index-for-index with the input: the running
/// volume-weighted average of the typical price `(high + low + close) / 3` from
/// the first bar to each bar. `None` only while cumulative volume is zero. All
/// slices are expected to be equal length.
///
/// This is the running (session-from-start) VWAP; anchored/rolling variants can be
/// layered on later if signalling needs them.
#[must_use]
pub fn vwap(high: &[f64], low: &[f64], close: &[f64], volume: &[f64]) -> Vec<Option<f64>> {
    let mut out = vec![None; close.len()];
    let mut cumulative_pv = 0.0;
    let mut cumulative_volume = 0.0;
    for (index, ((high, low), (close, volume))) in high.iter().zip(low).zip(close.iter().zip(volume)).enumerate() {
        let typical = (high + low + close) / 3.0;
        cumulative_pv += typical * volume;
        cumulative_volume += volume;
        if cumulative_volume > 0.0
            && let Some(slot) = out.get_mut(index)
        {
            *slot = Some(cumulative_pv / cumulative_volume);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::vwap;

    #[test]
    fn vwap_of_a_constant_price_is_that_price() {
        let price = [10.0; 5];
        let volume = [100.0; 5];
        for value in vwap(&price, &price, &price, &volume).into_iter().flatten() {
            assert!((value - 10.0).abs() < 1e-9);
        }
    }

    #[test]
    fn vwap_is_volume_weighted() {
        // Two bars: price 10 (vol 1), price 20 (vol 3) -> (10·1 + 20·3)/4 = 17.5 at index 1.
        let high = [10.0, 20.0];
        let volume = [1.0, 3.0];
        let out = vwap(&high, &high, &high, &volume);
        assert!((out.get(1).copied().flatten().expect("vwap") - 17.5).abs() < 1e-9);
    }
}
