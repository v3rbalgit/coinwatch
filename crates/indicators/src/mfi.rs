//! Money Flow Index (a volume-weighted momentum oscillator), bounded 0–100.

use crate::typical::typical_prices;

/// Money Flow Index over `period`, aligned index-for-index with the input.
///
/// Uses the typical price `(high + low + close) / 3` and raw money flow
/// `typical · volume`, classed positive/negative by the typical price's move from
/// the previous bar. `MFI = 100 − 100 / (1 + positive_flow / negative_flow)` over
/// the window. The first bar has no direction, so values begin at index `period`.
/// All slices are expected to be equal length.
#[must_use]
pub fn mfi(high: &[f64], low: &[f64], close: &[f64], volume: &[f64], period: usize) -> Vec<Option<f64>> {
    let len = close.len();
    let mut out = vec![None; len];
    if period == 0 {
        return out;
    }
    let typical = typical_prices(high, low, close);

    let mut positive = vec![0.0; len];
    let mut negative = vec![0.0; len];
    let mut prev_typical: Option<f64> = None;
    for (index, (typical, volume)) in typical.iter().zip(volume).enumerate() {
        if let Some(prev) = prev_typical {
            let flow = typical * volume;
            let target = if *typical > prev {
                positive.get_mut(index)
            } else if *typical < prev {
                negative.get_mut(index)
            } else {
                None
            };
            if let Some(cell) = target {
                *cell = flow;
            }
        }
        prev_typical = Some(*typical);
    }

    for (index, (positives, negatives)) in positive.windows(period).zip(negative.windows(period)).enumerate() {
        let slot = index + period - 1;
        // The first window straddles the direction-less first bar; skip it.
        if slot < period {
            continue;
        }
        let positive_flow: f64 = positives.iter().sum();
        let negative_flow: f64 = negatives.iter().sum();
        let value = if negative_flow > 0.0 {
            100.0 - 100.0 / (1.0 + positive_flow / negative_flow)
        } else if positive_flow > 0.0 {
            100.0
        } else {
            50.0
        };
        if let Some(cell) = out.get_mut(slot) {
            *cell = Some(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::mfi;

    #[test]
    fn mfi_saturates_high_on_a_steady_advance() {
        let price: Vec<f64> = (1..=20).map(f64::from).collect();
        let volume = [100.0; 20];
        let last = mfi(&price, &price, &price, &volume, 5)
            .into_iter()
            .flatten()
            .last()
            .expect("mfi");
        assert!((last - 100.0).abs() < 1e-9, "a pure advance should give MFI 100, got {last}");
    }

    #[test]
    fn mfi_stays_in_range() {
        let high = [2.0, 3.0, 2.5, 4.0, 3.5, 5.0, 4.5, 6.0];
        let low = [1.0, 1.5, 1.0, 2.0, 1.5, 3.0, 2.5, 4.0];
        let close = [1.5, 2.5, 2.0, 3.0, 2.5, 4.0, 3.5, 5.0];
        let volume = [100.0, 120.0, 90.0, 200.0, 80.0, 300.0, 110.0, 250.0];
        for value in mfi(&high, &low, &close, &volume, 4).into_iter().flatten() {
            assert!((0.0..=100.0).contains(&value), "MFI out of range: {value}");
        }
    }
}
