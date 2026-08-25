//! Bollinger Bands (a price-pane overlay).

/// Bollinger Bands aligned index-for-index with `closes`: `None` until `period`
/// samples, then an SMA middle band with upper/lower bands `mult` standard
/// deviations away.
#[derive(Debug, Clone)]
pub struct Bollinger {
    /// Middle band (the SMA).
    pub middle: Vec<Option<f64>>,
    /// Upper band (middle + mult·σ).
    pub upper: Vec<Option<f64>>,
    /// Lower band (middle − mult·σ).
    pub lower: Vec<Option<f64>>,
}

/// Bollinger Bands over `closes` for `period` and `mult` standard deviations
/// (population σ over the same window as the middle SMA).
#[must_use]
pub fn bollinger(closes: &[f64], period: usize, mult: f64) -> Bollinger {
    let len = closes.len();
    let mut bands = Bollinger {
        middle: vec![None; len],
        upper: vec![None; len],
        lower: vec![None; len],
    };
    if period == 0 {
        return bands;
    }
    let period_f = period as f64;
    for (index, window) in closes.windows(period).enumerate() {
        let mean = window.iter().sum::<f64>() / period_f;
        let variance = window.iter().map(|value| (value - mean).powi(2)).sum::<f64>() / period_f;
        let deviation = mult * variance.sqrt();
        let slot = index + period - 1;
        if let Some(cell) = bands.middle.get_mut(slot) {
            *cell = Some(mean);
        }
        if let Some(cell) = bands.upper.get_mut(slot) {
            *cell = Some(mean + deviation);
        }
        if let Some(cell) = bands.lower.get_mut(slot) {
            *cell = Some(mean - deviation);
        }
    }
    bands
}

#[cfg(test)]
mod tests {
    use super::bollinger;

    #[test]
    fn bands_bracket_the_middle() {
        let closes: Vec<f64> = (1..=20).map(f64::from).collect();
        let bands = bollinger(&closes, 5, 2.0);
        for ((middle, upper), lower) in bands.middle.iter().zip(&bands.upper).zip(&bands.lower) {
            if let (Some(middle), Some(upper), Some(lower)) = (middle, upper, lower) {
                assert!(upper >= middle && middle >= lower, "bands must bracket the middle");
            }
        }
    }

    #[test]
    fn none_until_the_period_is_reached() {
        let bands = bollinger(&[1.0, 2.0, 3.0, 4.0], 3, 2.0);
        assert!(bands.middle.iter().take(2).all(Option::is_none));
        assert!(bands.middle.get(2).copied().flatten().is_some());
    }
}
