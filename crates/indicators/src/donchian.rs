//! Donchian Channel (a price-pane overlay): rolling highest-high / lowest-low.

/// Donchian Channel aligned index-for-index with the input: for each window of
/// `period` bars, the upper band is the highest high, the lower band the lowest
/// low, and the middle their average. `high`/`low` are expected to be equal length.
#[derive(Debug, Clone)]
pub struct Donchian {
    /// Highest high over the window.
    pub upper: Vec<Option<f64>>,
    /// Lowest low over the window.
    pub lower: Vec<Option<f64>>,
    /// Midpoint of the channel.
    pub middle: Vec<Option<f64>>,
}

/// Donchian Channel over `period` from `high`/`low` slices.
#[must_use]
pub fn donchian(high: &[f64], low: &[f64], period: usize) -> Donchian {
    let len = high.len();
    let mut channel = Donchian {
        upper: vec![None; len],
        lower: vec![None; len],
        middle: vec![None; len],
    };
    if period == 0 {
        return channel;
    }
    for (index, (highs, lows)) in high.windows(period).zip(low.windows(period)).enumerate() {
        let upper = highs.iter().copied().fold(f64::MIN, f64::max);
        let lower = lows.iter().copied().fold(f64::MAX, f64::min);
        let slot = index + period - 1;
        if let Some(cell) = channel.upper.get_mut(slot) {
            *cell = Some(upper);
        }
        if let Some(cell) = channel.lower.get_mut(slot) {
            *cell = Some(lower);
        }
        if let Some(cell) = channel.middle.get_mut(slot) {
            *cell = Some(f64::midpoint(upper, lower));
        }
    }
    channel
}

#[cfg(test)]
mod tests {
    use super::donchian;

    #[test]
    fn channel_tracks_window_extremes() {
        let high = [2.0, 4.0, 3.0, 6.0, 5.0];
        let low = [1.0, 2.0, 1.5, 3.0, 2.5];
        let channel = donchian(&high, &low, 3);
        // Window [4,3,6] -> upper 6, [2,1.5,3] -> lower 1.5, middle 3.75, at index 3.
        assert_eq!(channel.upper.get(3).copied().flatten(), Some(6.0));
        assert_eq!(channel.lower.get(3).copied().flatten(), Some(1.5));
        assert_eq!(channel.middle.get(3).copied().flatten(), Some(3.75));
    }

    #[test]
    fn none_until_the_period() {
        let channel = donchian(&[1.0, 2.0, 3.0], &[0.5, 1.0, 1.5], 3);
        assert!(channel.upper.iter().take(2).all(Option::is_none));
        assert!(channel.upper.get(2).copied().flatten().is_some());
    }
}
