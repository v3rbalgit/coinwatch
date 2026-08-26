//! Divergence detection between price and an oscillator (RSI, MACD, ...).

use serde::Serialize;
use strum::Display;

use crate::pivot::{Pivot, PivotKind, pivots};

/// Regular (reversal) versus hidden (continuation) divergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Display)]
pub enum DivergenceKind {
    /// Reversal: price extends a swing but the oscillator fails to confirm it.
    Regular,
    /// Continuation: a counter-trend pullback the oscillator over/undershoots.
    Hidden,
}

/// Which way the divergence points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Display)]
pub enum DivergenceDirection {
    /// Anchored on swing lows; expects upside.
    Bullish,
    /// Anchored on swing highs; expects downside.
    Bearish,
}

/// A divergence spanning two consecutive same-kind pivots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Divergence {
    /// Reversal or continuation.
    pub(crate) kind: DivergenceKind,
    /// Bullish or bearish.
    pub(crate) direction: DivergenceDirection,
    /// Index of the earlier pivot.
    pub(crate) prior_index: usize,
    /// Index of the later pivot.
    pub(crate) recent_index: usize,
}

fn divergence_for_pair(
    prior: &Pivot,
    recent: &Pivot,
    prior_oscillator: f64,
    recent_oscillator: f64,
) -> Option<Divergence> {
    use DivergenceDirection::{Bearish, Bullish};
    use DivergenceKind::{Hidden, Regular};

    let (kind, direction) = match prior.kind {
        PivotKind::Low if recent.value < prior.value && recent_oscillator > prior_oscillator => (Regular, Bullish),
        PivotKind::Low if recent.value > prior.value && recent_oscillator < prior_oscillator => (Hidden, Bullish),
        PivotKind::High if recent.value > prior.value && recent_oscillator < prior_oscillator => (Regular, Bearish),
        PivotKind::High if recent.value < prior.value && recent_oscillator > prior_oscillator => (Hidden, Bearish),
        PivotKind::Low | PivotKind::High => return None,
    };
    Some(Divergence {
        kind,
        direction,
        prior_index: prior.index,
        recent_index: recent.index,
    })
}

/// Detect divergences between `price` and an aligned oscillator. Price swing
/// pivots — found with `left`/`right` bars of
/// confirmation — are compared against the oscillator's value at the same bars;
/// consecutive same-kind pivots more than `max_gap` bars apart are ignored.
///
/// - Regular bullish: price lower low, oscillator higher low.
/// - Hidden bullish: price higher low, oscillator lower low.
/// - Regular bearish: price higher high, oscillator lower high.
/// - Hidden bearish: price lower high, oscillator higher high.
#[must_use]
pub(crate) fn detect_divergences(
    price: &[f64],
    oscillator: &[Option<f64>],
    left: usize,
    right: usize,
    max_gap: usize,
) -> Vec<Divergence> {
    let all = pivots(price, left, right);
    let lows: Vec<&_> = all.iter().filter(|pivot| pivot.kind == PivotKind::Low).collect();
    let highs: Vec<&_> = all.iter().filter(|pivot| pivot.kind == PivotKind::High).collect();

    lows.windows(2)
        .chain(highs.windows(2))
        .filter_map(|pair| {
            let [prior, recent] = pair else { return None };
            if recent.index.saturating_sub(prior.index) > max_gap {
                return None;
            }
            let (&Some(prior_oscillator), &Some(recent_oscillator)) =
                (oscillator.get(prior.index)?, oscillator.get(recent.index)?)
            else {
                return None;
            };
            divergence_for_pair(prior, recent, prior_oscillator, recent_oscillator)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{DivergenceDirection, DivergenceKind, detect_divergences};

    // Price makes a lower low at index 2 (6.0) then index 6 (5.0).
    const PRICE: [f64; 9] = [10.0, 8.0, 6.0, 8.0, 10.0, 7.0, 5.0, 7.0, 9.0];

    #[test]
    fn detects_regular_bullish() {
        // Oscillator makes a HIGHER low (30 -> 40) while price makes a lower low.
        let osc: Vec<Option<f64>> = vec![
            Some(50.0),
            Some(50.0),
            Some(30.0),
            Some(50.0),
            Some(50.0),
            Some(50.0),
            Some(40.0),
            Some(50.0),
            Some(50.0),
        ];
        let divs = detect_divergences(&PRICE, &osc, 2, 2, 100);
        assert!(divs.iter().any(|d| d.kind == DivergenceKind::Regular
            && d.direction == DivergenceDirection::Bullish
            && d.prior_index == 2
            && d.recent_index == 6));
    }

    #[test]
    fn confirmed_downtrend_is_not_a_divergence() {
        // Oscillator ALSO makes a lower low (30 -> 20): price and oscillator agree.
        let osc: Vec<Option<f64>> = vec![
            Some(50.0),
            Some(50.0),
            Some(30.0),
            Some(50.0),
            Some(50.0),
            Some(50.0),
            Some(20.0),
            Some(50.0),
            Some(50.0),
        ];
        let divs = detect_divergences(&PRICE, &osc, 2, 2, 100);
        assert!(!divs.iter().any(|d| d.direction == DivergenceDirection::Bullish));
    }

    #[test]
    fn missing_oscillator_values_are_skipped() {
        let osc: Vec<Option<f64>> = vec![None; PRICE.len()];
        assert!(detect_divergences(&PRICE, &osc, 2, 2, 100).is_empty());
    }
}
