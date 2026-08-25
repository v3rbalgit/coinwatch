//! Supertrend (an ATR-based trend follower / price-pane overlay).

use crate::atr::atr;

/// Supertrend aligned index-for-index with the input: the trailing stop line plus
/// whether each bar is in an uptrend (line below price). Defined once ATR is
/// (from index `period-1`). `high`/`low`/`close` are expected to be equal length.
#[derive(Debug, Clone)]
pub struct Supertrend {
    /// The trailing stop line (lower band in an uptrend, upper band in a downtrend).
    pub line: Vec<Option<f64>>,
    /// `true` while in an uptrend (line beneath price), `false` in a downtrend.
    pub uptrend: Vec<Option<bool>>,
}

/// Supertrend over `period` (ATR) scaled by `mult`. The final bands carry over
/// from the prior bar unless price breaks them; the trend flips when close crosses
/// the active band.
#[must_use]
pub fn supertrend(high: &[f64], low: &[f64], close: &[f64], period: usize, mult: f64) -> Supertrend {
    let len = close.len();
    let mut result = Supertrend {
        line: vec![None; len],
        uptrend: vec![None; len],
    };
    let atr = atr(high, low, close, period);

    let mut prev_upper = 0.0;
    let mut prev_lower = 0.0;
    let mut prev_close = 0.0;
    let mut prev_up = true;
    let mut initialized = false;
    for (index, (((high, low), close), atr)) in high.iter().zip(low).zip(close).zip(&atr).enumerate() {
        let Some(atr) = atr else {
            continue;
        };
        let mid = f64::midpoint(*high, *low);
        let basic_upper = mid + mult * atr;
        let basic_lower = mid - mult * atr;

        let (upper, lower, up) = if initialized {
            let upper = if basic_upper < prev_upper || prev_close > prev_upper {
                basic_upper
            } else {
                prev_upper
            };
            let lower = if basic_lower > prev_lower || prev_close < prev_lower {
                basic_lower
            } else {
                prev_lower
            };
            // Stay in trend until price breaks the active band, then flip.
            let up = if prev_up { *close >= lower } else { *close > upper };
            (upper, lower, up)
        } else {
            // Seed the bands; pick the opening trend from close vs the midpoint.
            (basic_upper, basic_lower, *close >= mid)
        };

        if let Some(cell) = result.line.get_mut(index) {
            *cell = Some(if up { lower } else { upper });
        }
        if let Some(cell) = result.uptrend.get_mut(index) {
            *cell = Some(up);
        }
        prev_upper = upper;
        prev_lower = lower;
        prev_close = *close;
        prev_up = up;
        initialized = true;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::supertrend;

    #[test]
    fn supertrend_tracks_below_price_in_an_uptrend() {
        let high: Vec<f64> = (1..=40).map(|n| f64::from(n) + 1.0).collect();
        let low: Vec<f64> = (1..=40).map(f64::from).collect();
        let close: Vec<f64> = (1..=40).map(|n| f64::from(n) + 0.5).collect();
        let out = supertrend(&high, &low, &close, 10, 3.0);
        let mut checked = false;
        for ((line, up), close) in out.line.iter().zip(&out.uptrend).zip(&close) {
            if let (Some(line), Some(true)) = (line, up) {
                assert!(line < close, "uptrend supertrend should sit below close ({line} vs {close})");
                checked = true;
            }
        }
        assert!(checked, "a steady rise should produce uptrend bars");
    }

    #[test]
    fn supertrend_is_none_until_atr_warms_up() {
        let high: Vec<f64> = (1..=12).map(|n| f64::from(n) + 1.0).collect();
        let low: Vec<f64> = (1..=12).map(f64::from).collect();
        let close: Vec<f64> = (1..=12).map(|n| f64::from(n) + 0.5).collect();
        let out = supertrend(&high, &low, &close, 10, 3.0);
        assert!(out.line.iter().take(9).all(Option::is_none));
        assert!(out.line.get(9).copied().flatten().is_some());
    }
}
