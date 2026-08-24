//! MACD (moving-average convergence/divergence) and its EMA building block.

/// Exponential moving average of `values` for `period`, seeded with the simple
/// mean of the first `period` values. Returns one entry per input: `None` until
/// index `period - 1`, then the EMA.
#[must_use]
pub fn ema(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out: Vec<Option<f64>> = vec![None; values.len()];
    if period == 0 || values.len() < period {
        return out;
    }

    #[expect(clippy::cast_precision_loss, reason = "indicator periods are tiny, well within f64")]
    let period_f = period as f64;
    let alpha = 2.0 / (period_f + 1.0);

    let seed = values.iter().take(period).sum::<f64>() / period_f;
    let mut prev = seed;
    if let Some(slot) = out.get_mut(period - 1) {
        *slot = Some(seed);
    }
    for (index, value) in values.iter().enumerate().skip(period) {
        prev = alpha * value + (1.0 - alpha) * prev;
        if let Some(slot) = out.get_mut(index) {
            *slot = Some(prev);
        }
    }
    out
}

/// MACD output: the MACD line, its signal line, and the histogram, each aligned
/// index-for-index with the input closes (`None` where undefined).
#[derive(Debug, Clone)]
pub struct Macd {
    /// Fast EMA minus slow EMA.
    pub macd: Vec<Option<f64>>,
    /// EMA of the MACD line.
    pub signal: Vec<Option<f64>>,
    /// MACD line minus signal line.
    pub histogram: Vec<Option<f64>>,
}

/// Compute MACD over `closes` with the given `fast`/`slow`/`signal` periods
/// (classically 12/26/9). The MACD line feeds
/// [`detect_divergences`](crate::detect_divergences) as an oscillator, exactly
/// like RSI.
#[must_use]
pub fn macd(closes: &[f64], fast: usize, slow: usize, signal: usize) -> Macd {
    let fast_ema = ema(closes, fast);
    let slow_ema = ema(closes, slow);

    let macd_line: Vec<Option<f64>> = fast_ema
        .iter()
        .zip(slow_ema.iter())
        .map(|pair| match pair {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        })
        .collect();

    // The MACD line is defined contiguously from where the slow EMA begins; run
    // the signal EMA over that dense slice, then re-align it to full length.
    let dense: Vec<f64> = macd_line.iter().flatten().copied().collect();
    let start = macd_line.iter().position(Option::is_some).unwrap_or(macd_line.len());
    let signal_dense = ema(&dense, signal);

    let mut signal_line: Vec<Option<f64>> = vec![None; closes.len()];
    for (offset, value) in signal_dense.into_iter().enumerate() {
        if let Some(slot) = signal_line.get_mut(start + offset) {
            *slot = value;
        }
    }

    let histogram: Vec<Option<f64>> = macd_line
        .iter()
        .zip(signal_line.iter())
        .map(|pair| match pair {
            (Some(m), Some(s)) => Some(m - s),
            _ => None,
        })
        .collect();

    Macd {
        macd: macd_line,
        signal: signal_line,
        histogram,
    }
}

#[cfg(test)]
mod tests {
    use super::{ema, macd};

    #[test]
    fn ema_of_constant_is_constant() {
        for value in ema(&[5.0; 20], 10).into_iter().flatten() {
            assert!((value - 5.0).abs() < 1e-9);
        }
    }

    #[test]
    fn ema_is_none_until_seeded() {
        let out = ema(&[1.0, 2.0, 3.0, 4.0], 3);
        assert!(out.iter().take(2).all(Option::is_none));
        assert!(out.get(2).copied().flatten().is_some());
    }

    #[test]
    fn macd_line_is_positive_in_an_uptrend() {
        let closes: Vec<f64> = (1..=60).map(f64::from).collect();
        let result = macd(&closes, 12, 26, 9);
        assert_eq!(result.macd.len(), closes.len());
        let last = result.macd.iter().flatten().last().copied().expect("macd defined");
        assert!(last > 0.0, "a steady rise should lift the MACD line above zero: {last}");
    }

    #[test]
    fn signal_and_histogram_align_with_input() {
        let closes: Vec<f64> = (1..=60).map(f64::from).collect();
        let result = macd(&closes, 12, 26, 9);
        assert_eq!(result.signal.len(), closes.len());
        assert_eq!(result.histogram.len(), closes.len());
    }
}
