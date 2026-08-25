//! On-Balance Volume (a cumulative volume-flow measure).

/// On-Balance Volume aligned index-for-index with the input: a running total that
/// adds the bar's volume when the close rises, subtracts it when the close falls,
/// and holds it flat when unchanged. Starts at zero on the first bar.
/// `close`/`volume` are expected to be equal length.
#[must_use]
pub fn obv(close: &[f64], volume: &[f64]) -> Vec<Option<f64>> {
    let mut out = vec![None; close.len()];
    let mut running = 0.0;
    let mut prev_close: Option<f64> = None;
    for (index, (close, volume)) in close.iter().zip(volume).enumerate() {
        if let Some(prev) = prev_close {
            if *close > prev {
                running += volume;
            } else if *close < prev {
                running -= volume;
            }
        }
        if let Some(slot) = out.get_mut(index) {
            *slot = Some(running);
        }
        prev_close = Some(*close);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::obv;

    #[test]
    fn accumulates_on_up_bars_and_bleeds_on_down_bars() {
        let close = [10.0, 11.0, 10.5, 10.5, 12.0];
        let volume = [100.0, 200.0, 150.0, 80.0, 300.0];
        let out = obv(&close, &volume);
        // 0, +200, -150, +0, +300 -> [0, 200, 50, 50, 350]
        assert_eq!(out.first().copied().flatten(), Some(0.0));
        assert_eq!(out.get(1).copied().flatten(), Some(200.0));
        assert_eq!(out.get(2).copied().flatten(), Some(50.0));
        assert_eq!(out.get(3).copied().flatten(), Some(50.0));
        assert_eq!(out.get(4).copied().flatten(), Some(350.0));
    }
}
