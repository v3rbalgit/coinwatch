//! Parabolic SAR (stop-and-reverse trailing stop / price-pane overlay).

/// Parabolic SAR aligned index-for-index with the input, starting at the second
/// bar. `step` is the acceleration increment (classically 0.02) and `max_af` its
/// cap (classically 0.2). `high`/`low` are expected to be equal length.
///
/// Standard Wilder recursion: `SAR += AF·(EP − SAR)`, clamped so it never
/// penetrates the prior two bars' extremes, reversing when price crosses it.
#[must_use]
pub fn psar(high: &[f64], low: &[f64], step: f64, max_af: f64) -> Vec<Option<f64>> {
    let len = high.len();
    let mut out = vec![None; len];
    let (Some(&first_high), Some(&second_high), Some(&first_low), Some(&second_low)) =
        (high.first(), high.get(1), low.first(), low.get(1))
    else {
        return out;
    };

    // Seed trend, extreme point, and SAR from the first two bars.
    let mut uptrend = second_high >= first_high;
    let mut extreme = if uptrend {
        first_high.max(second_high)
    } else {
        first_low.min(second_low)
    };
    let mut sar = if uptrend {
        first_low.min(second_low)
    } else {
        first_high.max(second_high)
    };
    let mut af = step;
    if let Some(cell) = out.get_mut(1) {
        *cell = Some(sar);
    }

    let mut prev_high = second_high;
    let mut prev_low = second_low;
    let mut prev2_high = first_high;
    let mut prev2_low = first_low;

    for (index, (current_high, current_low)) in high.iter().zip(low).enumerate().skip(2) {
        let high = *current_high;
        let low = *current_low;
        let mut next_sar = sar + af * (extreme - sar);
        // SAR may not penetrate the prior two bars' extremes.
        next_sar = if uptrend {
            next_sar.min(prev_low).min(prev2_low)
        } else {
            next_sar.max(prev_high).max(prev2_high)
        };

        if uptrend {
            if low < next_sar {
                // Reverse to a downtrend: SAR jumps to the prior extreme.
                uptrend = false;
                next_sar = extreme;
                extreme = low;
                af = step;
            } else if high > extreme {
                extreme = high;
                af = (af + step).min(max_af);
            }
        } else if high > next_sar {
            // Reverse to an uptrend.
            uptrend = true;
            next_sar = extreme;
            extreme = high;
            af = step;
        } else if low < extreme {
            extreme = low;
            af = (af + step).min(max_af);
        }

        sar = next_sar;
        if let Some(cell) = out.get_mut(index) {
            *cell = Some(sar);
        }
        prev2_high = prev_high;
        prev2_low = prev_low;
        prev_high = high;
        prev_low = low;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::psar;

    #[test]
    fn psar_trails_at_or_below_price_in_a_rise() {
        let high: Vec<f64> = (1..=30).map(|n| f64::from(n) + 1.0).collect();
        let low: Vec<f64> = (1..=30).map(f64::from).collect();
        let out = psar(&high, &low, 0.02, 0.2);
        let mut checked = false;
        for (sar, low) in out.iter().zip(&low).skip(5) {
            if let Some(sar) = sar {
                assert!(*sar <= *low, "a rising PSAR should trail at/below the low ({sar} vs {low})");
                checked = true;
            }
        }
        assert!(checked, "expected SAR values in a steady rise");
    }

    #[test]
    fn psar_starts_at_the_second_bar() {
        let out = psar(&[2.0, 3.0, 4.0], &[1.0, 2.0, 3.0], 0.02, 0.2);
        assert!(out.first().copied().flatten().is_none());
        assert!(out.get(1).copied().flatten().is_some());
    }
}
