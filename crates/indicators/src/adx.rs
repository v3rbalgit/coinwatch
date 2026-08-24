//! Average Directional Index (trend strength) with its +DI / −DI lines.

/// ADX plus the directional indicators, each aligned index-for-index with the
/// input. `high`/`low`/`close` are expected to be equal length.
#[derive(Debug, Clone)]
pub struct Adx {
    /// Trend-strength line (0–100), Wilder-smoothed from DX. Warms up over ~2·period bars.
    pub adx: Vec<Option<f64>>,
    /// Positive directional indicator (+DI).
    pub plus_di: Vec<Option<f64>>,
    /// Negative directional indicator (−DI).
    pub minus_di: Vec<Option<f64>>,
}

/// Average Directional Index over `period` (Wilder). Directional movement and true
/// range are smoothed with Wilder's running sum; DX = 100·|+DI − −DI| / (+DI + −DI)
/// is then Wilder-averaged into ADX.
#[must_use]
pub fn adx(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Adx {
    let len = close.len();
    let mut result = Adx {
        adx: vec![None; len],
        plus_di: vec![None; len],
        minus_di: vec![None; len],
    };
    if period == 0 {
        return result;
    }

    // Per-bar +DM / −DM / TR for bars 1..len (vec index j corresponds to bar j+1).
    let mut trs: Vec<f64> = Vec::new();
    let mut plus_dm: Vec<f64> = Vec::new();
    let mut minus_dm: Vec<f64> = Vec::new();
    for (((prev_high, high), (prev_low, low)), prev_close) in high
        .iter()
        .zip(high.iter().skip(1))
        .zip(low.iter().zip(low.iter().skip(1)))
        .zip(close.iter())
    {
        let up = high - prev_high;
        let down = prev_low - low;
        plus_dm.push(if up > down && up > 0.0 { up } else { 0.0 });
        minus_dm.push(if down > up && down > 0.0 { down } else { 0.0 });
        trs.push((high - low).max((high - prev_close).abs()).max((low - prev_close).abs()));
    }
    if trs.len() < period {
        return result;
    }

    let period_f = period as f64;
    let mut smoothed_tr = trs.iter().take(period).sum::<f64>();
    let mut smoothed_plus = plus_dm.iter().take(period).sum::<f64>();
    let mut smoothed_minus = minus_dm.iter().take(period).sum::<f64>();

    // DX from bar `period` onward; the first smoothed value uses the seed sums.
    let mut dxs: Vec<f64> = Vec::new();
    for (offset, ((tr, plus), minus)) in trs.iter().zip(&plus_dm).zip(&minus_dm).enumerate().skip(period - 1) {
        if offset >= period {
            smoothed_tr = smoothed_tr - smoothed_tr / period_f + tr;
            smoothed_plus = smoothed_plus - smoothed_plus / period_f + plus;
            smoothed_minus = smoothed_minus - smoothed_minus / period_f + minus;
        }
        let bar = offset + 1;
        let plus_di = if smoothed_tr > 0.0 { 100.0 * smoothed_plus / smoothed_tr } else { 0.0 };
        let minus_di = if smoothed_tr > 0.0 { 100.0 * smoothed_minus / smoothed_tr } else { 0.0 };
        if let Some(cell) = result.plus_di.get_mut(bar) {
            *cell = Some(plus_di);
        }
        if let Some(cell) = result.minus_di.get_mut(bar) {
            *cell = Some(minus_di);
        }
        let di_sum = plus_di + minus_di;
        dxs.push(if di_sum > 0.0 { 100.0 * (plus_di - minus_di).abs() / di_sum } else { 0.0 });
    }

    // ADX = Wilder average of DX. First DX is at bar `period`, so the first ADX
    // (average of the first `period` DX values) lands at bar 2·period − 1.
    if dxs.len() >= period {
        let first_dx_bar = period;
        let mut adx = dxs.iter().take(period).sum::<f64>() / period_f;
        if let Some(cell) = result.adx.get_mut(first_dx_bar + period - 1) {
            *cell = Some(adx);
        }
        for (offset, dx) in dxs.iter().enumerate().skip(period) {
            adx = (adx * (period_f - 1.0) + dx) / period_f;
            if let Some(cell) = result.adx.get_mut(first_dx_bar + offset) {
                *cell = Some(adx);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::adx;

    #[test]
    fn plus_di_leads_in_an_uptrend_and_adx_stays_in_range() {
        let high: Vec<f64> = (1..=40).map(|n| f64::from(n) + 1.0).collect();
        let low: Vec<f64> = (1..=40).map(f64::from).collect();
        let close: Vec<f64> = (1..=40).map(|n| f64::from(n) + 0.5).collect();
        let out = adx(&high, &low, &close, 14);
        let plus = out.plus_di.iter().flatten().last().copied().expect("plus_di");
        let minus = out.minus_di.iter().flatten().last().copied().expect("minus_di");
        assert!(plus > minus, "uptrend should keep +DI above -DI ({plus} vs {minus})");
        for value in out.adx.iter().flatten() {
            assert!((0.0..=100.0).contains(value), "ADX out of range: {value}");
        }
    }

    #[test]
    fn adx_is_none_until_warmed_up() {
        let high: Vec<f64> = (1..=10).map(|n| f64::from(n) + 1.0).collect();
        let low: Vec<f64> = (1..=10).map(f64::from).collect();
        let close: Vec<f64> = (1..=10).map(|n| f64::from(n) + 0.5).collect();
        // Period 5 needs ~2·5 bars; 10 bars gives at most the very first ADX.
        let out = adx(&high, &low, &close, 5);
        assert!(out.adx.iter().take(8).all(Option::is_none));
    }
}
