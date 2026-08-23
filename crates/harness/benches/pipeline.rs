//! Micro-benchmark for the walk-forward sweep — the research hot path.
#![expect(missing_docs, reason = "criterion_group! generates an undocumented pub fn")]

use std::hint::black_box;

use chrono::{TimeZone, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use cw_backtest::CostModel;
use cw_core::{Bar, Decimal, FundingPoint, Instrument, Network, PoolId};
use cw_features::{FeatureContext, FeatureModule, FundingFeatures, LiquidityFeatures, TechnicalFeatures, combine};
use cw_harness::{WfConfig, sweep};
use cw_strategy::CrossSectional;
use rust_decimal::prelude::FromPrimitive;

type Dataset = Vec<(Instrument, Vec<Bar>)>;
type Funding = Vec<(Instrument, Vec<FundingPoint>)>;

fn dec(x: f64) -> Decimal {
    Decimal::from_f64(x).unwrap_or_default()
}

/// Deterministic LCG in `[0, 1)` so the benchmark data is reproducible.
fn next(state: &mut u64) -> f64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    (*state >> 33) as f64 / 2_147_483_648.0
}

fn synth(n_coins: usize, n_bars: usize) -> (Dataset, Funding) {
    let day = |d: i64| Utc.timestamp_opt(1_600_000_000 + d * 86_400, 0).single().unwrap_or_default();
    let mut dataset = Dataset::new();
    let mut funding = Funding::new();
    for c in 0..n_coins {
        let inst = Instrument {
            network: Network::Other("hl".into()),
            pool: PoolId(format!("C{c}")),
            name: format!("C{c}"),
            token: None,
        };
        let mut state = 0x1234_5678_u64.wrapping_add(c as u64 * 0x9E37_79B9);
        let mut price = 100.0_f64;
        let mut bars = Vec::with_capacity(n_bars);
        let mut fps = Vec::with_capacity(n_bars);
        for d in 0..n_bars {
            price *= 1.0 + (next(&mut state) - 0.5) * 0.06;
            let volume = 1_000_000.0 * (0.5 + next(&mut state));
            bars.push(Bar {
                ts: day(d as i64),
                open: dec(price),
                high: dec(price * 1.01),
                low: dec(price * 0.99),
                close: dec(price),
                volume: dec(volume),
            });
            fps.push(FundingPoint {
                time: day(d as i64),
                funding_rate: dec((next(&mut state) - 0.5) * 0.001),
                premium: Decimal::ZERO,
            });
        }
        dataset.push((inst.clone(), bars));
        funding.push((inst, fps));
    }
    (dataset, funding)
}

fn bench_sweep(c: &mut Criterion) {
    let (dataset, funding) = synth(24, 1200);
    let ctx = FeatureContext {
        bars: &dataset,
        funding: &funding,
    };
    let frame = combine(&[
        TechnicalFeatures::default().compute(&ctx).unwrap_or_default(),
        LiquidityFeatures::default().compute(&ctx).unwrap_or_default(),
        FundingFeatures::default().compute(&ctx).unwrap_or_default(),
    ]);
    let costs = CostModel {
        fee_frac: Decimal::new(5, 4),
        slippage_coeff: Decimal::new(5, 1),
        gas_usd: Decimal::new(50, 2),
    };
    let mk = |feature: &str, invert: bool| CrossSectional {
        rank_feature: feature.to_string(),
        top_quantile: 0.3,
        allow_short: true,
        invert,
        weight_feature: None,
    };
    let candidates = vec![
        ("illiq L/S".to_string(), mk("illiq", false)),
        ("vol L/S".to_string(), mk("vol", true)),
        ("fundz L/S".to_string(), mk("fund_z", true)),
    ];
    let cfg = WfConfig {
        train: 90,
        test: 30,
        embargo: 3,
        vol_target: None,
        rebalance_every: 14,
    };

    c.bench_function("sweep_24x1200_3cfg", |b| {
        b.iter(|| black_box(sweep(&candidates, &dataset, &frame, cfg, &costs, &funding).map(|r| r.len())));
    });
}

criterion_group!(benches, bench_sweep);
criterion_main!(benches);
