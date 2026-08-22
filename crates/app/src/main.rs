//! `coinwatch` — orchestration entry point.
//!
//! Phase-1 skeleton: this wires one instance of every seam into the intended
//! pipeline and prints the plan. It intentionally does **not** run a backtest
//! yet — the seam implementations return `Unimplemented`. Its job is to prove
//! the whole workspace compiles and links as one coherent DAG.

use rust_decimal::Decimal;

use cw_backtest::{Backtester, CostModel, WalkForward};
use cw_features::{FeatureModule, TechnicalFeatures};
use cw_providers::{DataSource, GeckoTerminal};
use cw_storage::DuckStore;
use cw_strategy::{CrossSectional, Strategy};

#[tokio::main]
async fn main() {
    let provider = GeckoTerminal::default();
    let _store = DuckStore::new("research/data/coinwatch.duckdb");
    let features = TechnicalFeatures::default();
    let strategy = CrossSectional {
        rank_feature: "mom".to_string(),
        top_quantile: 0.2,
        allow_short: false,
    };
    let backtester = Backtester::new(
        CostModel {
            fee_frac: Decimal::new(5, 4),   // 0.0005 = 5 bps
            gas_usd: Decimal::new(20, 2),   // $0.20
            slippage_coeff: Decimal::new(1, 1),
        },
        WalkForward { train_bars: 90, test_bars: 30, embargo_bars: 3 },
    );

    println!("coinwatch quant platform — Phase 1 skeleton");
    println!("pipeline: {provider} -> store -> {feat} -> {strat} -> backtest -> eval",
        provider = provider.name(),
        feat = features.name(),
        strat = strategy.name(),
    );
    println!(
        "backtest: walk-forward train={} test={} embargo={} bars",
        backtester.walk_forward.train_bars,
        backtester.walk_forward.test_bars,
        backtester.walk_forward.embargo_bars,
    );
    println!("status: seams wired; implementations pending (see DESIGN.md Phase 1).");
}
