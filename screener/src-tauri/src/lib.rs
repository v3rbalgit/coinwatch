//! Hyperliquid market screener — Tauri backend.
//!
//! Reuses the coinwatch engine crates (`cw-storage`, `cw-features`, `cw-providers`)
//! as a library: reads the ingested HL market data, computes indicators, and exposes
//! them to the Svelte frontend as Tauri commands.

use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use cw_core::{Bar, FundingPoint, Instrument, Interval};
use cw_features::{FeatureContext, FeatureModule, FeatureRow, FundingFeatures, LiquidityFeatures, TechnicalFeatures, combine};
use cw_storage::{DuckStore, Store, TimeRange};
use hl_feed::{HyperliquidAdapter, MarketEvent};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use tauri::Emitter;
use tokio::sync::broadcast::error::RecvError;

/// DuckDB store path, resolved to the repo's data dir at compile time (dev). A
/// shipped build will move this to a per-user app-data location.
const DB_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../research/data/coinwatch.duckdb");

/// Minimum daily bars a coin needs before its indicators are meaningful.
const MIN_BARS: usize = 60;

/// One screener row: latest price/liquidity plus the newest computed indicators.
#[derive(Serialize)]
struct ScreenerRow {
    coin: String,
    price: f64,
    change_24h_pct: f64,
    volume_usd: f64,
    momentum: f64,
    rsi: f64,
    vol: f64,
    illiq: f64,
    funding_rate: f64,
    funding_z: f64,
}

fn wide_range() -> TimeRange {
    TimeRange {
        from: Utc::now() - Duration::days(1500),
        to: Utc::now() + Duration::days(1),
    }
}

/// Read a feature value as f64, defaulting to 0.0 when absent.
fn fval(row: &FeatureRow, key: &str) -> f64 {
    row.values.get(key).and_then(ToPrimitive::to_f64).unwrap_or(0.0)
}

/// The market screener: every ingested HL perp with its latest indicators.
#[tauri::command]
fn screener_rows() -> Result<Vec<ScreenerRow>, String> {
    let store = DuckStore::open(DB_PATH).map_err(|e| e.to_string())?;
    let instruments = store.list_instruments().map_err(|e| e.to_string())?;

    let mut dataset: Vec<(Instrument, Vec<Bar>)> = Vec::new();
    let mut funding: Vec<(Instrument, Vec<FundingPoint>)> = Vec::new();
    for inst in &instruments {
        let bars = store.load_bars(inst, Interval::Day, wide_range()).unwrap_or_default();
        if bars.len() < MIN_BARS {
            continue;
        }
        let f = store.load_funding(&inst.network, &inst.pool.0, wide_range()).unwrap_or_default();
        if !f.is_empty() {
            funding.push((inst.clone(), f));
        }
        dataset.push((inst.clone(), bars));
    }

    let ctx = FeatureContext { bars: &dataset, funding: &funding };
    let frame = combine(&[
        TechnicalFeatures::default().compute(&ctx).unwrap_or_default(),
        LiquidityFeatures::default().compute(&ctx).unwrap_or_default(),
        FundingFeatures::default().compute(&ctx).unwrap_or_default(),
    ]);

    // Newest feature row per coin (by pool id).
    let mut latest: BTreeMap<String, &FeatureRow> = BTreeMap::new();
    for row in &frame.rows {
        latest
            .entry(row.instrument.pool.0.clone())
            .and_modify(|cur| {
                if row.ts > cur.ts {
                    *cur = row;
                }
            })
            .or_insert(row);
    }

    let mut rows = Vec::with_capacity(dataset.len());
    for (inst, bars) in &dataset {
        let Some(last) = bars.last() else { continue };
        let price = last.close.to_f64().unwrap_or(0.0);
        let prev = bars.get(bars.len().saturating_sub(2)).map_or(price, |b| b.close.to_f64().unwrap_or(price));
        let change_24h_pct = if prev > 0.0 { (price / prev - 1.0) * 100.0 } else { 0.0 };
        let fr = latest.get(&inst.pool.0);
        rows.push(ScreenerRow {
            coin: inst.name.clone(),
            price,
            change_24h_pct,
            volume_usd: last.volume.to_f64().unwrap_or(0.0),
            momentum: fr.map_or(0.0, |r| fval(r, "mom")),
            rsi: fr.map_or(0.0, |r| fval(r, "rsi")),
            vol: fr.map_or(0.0, |r| fval(r, "vol")),
            illiq: fr.map_or(0.0, |r| fval(r, "illiq")),
            funding_rate: fr.map_or(0.0, |r| fval(r, "fund_rate")),
            funding_z: fr.map_or(0.0, |r| fval(r, "fund_z")),
        });
    }
    Ok(rows)
}

/// A live mid-price update forwarded to the frontend as a `mid` event.
#[derive(Clone, Serialize)]
struct MidPayload {
    coin: String,
    price: f64,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // rustls needs a process-wide crypto provider before the first TLS handshake
    // (the binary's job, per convention); hl-feed's WebSocket relies on it.
    let _ = rustls::crypto::ring::default_provider().install_default();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            // Stream live Hyperliquid mids and relay them to the UI as `mid` events.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut adapter = HyperliquidAdapter::new();
                let mut events = adapter.feed().subscribe_all();
                adapter.connect();
                loop {
                    match events.recv().await {
                        Ok(event) => {
                            let MarketEvent::Mid(mid) = &*event;
                            let payload = MidPayload {
                                coin: mid.symbol.clone(),
                                price: mid.price.to_f64().unwrap_or(0.0),
                            };
                            let _ = handle.emit("mid", payload);
                        }
                        Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => break,
                    }
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![screener_rows])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
