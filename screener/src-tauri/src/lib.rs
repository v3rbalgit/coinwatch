//! Hyperliquid market screener — Tauri backend.
//!
//! Reuses the coinwatch engine crates (`cw-storage`, `cw-features`, `cw-providers`)
//! as a library: reads the ingested HL market data, computes indicators, and exposes
//! them to the Svelte frontend as Tauri commands.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{Duration, Utc};
use cw_core::{Bar, FundingPoint, Instrument, Interval};
use cw_features::{FeatureContext, FeatureModule, FeatureRow, FundingFeatures, TechnicalFeatures, combine};
use cw_storage::{DuckStore, Store, TimeRange};
use hl_feed::{HyperliquidAdapter, MarketEvent, fetch_universe};
use indicators::{DivergenceDirection, DivergenceKind};
use rust_decimal::prelude::ToPrimitive;
use scanner::{ScanConfig, Signal, SignalSource, scan_coin};
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
    rsi: f64,
    funding_rate: f64,
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
            rsi: fr.map_or(0.0, |r| fval(r, "rsi")),
            funding_rate: fr.map_or(0.0, |r| fval(r, "fund_rate")),
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

/// A divergence setup forwarded to the frontend as a `scan:signal` event.
#[derive(Clone, Serialize)]
struct SignalPayload {
    coin: String,
    timeframe: String,
    source: String,
    kind: String,
    direction: String,
    at: String,
}

impl From<&Signal> for SignalPayload {
    fn from(signal: &Signal) -> Self {
        Self {
            coin: signal.coin.clone(),
            timeframe: signal.timeframe.as_hl().to_owned(),
            source: match signal.source {
                SignalSource::Rsi => "RSI",
                SignalSource::Macd => "MACD",
            }
            .to_owned(),
            kind: match signal.kind {
                DivergenceKind::Regular => "Regular",
                DivergenceKind::Hidden => "Hidden",
            }
            .to_owned(),
            direction: match signal.direction {
                DivergenceDirection::Bullish => "Bullish",
                DivergenceDirection::Bearish => "Bearish",
            }
            .to_owned(),
            at: signal.at.to_rfc3339(),
        }
    }
}

/// A coin whose scan failed, forwarded as a `scan:failure` event.
#[derive(Clone, Serialize)]
struct FailurePayload {
    coin: String,
    error: String,
}

/// Guards against overlapping scans.
struct ScanRunning(Arc<AtomicBool>);

/// Kick off a background universe scan; signals stream to the UI as events.
#[tauri::command]
fn start_scan(app: tauri::AppHandle, state: tauri::State<'_, ScanRunning>, limit: usize) -> Result<(), String> {
    let running = Arc::clone(&state.0);
    if running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return Err("a scan is already running".to_owned());
    }
    tauri::async_runtime::spawn(async move {
        run_scan(&app, limit).await;
        running.store(false, Ordering::SeqCst);
    });
    Ok(())
}

/// Fetch the top `limit` coins by volume and scan each for fresh RSI/MACD
/// divergences, emitting `scan:*` events as it progresses.
async fn run_scan(app: &tauri::AppHandle, limit: usize) {
    let client = reqwest::Client::new();
    let config = ScanConfig::default();

    let coins = match fetch_universe(&client, limit, 1_000_000.0).await {
        Ok(coins) => coins,
        Err(error) => {
            let _ = app.emit("scan:error", error.to_string());
            return;
        }
    };

    let total = coins.len();
    let _ = app.emit("scan:started", total);
    for (index, coin) in coins.iter().enumerate() {
        match scan_coin(&client, &coin.coin, &config).await {
            Ok(signals) => {
                for signal in &signals {
                    let _ = app.emit("scan:signal", SignalPayload::from(signal));
                }
            }
            Err(error) => {
                let _ = app.emit(
                    "scan:failure",
                    FailurePayload { coin: coin.coin.clone(), error: error.to_string() },
                );
            }
        }
        let _ = app.emit("scan:progress", index + 1);
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    let _ = app.emit("scan:done", total);
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // rustls needs a process-wide crypto provider before the first TLS handshake
    // (the binary's job, per convention); hl-feed's WebSocket relies on it.
    let _ = rustls::crypto::ring::default_provider().install_default();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(ScanRunning(Arc::new(AtomicBool::new(false))))
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
        .invoke_handler(tauri::generate_handler![screener_rows, start_scan])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
