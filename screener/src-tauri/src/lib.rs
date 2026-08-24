//! Hyperliquid market screener — Tauri backend.
//!
//! The market sync engine keeps a hot [`MarketStore`] cache warm from Hyperliquid;
//! commands read that cache and compute on demand (screener metrics, divergence
//! scans), so the UI never waits on the network. Live mids stream in over the
//! WebSocket and are relayed to the frontend as `mid` events.

use std::sync::Arc;

use domain::Timeframe;
use hl_feed::{
    ConnectionState, HyperliquidAdapter, MarketEvent, MarketStatus, MarketStore, SyncConfig, SyncPhase, spawn_sync,
};
use indicators::{DivergenceDirection, DivergenceKind};
use rust_decimal::prelude::ToPrimitive;
use scanner::{ScanConfig, Signal, SignalSource, scan_universe};
use serde::Serialize;
use tauri::Emitter;
use tokio::sync::broadcast::error::RecvError;

/// One screener row: live metadata from `metaAndAssetCtxs`. Reads memory only —
/// no network, no database — so it returns instantly.
#[derive(Serialize)]
struct ScreenerRow {
    coin: String,
    price: f64,
    prev_day: f64,
    volume_usd: f64,
    open_interest_usd: f64,
    funding: f64,
}

/// The market screener: every coin in the universe with its live metadata.
#[tauri::command]
fn screener_rows(store: tauri::State<'_, Arc<MarketStore>>) -> Vec<ScreenerRow> {
    store
        .universe()
        .iter()
        .map(|coin| {
            let open_interest_usd = match (coin.open_interest, coin.mark_px) {
                (Some(oi), Some(mark)) => oi * mark,
                _ => 0.0,
            };
            ScreenerRow {
                coin: coin.coin.clone(),
                price: coin.mark_px.unwrap_or(0.0),
                prev_day: coin.prev_day_px.unwrap_or(0.0),
                volume_usd: coin.day_volume_usd,
                open_interest_usd,
                funding: coin.funding.unwrap_or(0.0),
            }
        })
        .collect()
}

/// A live mid-price update forwarded to the frontend as a `mid` event.
#[derive(Clone, Serialize)]
struct MidPayload {
    coin: String,
    price: f64,
}

/// The market engine status forwarded to the frontend as a `status` event.
#[derive(Clone, Serialize)]
struct StatusPayload {
    connection: String,
    phase: String,
    universe: usize,
    backfill_done: usize,
    backfill_total: usize,
    backfill_errors: usize,
    streaming: usize,
    fetch_rate: u32,
}

impl From<&MarketStatus> for StatusPayload {
    fn from(status: &MarketStatus) -> Self {
        let connection = match status.connection {
            ConnectionState::Connecting => "connecting",
            ConnectionState::Live => "live",
            ConnectionState::Reconnecting => "reconnecting",
        };
        let phase = match status.phase {
            SyncPhase::LoadingUniverse => "loading",
            SyncPhase::Backfilling => "backfilling",
            SyncPhase::Live => "live",
        };
        Self {
            connection: connection.to_owned(),
            phase: phase.to_owned(),
            universe: status.universe,
            backfill_done: status.backfill_done,
            backfill_total: status.backfill_total,
            backfill_errors: status.backfill_errors,
            streaming: status.streaming,
            fetch_rate: status.fetch_rate,
        }
    }
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

/// Scan the cached universe for fresh divergences and stream them to the UI.
/// Pure CPU over the hot cache — returns in milliseconds.
#[tauri::command]
fn start_scan(app: tauri::AppHandle, store: tauri::State<'_, Arc<MarketStore>>) -> Result<(), String> {
    let signals = scan_universe(store.inner(), &ScanConfig::default());
    let _ = app.emit("scan:started", signals.len());
    for signal in &signals {
        let _ = app.emit("scan:signal", SignalPayload::from(signal));
    }
    let _ = app.emit("scan:done", signals.len());
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // rustls needs a process-wide crypto provider before the first TLS handshake
    // (the binary's job); the market feed and sync rely on it.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let store = MarketStore::new();
    let sync_store = Arc::clone(&store);
    let status_store = Arc::clone(&store);
    let adapter_store = Arc::clone(&store);

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(store)
        .setup(move |app| {
            // Keep the hot cache warm from Hyperliquid — the whole tradable universe.
            tauri::async_runtime::spawn(async move {
                let config =
                    SyncConfig { universe_limit: usize::MAX, min_day_volume_usd: 0.0, ..SyncConfig::default() };
                let _sync = spawn_sync(sync_store, config);
            });

            // Relay sync status (connecting / backfilling / live) to the UI.
            let status_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut status = status_store.watch_status();
                loop {
                    let payload = StatusPayload::from(&status.borrow_and_update().clone());
                    let _ = status_handle.emit("status", payload);
                    if status.changed().await.is_err() {
                        break;
                    }
                }
            });

            // Stream live mids and relay them to the UI as `mid` events.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut adapter =
                    HyperliquidAdapter::new(adapter_store, vec![Timeframe::Hour1, Timeframe::Hour4]);
                let mut events = adapter.feed().subscribe_all();
                adapter.connect();
                loop {
                    match events.recv().await {
                        Ok(event) => {
                            if let MarketEvent::Mid(mid) = &*event {
                                let payload = MidPayload {
                                    coin: mid.symbol.clone(),
                                    price: mid.price.to_f64().unwrap_or(0.0),
                                };
                                let _ = handle.emit("mid", payload);
                            }
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
