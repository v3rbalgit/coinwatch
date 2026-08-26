//! Hyperliquid market screener — Tauri backend.
//!
//! The market sync engine keeps a hot [`MarketStore`] cache warm from Hyperliquid.
//! Commands expose live metadata, candle history, user data, and personal signal
//! evaluation. Live mids stream over the WebSocket and are relayed to the frontend
//! as `mid` events.

mod indicator_presets;
mod signal_catalog;
mod signal_runs;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use domain::{Candle, Timeframe};
use hl_feed::{
    CandleHistory, ConnectionState, HyperliquidAdapter, HyperliquidRest, MarketEvent, MarketStatus, MarketStore,
    SyncConfig, SyncPhase, spawn_sync,
};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use storage::{CandleStore, ListView, SqliteStore};
use tauri::{Emitter, Manager};
use tokio::sync::broadcast::error::RecvError;

use crate::signal_runs::SignalRunState;

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
    connection: ConnectionState,
    phase: SyncPhase,
    universe: usize,
    backfill_done: usize,
    backfill_total: usize,
    backfill_errors: usize,
    streaming: usize,
    fetch_rate: u32,
}

impl From<&MarketStatus> for StatusPayload {
    fn from(status: &MarketStatus) -> Self {
        Self {
            connection: status.connection,
            phase: status.phase,
            universe: status.universe,
            backfill_done: status.backfill_done,
            backfill_total: status.backfill_total,
            backfill_errors: status.backfill_errors,
            streaming: status.streaming,
            fetch_rate: status.fetch_rate,
        }
    }
}

/// A candle in the shape lightweight-charts expects: epoch-second time, numeric OHLCV.
#[derive(Clone, Serialize)]
struct CandlePayload {
    time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl From<&Candle> for CandlePayload {
    fn from(candle: &Candle) -> Self {
        Self {
            time: candle.open_time.timestamp(),
            open: candle.open.to_f64().unwrap_or(0.0),
            high: candle.high.to_f64().unwrap_or(0.0),
            low: candle.low.to_f64().unwrap_or(0.0),
            close: candle.close.to_f64().unwrap_or(0.0),
            volume: candle.volume.to_f64().unwrap_or(0.0),
        }
    }
}

/// Cached candles for a (coin, timeframe), oldest first, for the detail chart.
#[tauri::command]
fn coin_candles(store: tauri::State<'_, Arc<MarketStore>>, coin: String, timeframe: String) -> Vec<CandlePayload> {
    let Some(timeframe) = Timeframe::from_hl(&timeframe) else {
        return Vec::new();
    };
    store
        .candles(&coin, timeframe)
        .map(|candles| candles.iter().map(CandlePayload::from).collect())
        .unwrap_or_default()
}

/// Older candles to load per scroll-back page — ~2x the chart's ~150-bar visible
/// window, so each page loads a couple screens of scrollback past the trigger.
const CHART_PAGE: usize = 300;

/// Candles just before `before_seconds`, oldest-first, for lazy history paging.
/// Serves from the accumulating DB cache; on a miss it fetches from Hyperliquid and
/// stores the result, so scroll-back depth accumulates.
#[tauri::command]
async fn coin_history(
    history: tauri::State<'_, Arc<CandleHistory>>,
    coin: String,
    timeframe: String,
    before_seconds: i64,
) -> Result<Vec<CandlePayload>, String> {
    let Some(timeframe) = Timeframe::from_hl(&timeframe) else {
        return Ok(Vec::new());
    };
    let before_ms = before_seconds
        .checked_mul(1000)
        .ok_or_else(|| "history boundary is outside the supported timestamp range".to_owned())?;
    let candles = history
        .load_before(&coin, timeframe, before_ms, CHART_PAGE)
        .await
        .map_err(|error| error.to_string())?;
    Ok(candles.iter().map(CandlePayload::from).collect())
}

/// Read a persisted UI setting (an opaque string), if set.
#[tauri::command]
async fn get_setting(store: tauri::State<'_, Arc<SqliteStore>>, key: String) -> Result<Option<String>, String> {
    store.get_setting(&key).await.map_err(|error| error.to_string())
}

/// Persist a UI setting (insert-or-replace).
#[tauri::command]
async fn set_setting(store: tauri::State<'_, Arc<SqliteStore>>, key: String, value: String) -> Result<(), String> {
    store.set_setting(&key, &value).await.map_err(|error| error.to_string())
}

/// A live candle update for the active coin, pushed to the detail chart.
#[derive(Clone, Serialize)]
struct CandleEventPayload {
    coin: String,
    timeframe: String,
    candle: CandlePayload,
}

/// Set (or clear) the coin whose live candles are streamed to the UI (the open detail page).
#[tauri::command]
fn set_active_coin(store: tauri::State<'_, Arc<MarketStore>>, coin: Option<String>) {
    store.set_active_coin(coin);
}

/// Open a coin's Hyperliquid trade page in the browser.
#[tauri::command]
fn open_on_hyperliquid(app: tauri::AppHandle, coin: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    app.opener()
        .open_url(format!("https://app.hyperliquid.xyz/trade/{coin}"), None::<&str>)
        .map_err(|error| error.to_string())
}

/// Every watchlist with its coins.
#[tauri::command]
async fn lists(store: tauri::State<'_, Arc<SqliteStore>>) -> Result<Vec<ListView>, String> {
    store.lists().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn create_list(store: tauri::State<'_, Arc<SqliteStore>>, name: String) -> Result<(), String> {
    store.create_list(&name).await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn delete_list(store: tauri::State<'_, Arc<SqliteStore>>, name: String) -> Result<(), String> {
    store.delete_list(&name).await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn add_to_list(store: tauri::State<'_, Arc<SqliteStore>>, list: String, coin: String) -> Result<(), String> {
    store.add_to_list(&list, &coin).await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn remove_from_list(store: tauri::State<'_, Arc<SqliteStore>>, list: String, coin: String) -> Result<(), String> {
    store.remove_from_list(&list, &coin).await.map_err(|error| error.to_string())
}

/// Every cached coin logo, keyed by coin.
#[tauri::command]
async fn logos(store: tauri::State<'_, Arc<SqliteStore>>) -> Result<HashMap<String, String>, String> {
    store
        .logos()
        .await
        .map(|rows| rows.into_iter().collect())
        .map_err(|error| error.to_string())
}

/// Fetch a coin's SVG logo from Hyperliquid; None when it has no real SVG.
async fn fetch_logo(client: &reqwest::Client, coin: &str) -> Option<String> {
    let response = client
        .get(format!("https://app.hyperliquid.xyz/coins/{coin}.svg"))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let is_svg = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| content_type.contains("svg"));
    if !is_svg {
        return None;
    }
    let body = response.text().await.ok()?;
    body.contains("<svg").then_some(body)
}

/// Download the universe's coin logos into the store, paced to spare the CDN.
/// Cached coins are skipped; a periodic re-scan picks up new listings.
async fn cache_logos(market: Arc<MarketStore>, db: Arc<SqliteStore>, client: reqwest::Client) {
    loop {
        let universe = market.universe();
        if universe.is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        let cached = db.logo_coins().await.unwrap_or_default();
        for coin in universe {
            if cached.contains(&coin.coin) {
                continue;
            }
            if let Some(svg) = fetch_logo(&client, &coin.coin).await
                && let Err(error) = db.cache_logo(&coin.coin, &svg).await
            {
                tracing::warn!(coin = %coin.coin, %error, "failed to cache logo");
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    // rustls needs a process-wide crypto provider before the first TLS handshake
    // (the binary's job); the market feed and sync rely on it.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let store = MarketStore::new();
    let sync_store = Arc::clone(&store);
    let status_store = Arc::clone(&store);
    let adapter_store = Arc::clone(&store);
    let logo_market_store = Arc::clone(&store);
    let sync_config = SyncConfig {
        universe_limit: usize::MAX,
        min_day_volume_usd: 0.0,
        ..SyncConfig::default()
    };
    let streamed_timeframes = sync_config.streamed_timeframes.clone();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(store)
        .manage(Arc::new(SignalRunState::default()))
        .setup(move |app| {
            // Client clones share one pool; Hyperliquid requests add the shared
            // weight limiter while logo downloads use the same HTTP transport directly.
            let http = reqwest::Client::new();
            let rest = Arc::new(HyperliquidRest::new(http.clone(), sync_config.weight_budget_per_min));

            // The local database holds candles + watchlists. Open it synchronously so
            // the list commands have their managed state before the UI can call them.
            let data_dir = app.path().app_data_dir().unwrap_or_else(|_| std::env::temp_dir());
            let _ = std::fs::create_dir_all(&data_dir);
            let db_path = data_dir.join("coinwatch.db");

            match tauri::async_runtime::block_on(SqliteStore::open(&db_path)) {
                Ok(store) => {
                    let store = Arc::new(store);
                    let persistence: Arc<dyn CandleStore> = store.clone();
                    let history = Arc::new(CandleHistory::new(Arc::clone(&persistence), Arc::clone(&rest)));
                    app.manage(Arc::clone(&store));
                    app.manage(history);
                    let logo_db = Arc::clone(&store);
                    // Keep the hot cache warm from Hyperliquid — the whole tradable universe.
                    tauri::async_runtime::spawn(async move {
                        let _sync = spawn_sync(sync_store, sync_config, persistence, rest);
                    });
                    // Cache coin logos locally so they load instantly and offline.
                    tauri::async_runtime::spawn(cache_logos(logo_market_store, logo_db, http));
                }
                Err(error) => tracing::error!(%error, "failed to open the local database; market sync disabled"),
            }

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
                let event_store = Arc::clone(&adapter_store);
                let mut adapter = HyperliquidAdapter::new(adapter_store, streamed_timeframes);
                let mut events = adapter.feed().subscribe_all();
                adapter.connect();
                loop {
                    match events.recv().await {
                        Ok(event) => match &*event {
                            MarketEvent::Mid(mid) => {
                                let payload = MidPayload {
                                    coin: mid.symbol.clone(),
                                    price: mid.price.to_f64().unwrap_or(0.0),
                                };
                                let _ = handle.emit("mid", payload);
                            }
                            MarketEvent::Candle(candle) => {
                                if event_store.active_coin().as_deref() == Some(candle.coin.as_str()) {
                                    let payload = CandleEventPayload {
                                        coin: candle.coin.clone(),
                                        timeframe: candle.timeframe.as_hl().to_owned(),
                                        candle: CandlePayload::from(&candle.candle),
                                    };
                                    let _ = handle.emit("candle", payload);
                                }
                            }
                        },
                        Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => break,
                    }
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            screener_rows,
            open_on_hyperliquid,
            lists,
            create_list,
            delete_list,
            add_to_list,
            remove_from_list,
            logos,
            coin_candles,
            coin_history,
            get_setting,
            set_setting,
            set_active_coin,
            signal_catalog::list_signals,
            signal_catalog::get_signal,
            signal_catalog::create_signal,
            signal_catalog::update_signal,
            signal_catalog::delete_signal,
            indicator_presets::list_indicator_presets,
            indicator_presets::create_indicator_preset,
            indicator_presets::update_indicator_preset,
            indicator_presets::delete_indicator_preset,
            signal_runs::start_signal_run,
            signal_runs::cancel_signal_run
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
