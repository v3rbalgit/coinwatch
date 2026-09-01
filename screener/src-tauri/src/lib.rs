//! Hyperliquid market screener — Tauri backend.
//!
//! The market sync engine keeps a hot [`MarketStore`] cache warm from Hyperliquid.
//! Commands expose live metadata, candle history, user data, and personal signal
//! evaluation. Live marks, active-series candles, candle snapshot invalidations,
//! and engine status are relayed to the frontend as typed events.

mod indicator_presets;
mod naming;
mod signal_catalog;
mod signal_runs;
mod signal_validation;
mod task_lifecycle;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use domain::{Candle, Timeframe};
use hl_feed::{
    ActiveSeries, CandleHealth, CandleHistory, CandleProvider, HyperliquidAdapter, HyperliquidRest, MarketEvent,
    MarketStatus, MarketStore, StorageMode, SyncConfig, spawn_sync,
};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use storage::{CandleStore, ListView, SqliteStore};
use tauri::{Emitter, Manager};
use tokio::sync::broadcast::error::RecvError;

use crate::signal_runs::SignalRunState;
use crate::signal_validation::SignalValidationState;

/// Current engine status for frontend initialization before subsequent events arrive.
#[tauri::command]
fn market_status(store: tauri::State<'_, Arc<MarketStore>>) -> MarketStatus {
    store.status()
}

/// One screener row: live metadata from `metaAndAssetCtxs`. Reads memory only —
/// no network, no database — so it returns instantly.
#[derive(Serialize)]
struct ScreenerRow {
    coin: String,
    price: f64,
    price_decimals: u32,
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
                price_decimals: coin.price_decimals,
                prev_day: coin.prev_day_px.unwrap_or(0.0),
                volume_usd: coin.day_volume_usd,
                open_interest_usd,
                funding: coin.funding.unwrap_or(0.0),
            }
        })
        .collect()
}

/// A live mark-price update forwarded to the frontend as a `mark-price` event.
#[derive(Clone, Serialize)]
struct MarkPricePayload {
    coin: String,
    price: f64,
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

/// Cached chart candles together with their authoritative trust state.
#[derive(Clone, Serialize)]
struct CandleSnapshotPayload {
    candles: Vec<CandlePayload>,
    health: CandleHealth,
    revision: u64,
}

/// Cached candles and health for a (coin, timeframe), oldest first.
#[tauri::command]
fn coin_candles(store: tauri::State<'_, Arc<MarketStore>>, coin: String, timeframe: String) -> CandleSnapshotPayload {
    let Some(timeframe) = Timeframe::from_hl(&timeframe) else {
        return CandleSnapshotPayload {
            candles: Vec::new(),
            health: CandleHealth::Loading,
            revision: 0,
        };
    };
    let snapshot = store.candle_snapshot(&coin, timeframe, Utc::now().timestamp_millis());
    CandleSnapshotPayload {
        candles: snapshot.candles.iter().map(CandlePayload::from).collect(),
        health: snapshot.live_health,
        revision: snapshot.revision,
    }
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
    health: CandleHealth,
    revision: u64,
}

/// A REST or health transition invalidated the active chart's snapshot.
#[derive(Clone, Serialize)]
struct CandleSnapshotUpdatePayload {
    coin: String,
    timeframe: String,
    health: CandleHealth,
    revision: u64,
}

fn emit_candle_snapshot_update(handle: &tauri::AppHandle, store: &MarketStore, coin: &str, timeframe: Timeframe) {
    let snapshot = store.candle_snapshot(coin, timeframe, Utc::now().timestamp_millis());
    let payload = CandleSnapshotUpdatePayload {
        coin: coin.to_owned(),
        timeframe: timeframe.as_hl().to_owned(),
        health: snapshot.live_health,
        revision: snapshot.revision,
    };
    let _ = handle.emit("candle-snapshot-updated", payload);
}

/// Set (or clear) the exact candle series consumed by the open detail chart.
#[tauri::command]
fn set_active_series(store: tauri::State<'_, Arc<MarketStore>>, coin: Option<String>, timeframe: Option<String>) {
    let series = coin
        .zip(timeframe)
        .and_then(|(coin, timeframe)| Timeframe::from_hl(&timeframe).map(|timeframe| ActiveSeries { coin, timeframe }));
    store.set_active_series(series);
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
    let candle_update_store = Arc::clone(&store);
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
        .manage(Arc::new(SignalValidationState::default()))
        .setup(move |app| {
            // Client clones share one pool; Hyperliquid requests add the shared
            // weight limiter while logo downloads use the same HTTP transport directly.
            // Timeouts keep a half-open connection from wedging sync, paging, and logos.
            let http = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?;
            let rest = Arc::new(HyperliquidRest::new(http.clone(), sync_config.weight_budget_per_min));

            // The local database holds candles + watchlists. Open it synchronously so
            // the list commands have their managed state before the UI can call them.
            let persistent_database = match app.path().app_data_dir() {
                Ok(data_dir) => {
                    if let Err(error) = std::fs::create_dir_all(&data_dir) {
                        tracing::warn!(path = %data_dir.display(), %error, "failed to create the application data directory");
                    }
                    tauri::async_runtime::block_on(SqliteStore::open(&data_dir.join("coinwatch.db")))
                        .map_err(|error| error.to_string())
                }
                Err(error) => Err(format!("failed to resolve the application data directory: {error}")),
            };

            let (database, storage_mode, storage_error) = match persistent_database {
                Ok(database) => (database, StorageMode::Persistent, None),
                Err(error) => {
                    tracing::error!(%error, "failed to open the local database; using temporary storage");
                    let temporary = tauri::async_runtime::block_on(SqliteStore::temporary())?;
                    (temporary, StorageMode::Temporary, Some(error))
                }
            };
            sync_store.set_storage_mode(storage_mode, storage_error);
            let database = Arc::new(database);
            let persistence: Arc<dyn CandleStore> = database.clone();
            let provider: Arc<dyn CandleProvider> = rest.clone();
            let history =
                Arc::new(CandleHistory::new(Arc::clone(&persistence), provider).with_status(Arc::clone(&sync_store)));
            app.manage(Arc::clone(&database));
            app.manage(history);
            let logo_db = Arc::clone(&database);
            // Keep the hot cache warm from Hyperliquid — the whole tradable universe.
            tauri::async_runtime::spawn(async move {
                let _sync = spawn_sync(sync_store, sync_config, persistence, rest);
            });
            // Cache coin logos locally so they load instantly and offline.
            tauri::async_runtime::spawn(cache_logos(logo_market_store, logo_db, http));

            // Relay sync status (connecting / backfilling / live) to the UI.
            let status_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut status = status_store.watch_status();
                loop {
                    let payload = status.borrow_and_update().clone();
                    let _ = status_handle.emit("status", payload);
                    if status.changed().await.is_err() {
                        break;
                    }
                }
            });

            // Relay REST reconciliations and health transitions for the active chart.
            let candle_update_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut updates = candle_update_store.subscribe_candle_updates();
                loop {
                    match updates.recv().await {
                        Ok(update) => {
                            if candle_update_store.is_active_series(&update.coin, update.timeframe) {
                                emit_candle_snapshot_update(
                                    &candle_update_handle,
                                    &candle_update_store,
                                    &update.coin,
                                    update.timeframe,
                                );
                            }
                        }
                        Err(RecvError::Lagged(_)) => {
                            if let Some(active) = candle_update_store.active_series() {
                                emit_candle_snapshot_update(
                                    &candle_update_handle,
                                    &candle_update_store,
                                    &active.coin,
                                    active.timeframe,
                                );
                            }
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            });

            // Stream canonical mark prices and relay active-series candles.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let event_store = Arc::clone(&adapter_store);
                let mut adapter = HyperliquidAdapter::new(adapter_store, streamed_timeframes);
                let mut events = adapter.feed().subscribe_all();
                adapter.connect();
                loop {
                    match events.recv().await {
                        Ok(event) => match &*event {
                            MarketEvent::Mark(mark) => {
                                let payload = MarkPricePayload {
                                    coin: mark.symbol.clone(),
                                    price: mark.price.to_f64().unwrap_or(0.0),
                                };
                                let _ = handle.emit("mark-price", payload);
                            }
                            MarketEvent::Candle(candle) => {
                                if event_store.is_active_series(&candle.coin, candle.timeframe) {
                                    let snapshot = event_store.candle_snapshot(
                                        &candle.coin,
                                        candle.timeframe,
                                        Utc::now().timestamp_millis(),
                                    );
                                    let payload = CandleEventPayload {
                                        coin: candle.coin.clone(),
                                        timeframe: candle.timeframe.as_hl().to_owned(),
                                        candle: CandlePayload::from(&candle.candle),
                                        health: snapshot.live_health,
                                        revision: snapshot.revision,
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
            market_status,
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
            set_active_series,
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
            signal_runs::cancel_signal_run,
            signal_validation::start_signal_validation,
            signal_validation::cancel_signal_validation
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod candle_contract_tests {
    use super::{CandleHealth, CandlePayload, CandleSnapshotPayload, CandleSnapshotUpdatePayload};

    #[test]
    fn candle_snapshot_contract_serializes_health_and_revision() {
        let snapshot = CandleSnapshotPayload {
            candles: vec![CandlePayload {
                time: 1,
                open: 2.0,
                high: 3.0,
                low: 1.0,
                close: 2.5,
                volume: 4.0,
            }],
            health: CandleHealth::Verifying,
            revision: 7,
        };
        assert_eq!(
            serde_json::to_value(snapshot).expect("serializable snapshot"),
            serde_json::json!({
                "candles": [{ "time": 1, "open": 2.0, "high": 3.0, "low": 1.0, "close": 2.5, "volume": 4.0 }],
                "health": "verifying",
                "revision": 7,
            })
        );

        let update = CandleSnapshotUpdatePayload {
            coin: "BTC".to_owned(),
            timeframe: "1h".to_owned(),
            health: CandleHealth::Ready,
            revision: 8,
        };
        assert_eq!(
            serde_json::to_value(update).expect("serializable update"),
            serde_json::json!({ "coin": "BTC", "timeframe": "1h", "health": "ready", "revision": 8 })
        );
    }
}
