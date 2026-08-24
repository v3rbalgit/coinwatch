//! In-memory hot cache of live Hyperliquid market state.

use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use domain::{Candle, Timeframe};
use tokio::sync::watch;

use crate::UniverseCoin;

/// Cap on candles retained per (coin, timeframe).
const MAX_CANDLES: usize = 500;

/// WebSocket connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Opening the first WebSocket connection.
    Connecting,
    /// Connected and streaming.
    Live,
    /// Dropped; backing off before a reconnect.
    Reconnecting,
}

/// What the REST sync is doing right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPhase {
    /// Fetching the universe/metadata snapshot.
    LoadingUniverse,
    /// Backfilling candle history over REST.
    Backfilling,
    /// Warm; candles refresh quietly in the background.
    Live,
}

/// A granular snapshot of what the market engine is doing, surfaced in the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketStatus {
    /// WebSocket connection state.
    pub connection: ConnectionState,
    /// Current REST sync phase.
    pub phase: SyncPhase,
    /// Active coins in the universe.
    pub universe: usize,
    /// Candle (coin, timeframe) sets cached so far this backfill pass.
    pub backfill_done: usize,
    /// Total (coin, timeframe) sets to backfill this pass.
    pub backfill_total: usize,
    /// Candle fetches skipped this pass (rate-limit / errors).
    pub backfill_errors: usize,
    /// Candle streams currently subscribed over the WebSocket.
    pub streaming: usize,
    /// Recent candle-fetch rate, in fetches per minute.
    pub fetch_rate: u32,
}

impl Default for MarketStatus {
    fn default() -> Self {
        Self {
            connection: ConnectionState::Connecting,
            phase: SyncPhase::LoadingUniverse,
            universe: 0,
            backfill_done: 0,
            backfill_total: 0,
            backfill_errors: 0,
            streaming: 0,
            fetch_rate: 0,
        }
    }
}

/// A lock-free hot cache of live market state: recent candles per (coin,
/// timeframe), the ranked universe, and the sync status. Cheap to share (`Arc`);
/// reads never touch the network, so all computation runs against this cache.
pub struct MarketStore {
    candles: DashMap<(String, Timeframe), Vec<Candle>>,
    universe: RwLock<Vec<UniverseCoin>>,
    status: watch::Sender<MarketStatus>,
}

impl MarketStore {
    /// Create an empty store behind an `Arc`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        let (status, _) = watch::channel(MarketStatus::default());
        Arc::new(Self {
            candles: DashMap::new(),
            universe: RwLock::new(Vec::new()),
            status,
        })
    }

    /// The latest cached candles for a (coin, timeframe), if backfilled yet.
    /// Returns a shared slice — no copy of the buffer.
    #[must_use]
    pub fn candles(&self, coin: &str, timeframe: Timeframe) -> Option<Arc<[Candle]>> {
        self.candles
            .get(&(coin.to_owned(), timeframe))
            .map(|entry| entry.value().clone().into())
    }

    /// A snapshot of the current volume-ranked universe.
    #[must_use]
    pub fn universe(&self) -> Vec<UniverseCoin> {
        self.universe.read().map(|guard| guard.clone()).unwrap_or_default()
    }

    /// The current sync status.
    #[must_use]
    pub fn status(&self) -> MarketStatus {
        self.status.borrow().clone()
    }

    /// Subscribe to sync-status changes.
    #[must_use]
    pub fn watch_status(&self) -> watch::Receiver<MarketStatus> {
        self.status.subscribe()
    }

    /// Replace the cached candles for a (coin, timeframe) — the REST backfill.
    pub(crate) fn set_candles(&self, coin: String, timeframe: Timeframe, candles: Vec<Candle>) {
        self.candles.insert((coin, timeframe), candles);
    }

    /// Merge a streamed candle: update the forming bar in place when its open time
    /// matches the last cached bar, otherwise append the new bar (capping length).
    pub(crate) fn merge_candle(&self, coin: &str, timeframe: Timeframe, candle: Candle) {
        let mut entry = self.candles.entry((coin.to_owned(), timeframe)).or_default();
        let buffer = entry.value_mut();
        match buffer.last_mut() {
            Some(last) if last.open_time == candle.open_time => *last = candle,
            _ => {
                buffer.push(candle);
                if buffer.len() > MAX_CANDLES {
                    let excess = buffer.len() - MAX_CANDLES;
                    buffer.drain(0..excess);
                }
            }
        }
    }

    /// Replace the cached universe.
    pub(crate) fn set_universe(&self, universe: Vec<UniverseCoin>) {
        if let Ok(mut guard) = self.universe.write() {
            *guard = universe;
        }
    }

    /// Update the WebSocket connection state.
    pub(crate) fn set_connection(&self, connection: ConnectionState) {
        self.status.send_modify(|status| status.connection = connection);
    }

    /// Update the number of subscribed candle streams.
    pub(crate) fn set_streaming(&self, streaming: usize) {
        self.status.send_modify(|status| status.streaming = streaming);
    }

    /// Update the REST sync phase.
    pub(crate) fn set_phase(&self, phase: SyncPhase) {
        self.status.send_modify(|status| status.phase = phase);
    }

    /// Update the universe size.
    pub(crate) fn set_universe_size(&self, universe: usize) {
        self.status.send_modify(|status| status.universe = universe);
    }

    /// Update backfill progress: sets cached / total, skipped fetches, and rate.
    pub(crate) fn set_backfill(&self, done: usize, total: usize, errors: usize, fetch_rate: u32) {
        self.status.send_modify(|status| {
            status.backfill_done = done;
            status.backfill_total = total;
            status.backfill_errors = errors;
            status.fetch_rate = fetch_rate;
        });
    }
}
