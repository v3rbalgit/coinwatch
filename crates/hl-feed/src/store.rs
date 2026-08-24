//! In-memory hot cache of live Hyperliquid market state.

use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use domain::{Candle, Timeframe};
use tokio::sync::watch;

use crate::UniverseCoin;

/// Cap on candles retained per (coin, timeframe).
const MAX_CANDLES: usize = 500;

/// What the market sync is currently doing — surfaced in the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketStatus {
    /// Establishing the first connection and fetching the universe.
    Connecting,
    /// Backfilling candle history: `done` of `total` coins seeded so far.
    Backfilling {
        /// Coins seeded so far.
        done: usize,
        /// Total coins to seed.
        total: usize,
    },
    /// Fully warm; refreshing quietly in the background.
    Live,
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
        let (status, _) = watch::channel(MarketStatus::Connecting);
        Arc::new(Self { candles: DashMap::new(), universe: RwLock::new(Vec::new()), status })
    }

    /// The latest cached candles for a (coin, timeframe), if backfilled yet.
    /// Returns a shared slice — no copy of the buffer.
    #[must_use]
    pub fn candles(&self, coin: &str, timeframe: Timeframe) -> Option<Arc<[Candle]>> {
        self.candles.get(&(coin.to_owned(), timeframe)).map(|entry| entry.value().clone().into())
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

    /// Publish a new sync status to watchers.
    pub(crate) fn set_status(&self, status: MarketStatus) {
        let _ = self.status.send(status);
    }
}
