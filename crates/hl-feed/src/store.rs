//! In-memory hot cache of live Hyperliquid market state.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use chrono::TimeDelta;
use dashmap::DashMap;
use domain::{Candle, Timeframe};
use tokio::sync::watch;

use crate::{CANDLE_WINDOW, UniverseCoin};

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
    /// (coin, timeframe) sets the WebSocket saw a gap in; the sync re-backfills them.
    stale: Mutex<HashSet<(String, Timeframe)>>,
    /// The coin whose live candles the UI wants forwarded (the open detail page).
    active_coin: RwLock<Option<String>>,
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
            stale: Mutex::new(HashSet::new()),
            active_coin: RwLock::new(None),
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
        self.universe.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// The coin whose live candles the UI wants forwarded (the open detail page).
    #[must_use]
    pub fn active_coin(&self) -> Option<String> {
        self.active_coin.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Set (or clear) the coin whose live candles are forwarded to the UI.
    pub fn set_active_coin(&self, coin: Option<String>) {
        *self.active_coin.write().unwrap_or_else(PoisonError::into_inner) = coin;
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
        enum Merge {
            Replace,
            Append { gap: bool },
            Ignore,
        }

        let mut entry = self.candles.entry((coin.to_owned(), timeframe)).or_default();
        let buffer = entry.value_mut();
        let merge = match buffer.last() {
            Some(last) if last.open_time == candle.open_time => Merge::Replace,
            Some(last) if candle.open_time > last.open_time => {
                let contiguous = last
                    .open_time
                    .checked_add_signed(TimeDelta::milliseconds(timeframe.duration_ms()))
                    .is_some_and(|next_open| candle.open_time <= next_open);
                Merge::Append { gap: !contiguous }
            }
            Some(_) => Merge::Ignore,
            None => Merge::Append { gap: false },
        };
        match merge {
            Merge::Replace => {
                if let Some(last) = buffer.last_mut() {
                    *last = candle;
                }
            }
            Merge::Append { gap } => {
                buffer.push(candle);
                if buffer.len() > CANDLE_WINDOW {
                    let excess = buffer.len() - CANDLE_WINDOW;
                    buffer.drain(0..excess);
                }
                if gap {
                    self.mark_stale(coin, timeframe);
                }
            }
            Merge::Ignore => {}
        }
    }

    /// Flag a (coin, timeframe) whose streamed candles are non-contiguous so the
    /// sync re-backfills it and heals the gap.
    fn mark_stale(&self, coin: &str, timeframe: Timeframe) {
        self.stale
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((coin.to_owned(), timeframe));
    }

    /// Drain the (coin, timeframe) sets flagged as gapped since the last call.
    pub(crate) fn take_stale(&self) -> HashSet<(String, Timeframe)> {
        let mut stale = self.stale.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *stale)
    }

    /// Replace the cached universe.
    pub(crate) fn set_universe(&self, universe: Vec<UniverseCoin>) {
        *self.universe.write().unwrap_or_else(PoisonError::into_inner) = universe;
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

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use rust_decimal::Decimal;

    use super::MarketStore;

    fn candle(hour: i64) -> Candle {
        Candle {
            open_time: Utc.timestamp_opt(hour * 3600, 0).single().expect("timestamp"),
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ZERO,
        }
    }

    #[test]
    fn contiguous_merges_flag_nothing() {
        let store = MarketStore::new();
        for hour in 0..3 {
            store.merge_candle("BTC", Timeframe::Hour1, candle(hour));
        }
        assert!(store.take_stale().is_empty());
    }

    #[test]
    fn a_missed_bar_flags_stale_once() {
        let store = MarketStore::new();
        store.merge_candle("BTC", Timeframe::Hour1, candle(0));
        store.merge_candle("BTC", Timeframe::Hour1, candle(2));
        assert!(store.take_stale().contains(&("BTC".to_owned(), Timeframe::Hour1)));
        assert!(store.take_stale().is_empty());
    }
}
