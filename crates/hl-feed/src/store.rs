//! In-memory hot cache of live Hyperliquid market state.

use std::sync::{Arc, PoisonError, RwLock};

use dashmap::DashMap;
use domain::{Candle, Timeframe};
use serde::Serialize;
use tokio::sync::watch;

use crate::{CANDLE_WINDOW, UniverseCoin};

/// WebSocket connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionState {
    /// Opening the first WebSocket connection.
    Connecting,
    /// Connected and streaming.
    Live,
    /// Dropped; backing off before a reconnect.
    Reconnecting,
}

/// What the REST sync is doing right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncPhase {
    /// Fetching the universe/metadata snapshot.
    #[serde(rename = "loading")]
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
    /// Candle fetch or reconciliation errors in the current pass.
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

struct CandleBuffer {
    candles: Arc<Vec<Candle>>,
    needs_repair: bool,
}

impl Default for CandleBuffer {
    fn default() -> Self {
        Self {
            candles: Arc::new(Vec::new()),
            needs_repair: false,
        }
    }
}

/// How a REST candle snapshot updates an existing hot buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileMode {
    /// Discard the old hot buffer after an initial or full repair fetch.
    Replace,
    /// Replace overlapping bars and append newer bars while preserving history.
    Merge,
}

/// Minimal series state used to plan the next REST reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHealth {
    /// Latest cached candle open, if the buffer is non-empty.
    pub latest_open_ms: Option<i64>,
    /// Whether a disconnect or alignment gap requires a full REST repair.
    pub needs_repair: bool,
}

impl SeriesHealth {
    /// Whether the series is safe to expose at this instant.
    pub(crate) fn is_current(self, timeframe: Timeframe, now_ms: i64) -> bool {
        !self.needs_repair && self.latest_open_ms == Some(timeframe.bar_open_ms(now_ms))
    }
}

/// A concurrent hot cache of live market state: recent candles per (coin,
/// timeframe), the ranked universe, and the sync status. Cheap to share (`Arc`);
/// reads never touch the network, so all computation runs against this cache.
pub struct MarketStore {
    candles: DashMap<(String, Timeframe), CandleBuffer>,
    universe: RwLock<Vec<UniverseCoin>>,
    status: watch::Sender<MarketStatus>,
    /// The coin whose live candles the UI wants forwarded (the open detail page).
    active_coin: RwLock<Option<String>>,
}

fn trim_to_window(candles: &mut Vec<Candle>) {
    if candles.len() > CANDLE_WINDOW {
        let excess = candles.len() - CANDLE_WINDOW;
        candles.drain(..excess);
    }
}

fn push_capped(candles: &mut Vec<Candle>, candle: Candle) {
    if candles.len() >= CANDLE_WINDOW {
        let excess = candles.len() - CANDLE_WINDOW + 1;
        candles.drain(..excess);
    }
    candles.push(candle);
}

fn candles_are_contiguous(candles: &[Candle], timeframe: Timeframe) -> bool {
    candles
        .iter()
        .all(|candle| timeframe.is_aligned(candle.open_time.timestamp_millis()))
        && candles.windows(2).all(|pair| {
            let [previous, next] = pair else {
                return false;
            };
            previous.open_time.timestamp_millis().saturating_add(timeframe.duration_ms())
                == next.open_time.timestamp_millis()
        })
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
            active_coin: RwLock::new(None),
        })
    }

    /// The latest cached candles for a (coin, timeframe), if backfilled yet.
    /// Cloning the returned `Arc` does not copy the candle buffer. Writers use
    /// Arc-backed clone-on-write only while another snapshot is held.
    #[must_use]
    pub fn candles(&self, coin: &str, timeframe: Timeframe) -> Option<Arc<Vec<Candle>>> {
        self.candles
            .get(&(coin.to_owned(), timeframe))
            .map(|entry| Arc::clone(&entry.candles))
    }

    /// A current, contiguous candle snapshot suitable for signal evaluation.
    ///
    /// The latest row must be the expected forming bar at `now_ms`. During a bar
    /// boundary or detected gap this returns `None` until REST reconciliation has
    /// repaired the series, so stale data cannot become a completed-bar signal.
    #[must_use]
    pub fn current_candles(&self, coin: &str, timeframe: Timeframe, now_ms: i64) -> Option<Arc<Vec<Candle>>> {
        let entry = self.candles.get(&(coin.to_owned(), timeframe))?;
        let health = SeriesHealth {
            latest_open_ms: entry.candles.last().map(|candle| candle.open_time.timestamp_millis()),
            needs_repair: entry.needs_repair,
        };
        health.is_current(timeframe, now_ms).then(|| Arc::clone(&entry.candles))
    }

    /// Latest-open and gap state for REST reconciliation planning.
    pub(crate) fn series_health(&self, coin: &str, timeframe: Timeframe) -> SeriesHealth {
        self.candles.get(&(coin.to_owned(), timeframe)).map_or(
            SeriesHealth {
                latest_open_ms: None,
                needs_repair: false,
            },
            |entry| SeriesHealth {
                latest_open_ms: entry.candles.last().map(|candle| candle.open_time.timestamp_millis()),
                needs_repair: entry.needs_repair,
            },
        )
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

    /// Replace the cached candles for a (coin, timeframe) — an initial or full
    /// REST backfill. Misaligned or non-contiguous snapshots remain marked as gaps.
    pub(crate) fn set_candles(&self, coin: String, timeframe: Timeframe, mut candles: Vec<Candle>) {
        trim_to_window(&mut candles);
        let needs_repair = !candles_are_contiguous(&candles, timeframe);
        self.candles.insert(
            (coin, timeframe),
            CandleBuffer {
                candles: Arc::new(candles),
                needs_repair,
            },
        );
    }

    /// Reconcile a REST snapshot with the hot buffer. Incremental snapshots replace
    /// overlapping bars and fill new bars without discarding older history.
    pub(crate) fn reconcile_candles(
        &self,
        coin: String,
        timeframe: Timeframe,
        candles: Vec<Candle>,
        mode: ReconcileMode,
    ) {
        if mode == ReconcileMode::Replace {
            self.set_candles(coin, timeframe, candles);
            return;
        }
        if candles.is_empty() {
            return;
        }

        let mut entry = self.candles.entry((coin, timeframe)).or_default();
        let state = entry.value_mut();
        let buffer = Arc::make_mut(&mut state.candles);
        for candle in candles {
            let open_ms = candle.open_time.timestamp_millis();
            let index = buffer.partition_point(|existing| existing.open_time.timestamp_millis() < open_ms);
            if index == buffer.len() {
                push_capped(buffer, candle);
            } else {
                match buffer.get_mut(index) {
                    Some(existing) if existing.open_time.timestamp_millis() == open_ms => *existing = candle,
                    _ => buffer.insert(index, candle),
                }
            }
        }
        trim_to_window(buffer);
        state.needs_repair = !candles_are_contiguous(buffer, timeframe);
    }

    /// Merge a streamed candle: update the forming bar in place when its open time
    /// matches the last cached bar, otherwise append it and retain any gap marker
    /// until REST reconciliation succeeds.
    pub(crate) fn merge_candle(&self, coin: &str, timeframe: Timeframe, candle: Candle) {
        enum Merge {
            Replace,
            Append { gap: bool },
            Ignore,
        }

        let mut entry = self.candles.entry((coin.to_owned(), timeframe)).or_default();
        let state = entry.value_mut();
        let buffer = Arc::make_mut(&mut state.candles);
        let candle_open_ms = candle.open_time.timestamp_millis();
        let merge = match buffer.last() {
            Some(last) if last.open_time == candle.open_time => Merge::Replace,
            Some(last) if candle.open_time > last.open_time => {
                let expected = last.open_time.timestamp_millis().saturating_add(timeframe.duration_ms());
                Merge::Append {
                    gap: candle_open_ms != expected,
                }
            }
            Some(_) => Merge::Ignore,
            None => Merge::Append {
                gap: !timeframe.is_aligned(candle_open_ms),
            },
        };
        match merge {
            Merge::Replace => {
                if let Some(last) = buffer.last_mut() {
                    *last = candle;
                }
            }
            Merge::Append { gap } => {
                push_capped(buffer, candle);
                state.needs_repair |= gap;
            }
            Merge::Ignore => {}
        }
    }

    /// Mark streamed series unavailable after a WebSocket session ends so REST
    /// confirms any candle updates missed before or across the disconnect.
    pub(crate) fn mark_streamed_for_reconciliation(&self, timeframes: &[Timeframe]) {
        for mut entry in self.candles.iter_mut() {
            if timeframes.contains(&entry.key().1) {
                entry.needs_repair = true;
            }
        }
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

    /// Update backfill progress: sets completed / total, errors, and rate.
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
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe};
    use rust_decimal::Decimal;

    use super::{CANDLE_WINDOW, MarketStore, ReconcileMode};

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
        assert!(!store.series_health("BTC", Timeframe::Hour1).needs_repair);
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_some());
    }

    #[test]
    fn a_gap_remains_unavailable_until_rest_reconciles_it() {
        let store = MarketStore::new();
        store.merge_candle("BTC", Timeframe::Hour1, candle(0));
        store.merge_candle("BTC", Timeframe::Hour1, candle(2));
        assert!(store.series_health("BTC", Timeframe::Hour1).needs_repair);
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_none());

        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(1), candle(2)],
            ReconcileMode::Merge,
        );
        assert!(!store.series_health("BTC", Timeframe::Hour1).needs_repair);
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_some());
    }

    #[test]
    fn non_contiguous_backfills_require_repair() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(2)]);

        assert!(store.series_health("BTC", Timeframe::Hour1).needs_repair);
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_none());
    }

    #[test]
    fn a_series_is_unavailable_when_the_next_bar_opens() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1)]);

        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000 - 1).is_some());
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_none());
    }

    #[test]
    fn disconnected_streams_remain_unavailable_until_rest_repair() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0)]);
        store.mark_streamed_for_reconciliation(&[Timeframe::Hour1]);
        store.merge_candle("BTC", Timeframe::Hour1, candle(0));
        assert!(store.current_candles("BTC", Timeframe::Hour1, 0).is_none());

        store.reconcile_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0)], ReconcileMode::Replace);
        assert!(store.current_candles("BTC", Timeframe::Hour1, 0).is_some());
    }

    #[test]
    fn capped_appends_do_not_grow_the_buffer_allocation() {
        let store = MarketStore::new();
        let mut candles: Vec<Candle> = (0..i64::try_from(CANDLE_WINDOW).expect("window fits i64"))
            .map(candle)
            .collect();
        candles.shrink_to_fit();
        let capacity = candles.capacity();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, candles);

        store.merge_candle(
            "BTC",
            Timeframe::Hour1,
            candle(i64::try_from(CANDLE_WINDOW).expect("window fits i64")),
        );
        let updated = store.candles("BTC", Timeframe::Hour1).expect("updated buffer");
        assert_eq!(updated.len(), CANDLE_WINDOW);
        assert_eq!(updated.capacity(), capacity);
    }

    #[test]
    fn snapshots_share_buffers_and_stream_writes_use_clone_on_write() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1)]);
        let first = store.candles("BTC", Timeframe::Hour1).expect("first snapshot");
        let second = store.candles("BTC", Timeframe::Hour1).expect("second snapshot");
        assert!(Arc::ptr_eq(&first, &second));

        store.merge_candle("BTC", Timeframe::Hour1, candle(2));
        let updated = store.candles("BTC", Timeframe::Hour1).expect("updated snapshot");
        assert_eq!(first.len(), 2);
        assert_eq!(updated.len(), 3);
        assert!(!Arc::ptr_eq(&first, &updated));
    }
}
