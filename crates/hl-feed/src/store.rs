//! In-memory hot cache of live Hyperliquid market state.

use std::collections::HashSet;
use std::sync::{Arc, PoisonError, RwLock};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use domain::{Candle, Timeframe};
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::{broadcast, watch};

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

/// Durability of the SQLite store backing history and user-authored data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageMode {
    /// The application has not finished opening storage yet.
    Initializing,
    /// Data is backed by the normal on-disk database.
    Persistent,
    /// The on-disk database failed to open and this session uses an in-memory fallback.
    Temporary,
}

/// A granular snapshot of what the market engine is doing, surfaced in the UI.
/// Serialized field names are the frontend `status` event contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    /// Whether storage is durable or limited to this process.
    pub storage_mode: StorageMode,
    /// Storage failures observed during this process.
    pub storage_failures: u64,
    /// Most recent unresolved storage failure.
    pub last_storage_error: Option<String>,
    /// Universe or candle-reconciliation failures observed during this process.
    pub reconciliation_failures: u64,
    /// Most recent unresolved universe or candle-reconciliation failure.
    pub last_reconciliation_error: Option<String>,
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
            storage_mode: StorageMode::Initializing,
            storage_failures: 0,
            last_storage_error: None,
            reconciliation_failures: 0,
            last_reconciliation_error: None,
        }
    }
}

/// Trust state attached to every chart candle snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleHealth {
    /// No candle buffer has been seeded yet.
    Loading,
    /// The buffer is verified, contiguous, and current.
    Ready,
    /// The buffer is structurally valid but no longer current.
    Stale,
    /// A bounded REST overlap is verifying the buffer after a disconnect.
    Verifying,
    /// A proven structural problem requires REST replacement.
    Repairing,
}

/// Candle data and its authoritative chart-consumer health.
#[derive(Debug, Clone)]
pub struct CandleSnapshot {
    /// Cached candles, oldest first. Unhealthy rows may be shown as last-known data.
    pub candles: Arc<Vec<Candle>>,
    /// Whether consumers may treat the snapshot as current and verified.
    pub health: CandleHealth,
    /// Monotonic per-series version for rejecting stale asynchronous responses.
    pub revision: u64,
}

/// A candle snapshot should be re-read for this series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandleUpdate {
    /// Coin symbol.
    pub coin: String,
    /// Candle timeframe.
    pub timeframe: Timeframe,
}

struct CandleBuffer {
    candles: Arc<Vec<Candle>>,
    integrity: CandleIntegrity,
    repair_epoch: u64,
    stream_revision: u64,
    revision: u64,
    recent_stream_writes: Vec<(i64, u64)>,
}

impl CandleBuffer {
    fn health(&self) -> SeriesHealth {
        SeriesHealth {
            latest_open_ms: self.candles.last().map(|candle| candle.open_time.timestamp_millis()),
            integrity: self.integrity,
            repair_epoch: self.repair_epoch,
            write_revision: self.stream_revision,
        }
    }

    const fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }

    fn consumer_health(&self, timeframe: Timeframe, now_ms: i64) -> CandleHealth {
        match self.integrity {
            CandleIntegrity::VerifyOverlap => CandleHealth::Verifying,
            CandleIntegrity::Repair => CandleHealth::Repairing,
            CandleIntegrity::Verified if self.candles.is_empty() => CandleHealth::Loading,
            CandleIntegrity::Verified if self.health().is_current(timeframe, now_ms) => CandleHealth::Ready,
            CandleIntegrity::Verified => CandleHealth::Stale,
        }
    }
}

impl Default for CandleBuffer {
    fn default() -> Self {
        Self {
            candles: Arc::new(Vec::new()),
            integrity: CandleIntegrity::Verified,
            repair_epoch: 0,
            stream_revision: 0,
            revision: 0,
            recent_stream_writes: Vec::with_capacity(RECENT_STREAM_WRITE_WINDOW),
        }
    }
}

/// Whether a candle series is safe, awaiting disconnect verification, or broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandleIntegrity {
    /// The cached structure has been verified and may be exposed when current.
    Verified,
    /// A small REST overlap must confirm continuity after a disconnect.
    VerifyOverlap,
    /// A proven alignment or continuity failure requires full replacement.
    Repair,
}

/// How a REST candle snapshot updates an existing hot buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileMode {
    /// Discard the old hot buffer after an initial or full repair fetch.
    Replace,
    /// Replace overlapping bars and append newer bars while preserving history.
    Merge,
    /// Merge a bounded overlap and classify disconnect integrity from the result.
    VerifyOverlap,
}

/// Minimal series state used to plan the next REST reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHealth {
    /// Latest cached candle open, if the buffer is non-empty.
    pub latest_open_ms: Option<i64>,
    /// Whether the series is verified, awaiting overlap confirmation, or broken.
    pub integrity: CandleIntegrity,
    /// Bumped by every disconnect mark; a reconcile observing an older epoch keeps it.
    pub repair_epoch: u64,
    /// Bumped whenever WebSocket candle values change during a REST request.
    pub write_revision: u64,
}

impl SeriesHealth {
    /// Whether the series is safe to expose at this instant. A buffer one bar
    /// ahead of `now_ms` (clock skew, a boundary crossed mid-run) is current too.
    pub(crate) fn is_current(self, timeframe: Timeframe, now_ms: i64) -> bool {
        self.integrity == CandleIntegrity::Verified
            && self.latest_open_ms.is_some_and(|open_ms| {
                let expected = timeframe.bar_open_ms(now_ms);
                open_ms == expected || open_ms == expected.saturating_add(timeframe.duration_ms())
            })
    }
}

/// A concurrent hot cache of live market state: recent candles per (coin,
/// timeframe), the ranked universe, and the sync status. Cheap to share (`Arc`);
/// reads never touch the network, so all computation runs against this cache.
pub struct MarketStore {
    candles: DashMap<(String, Timeframe), CandleBuffer>,
    candle_updates: broadcast::Sender<CandleUpdate>,
    universe: RwLock<Vec<UniverseCoin>>,
    status: watch::Sender<MarketStatus>,
    /// The coin whose live candles the UI wants forwarded (the open detail page).
    active_coin: RwLock<Option<String>>,
}

/// Bars fetched to verify a disconnect and still replaceable by late streamed updates.
pub(crate) const STREAM_RECONCILE_WINDOW: usize = 3;
const CANDLE_UPDATE_CAPACITY: usize = 4_096;
const MIN_STREAM_OVERLAP_ROWS: usize = 2;
/// A fetch can cross one boundary after all mutable trailing bars were updated.
const RECENT_STREAM_WRITE_WINDOW: usize = STREAM_RECONCILE_WINDOW + 1;

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

pub(crate) fn candles_are_contiguous(candles: &[Candle], timeframe: Timeframe) -> bool {
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

fn overlap_verification_is_sufficient(existing: &[Candle], incoming: &[Candle], timeframe: Timeframe) -> bool {
    let required_rows = existing.len().min(MIN_STREAM_OVERLAP_ROWS);
    if incoming.len() < required_rows {
        return false;
    }
    let (Some(existing_latest), Some(incoming_latest)) = (existing.last(), incoming.last()) else {
        return false;
    };
    let overlap_intervals = i64::try_from(STREAM_RECONCILE_WINDOW.saturating_sub(1)).unwrap_or(i64::MAX);
    let earliest_overlap = existing_latest
        .open_time
        .timestamp_millis()
        .saturating_sub(timeframe.duration_ms().saturating_mul(overlap_intervals));
    incoming_latest.open_time.timestamp_millis() >= earliest_overlap
}

impl MarketStore {
    /// Create an empty store behind an `Arc`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        let (status, _) = watch::channel(MarketStatus::default());
        let (candle_updates, _) = broadcast::channel(CANDLE_UPDATE_CAPACITY);
        Arc::new(Self {
            candles: DashMap::new(),
            candle_updates,
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

    /// Candle data plus explicit health for chart consumers.
    #[must_use]
    pub fn candle_snapshot(&self, coin: &str, timeframe: Timeframe, now_ms: i64) -> CandleSnapshot {
        self.candles.get(&(coin.to_owned(), timeframe)).map_or(
            CandleSnapshot {
                candles: Arc::new(Vec::new()),
                health: CandleHealth::Loading,
                revision: 0,
            },
            |entry| CandleSnapshot {
                candles: Arc::clone(&entry.candles),
                health: entry.consumer_health(timeframe, now_ms),
                revision: entry.revision,
            },
        )
    }

    /// Subscribe to candle snapshot invalidations caused by health or REST changes.
    #[must_use]
    pub fn subscribe_candle_updates(&self) -> broadcast::Receiver<CandleUpdate> {
        self.candle_updates.subscribe()
    }

    /// Notify chart consumers to re-read a series after data or health changes.
    pub(crate) fn notify_candle_update(&self, coin: &str, timeframe: Timeframe) {
        let _ = self.candle_updates.send(CandleUpdate {
            coin: coin.to_owned(),
            timeframe,
        });
    }

    /// A current, contiguous candle snapshot suitable for signal evaluation.
    ///
    /// The latest row must be the expected forming bar at `now_ms`. During a bar
    /// boundary or detected gap this returns `None` until REST reconciliation has
    /// repaired the series, so stale data cannot become a completed-bar signal.
    #[must_use]
    pub fn current_candles(&self, coin: &str, timeframe: Timeframe, now_ms: i64) -> Option<Arc<Vec<Candle>>> {
        let entry = self.candles.get(&(coin.to_owned(), timeframe))?;
        entry.health().is_current(timeframe, now_ms).then(|| Arc::clone(&entry.candles))
    }

    /// Latest-open and gap state for REST reconciliation planning.
    pub(crate) fn series_health(&self, coin: &str, timeframe: Timeframe) -> SeriesHealth {
        self.candles.get(&(coin.to_owned(), timeframe)).map_or(
            SeriesHealth {
                latest_open_ms: None,
                integrity: CandleIntegrity::Verified,
                repair_epoch: 0,
                write_revision: 0,
            },
            |entry| entry.health(),
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
        let universe = self.universe.read().unwrap_or_else(PoisonError::into_inner);
        let coin = coin.filter(|coin| universe.is_empty() || universe.iter().any(|market| market.coin == *coin));
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
        let integrity = if candles_are_contiguous(&candles, timeframe) {
            CandleIntegrity::Verified
        } else {
            CandleIntegrity::Repair
        };
        let update_coin = coin.clone();
        let mut entry = self.candles.entry((coin, timeframe)).or_default();
        let state = entry.value_mut();
        let changed = state.candles.as_ref() != &candles || state.integrity != integrity;
        if changed {
            state.candles = Arc::new(candles);
            state.integrity = integrity;
            state.bump_revision();
        }
        state.recent_stream_writes.clear();
        drop(entry);
        if changed {
            self.notify_candle_update(&update_coin, timeframe);
        }
    }

    /// Reconcile a REST snapshot with the hot buffer. Incremental snapshots replace
    /// overlapping bars and fill new bars without discarding older history. When a
    /// streamed write landed after planning, REST may update history but the mutable
    /// streamed tail remains authoritative.
    pub(crate) fn reconcile_candles(
        &self,
        coin: String,
        timeframe: Timeframe,
        mut candles: Vec<Candle>,
        mode: ReconcileMode,
        observed_epoch: u64,
        observed_stream_revision: u64,
    ) {
        let update_coin = coin.clone();
        let mut entry = self.candles.entry((coin, timeframe)).or_default();
        let state = entry.value_mut();
        let previous_integrity = state.integrity;
        let mut content_changed = false;
        let incoming_contiguous = candles_are_contiguous(&candles, timeframe);
        let verification_sufficient = overlap_verification_is_sufficient(&state.candles, &candles, timeframe);
        let stream_advanced = state.stream_revision != observed_stream_revision;
        if stream_advanced {
            candles.retain(|candle| {
                let open_ms = candle.open_time.timestamp_millis();
                state.recent_stream_writes.iter().all(|&(written_open_ms, revision)| {
                    written_open_ms != open_ms || revision <= observed_stream_revision
                })
            });
        }

        if mode == ReconcileMode::Replace && !stream_advanced {
            trim_to_window(&mut candles);
            if state.candles.as_ref() != &candles {
                state.candles = Arc::new(candles);
                content_changed = true;
            }
        } else {
            let buffer = Arc::make_mut(&mut state.candles);
            for candle in candles {
                let open_ms = candle.open_time.timestamp_millis();
                let index = buffer.partition_point(|existing| existing.open_time.timestamp_millis() < open_ms);
                if index == buffer.len() {
                    push_capped(buffer, candle);
                    content_changed = true;
                } else {
                    match buffer.get_mut(index) {
                        Some(existing) if existing.open_time.timestamp_millis() == open_ms => {
                            if *existing != candle {
                                *existing = candle;
                                content_changed = true;
                            }
                        }
                        _ => {
                            buffer.insert(index, candle);
                            content_changed = true;
                        }
                    }
                }
            }
            trim_to_window(buffer);
        }

        // A newer disconnect mark survives this in-flight snapshot unchanged.
        if state.repair_epoch == observed_epoch {
            let final_contiguous = candles_are_contiguous(&state.candles, timeframe);
            state.integrity = if !final_contiguous || (mode == ReconcileMode::VerifyOverlap && !incoming_contiguous) {
                CandleIntegrity::Repair
            } else if mode == ReconcileMode::VerifyOverlap && !verification_sufficient {
                CandleIntegrity::VerifyOverlap
            } else {
                CandleIntegrity::Verified
            };
        }
        let changed = content_changed || previous_integrity != state.integrity;
        if changed {
            state.bump_revision();
        }
        drop(entry);
        if changed {
            self.notify_candle_update(&update_coin, timeframe);
        }
    }

    /// Append Hyperliquid's eventual flat zero-trade forming candle after a
    /// successful REST reconcile ends exactly one interval behind current.
    pub(crate) fn provision_forming_candle_after_reconcile(
        &self,
        coin: &str,
        timeframe: Timeframe,
        current_open_ms: i64,
    ) -> bool {
        if !timeframe.is_aligned(current_open_ms) {
            return false;
        }
        let Some(mut entry) = self.candles.get_mut(&(coin.to_owned(), timeframe)) else {
            return false;
        };
        let state = entry.value_mut();
        if state.integrity != CandleIntegrity::Verified || !candles_are_contiguous(&state.candles, timeframe) {
            return false;
        }
        let previous_open_ms = current_open_ms.saturating_sub(timeframe.duration_ms());
        let Some(previous_close) = state
            .candles
            .last()
            .and_then(|candle| (candle.open_time.timestamp_millis() == previous_open_ms).then_some(candle.close))
        else {
            return false;
        };
        let Some(open_time) = DateTime::<Utc>::from_timestamp_millis(current_open_ms) else {
            return false;
        };
        push_capped(
            Arc::make_mut(&mut state.candles),
            Candle {
                open_time,
                open: previous_close,
                high: previous_close,
                low: previous_close,
                close: previous_close,
                volume: Decimal::ZERO,
            },
        );
        state.bump_revision();
        drop(entry);
        self.notify_candle_update(coin, timeframe);
        true
    }

    /// Merge a streamed candle only while its market belongs to the current universe.
    /// Holding the universe read lock through the write prevents a late frame from
    /// recreating a buffer while a universe refresh evicts it.
    pub(crate) fn merge_active_candle(&self, coin: &str, timeframe: Timeframe, candle: Candle) -> bool {
        let universe = self.universe.read().unwrap_or_else(PoisonError::into_inner);
        if !universe.iter().any(|market| market.coin == coin) {
            return false;
        }
        self.merge_candle(coin, timeframe, candle);
        true
    }

    /// Merge a streamed candle: update the forming bar in place when its open time
    /// matches the last cached bar, replace a just-closed bar whose final update
    /// arrived late, otherwise append it and retain any gap marker until REST
    /// reconciliation succeeds.
    pub(crate) fn merge_candle(&self, coin: &str, timeframe: Timeframe, candle: Candle) {
        enum Merge {
            ReplaceAt(usize),
            Append { gap: bool },
            Ignore,
        }

        let mut entry = self.candles.entry((coin.to_owned(), timeframe)).or_default();
        let state = entry.value_mut();
        let previous_integrity = state.integrity;
        let buffer = Arc::make_mut(&mut state.candles);
        let candle_open_ms = candle.open_time.timestamp_millis();
        let merge = match buffer.last() {
            Some(last) if last.open_time == candle.open_time => Merge::ReplaceAt(buffer.len() - 1),
            Some(last) if candle.open_time > last.open_time => {
                let expected = last.open_time.timestamp_millis().saturating_add(timeframe.duration_ms());
                Merge::Append {
                    gap: candle_open_ms != expected,
                }
            }
            Some(_) => {
                let index = buffer.partition_point(|existing| existing.open_time < candle.open_time);
                match buffer.get(index) {
                    Some(existing)
                        if existing.open_time == candle.open_time
                            && buffer.len().saturating_sub(index) <= STREAM_RECONCILE_WINDOW =>
                    {
                        Merge::ReplaceAt(index)
                    }
                    _ => Merge::Ignore,
                }
            }
            None => Merge::Append {
                gap: !timeframe.is_aligned(candle_open_ms),
            },
        };
        let changed = match merge {
            Merge::ReplaceAt(index) => {
                if let Some(slot) = buffer.get_mut(index) {
                    *slot = candle;
                    true
                } else {
                    false
                }
            }
            Merge::Append { gap } => {
                push_capped(buffer, candle);
                if gap {
                    state.integrity = CandleIntegrity::Repair;
                }
                true
            }
            Merge::Ignore => false,
        };
        if changed {
            state.stream_revision = state.stream_revision.wrapping_add(1);
            state.bump_revision();
            state.recent_stream_writes.retain(|(open_ms, _)| *open_ms != candle_open_ms);
            if state.recent_stream_writes.len() >= RECENT_STREAM_WRITE_WINDOW {
                state.recent_stream_writes.remove(0);
            }
            state.recent_stream_writes.push((candle_open_ms, state.stream_revision));
        }
        let health_changed = state.integrity != previous_integrity;
        drop(entry);
        if health_changed {
            self.notify_candle_update(coin, timeframe);
        }
    }

    /// Keep streamed series unavailable after a disconnect until a bounded REST
    /// overlap verifies them. Proven structural damage is never downgraded.
    pub(crate) fn mark_streamed_for_verification(&self, timeframes: &[Timeframe]) {
        let mut updates = Vec::new();
        for mut entry in self.candles.iter_mut() {
            let (coin, timeframe) = entry.key().clone();
            if timeframes.contains(&timeframe) && !entry.candles.is_empty() {
                if entry.integrity != CandleIntegrity::Repair {
                    entry.integrity = CandleIntegrity::VerifyOverlap;
                }
                entry.repair_epoch = entry.repair_epoch.wrapping_add(1);
                entry.bump_revision();
                updates.push((coin, timeframe));
            }
        }
        for (coin, timeframe) in updates {
            self.notify_candle_update(&coin, timeframe);
        }
    }

    /// Replace the cached universe, evict inactive hot series, and update its status count.
    /// Persisted candles intentionally remain available for history and relisting.
    pub(crate) fn set_universe(&self, universe: Vec<UniverseCoin>) {
        let active: HashSet<String> = universe.iter().map(|market| market.coin.clone()).collect();
        let count = universe.len();
        let mut current = self.universe.write().unwrap_or_else(PoisonError::into_inner);
        *current = universe;
        self.candles.retain(|(coin, _), _| active.contains(coin));
        if self
            .active_coin
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|coin| !active.contains(coin))
        {
            *self.active_coin.write().unwrap_or_else(PoisonError::into_inner) = None;
        }
        drop(current);
        self.status.send_modify(|status| status.universe = count);
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

    /// Update backfill progress: sets completed / total, errors, and rate.
    pub(crate) fn set_backfill(&self, done: usize, total: usize, errors: usize, fetch_rate: u32) {
        self.status.send_modify(|status| {
            status.backfill_done = done;
            status.backfill_total = total;
            status.backfill_errors = errors;
            status.fetch_rate = fetch_rate;
        });
    }

    /// Record whether this process is using durable or temporary storage.
    pub fn set_storage_mode(&self, mode: StorageMode, error: Option<String>) {
        self.status.send_modify(|status| {
            status.storage_mode = mode;
            status.last_storage_error = error;
            if status.last_storage_error.is_some() {
                status.storage_failures = status.storage_failures.saturating_add(1);
            }
        });
    }

    /// Record a failed storage operation without changing its durability mode.
    pub(crate) fn record_storage_failure(&self, error: impl Into<String>) {
        self.status.send_modify(|status| {
            status.storage_failures = status.storage_failures.saturating_add(1);
            status.last_storage_error = Some(error.into());
        });
    }

    /// Record a failed universe or candle reconciliation for diagnostics and UI state.
    pub(crate) fn record_reconciliation_failure(&self, error: impl Into<String>) {
        self.status.send_modify(|status| {
            status.reconciliation_failures = status.reconciliation_failures.saturating_add(1);
            status.last_reconciliation_error = Some(error.into());
        });
    }

    /// Clear the current reconciliation failure after a successful retry cycle begins.
    pub(crate) fn clear_reconciliation_error(&self) {
        self.status.send_modify(|status| status.last_reconciliation_error = None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use domain::{Candle, Timeframe, completed_prefix_len};
    use rust_decimal::Decimal;

    use super::{CANDLE_WINDOW, CandleHealth, CandleIntegrity, MarketStore, ReconcileMode, StorageMode};
    use crate::UniverseCoin;

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

    fn market(coin: &str) -> UniverseCoin {
        UniverseCoin {
            coin: coin.to_owned(),
            day_volume_usd: 0.0,
            mark_px: None,
            price_decimals: 0,
            prev_day_px: None,
            funding: None,
            open_interest: None,
        }
    }

    #[test]
    fn universe_changes_evict_inactive_hot_series_and_late_stream_frames() {
        let store = MarketStore::new();
        store.set_universe(vec![market("BTC"), market("ETH")]);
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0)]);
        store.set_candles("ETH".to_owned(), Timeframe::Hour1, vec![candle(0)]);
        store.set_active_coin(Some("ETH".to_owned()));

        store.set_universe(vec![market("BTC")]);

        assert!(store.candles("BTC", Timeframe::Hour1).is_some());
        assert!(store.candles("ETH", Timeframe::Hour1).is_none());
        assert!(store.active_coin().is_none());
        assert_eq!(store.status().universe, 1);
        store.set_active_coin(Some("ETH".to_owned()));
        assert!(store.active_coin().is_none());
        assert!(!store.merge_active_candle("ETH", Timeframe::Hour1, candle(1)));
        assert!(store.candles("ETH", Timeframe::Hour1).is_none());
    }

    #[test]
    fn status_retains_failure_diagnostics_for_the_session() {
        let store = MarketStore::new();
        assert_eq!(store.status().storage_mode, StorageMode::Initializing);

        store.set_storage_mode(StorageMode::Temporary, Some("disk unavailable".to_owned()));
        let temporary = store.status();
        assert_eq!(temporary.storage_mode, StorageMode::Temporary);
        assert_eq!(temporary.storage_failures, 1);
        assert_eq!(temporary.last_storage_error.as_deref(), Some("disk unavailable"));
        let serialized = serde_json::to_value(&temporary).expect("serializable status");
        assert_eq!(serialized["storage_mode"], "temporary");
        assert_eq!(serialized["storage_failures"], 1);
        assert_eq!(serialized["last_storage_error"], "disk unavailable");

        store.set_storage_mode(StorageMode::Persistent, None);
        store.record_storage_failure("write failed");
        let failed = store.status();
        assert_eq!(failed.storage_failures, 2);
        assert_eq!(failed.last_storage_error.as_deref(), Some("write failed"));

        store.record_reconciliation_failure("BTC 1h is stale");
        assert_eq!(store.status().reconciliation_failures, 1);
        assert_eq!(store.status().last_reconciliation_error.as_deref(), Some("BTC 1h is stale"));
        store.clear_reconciliation_error();
        assert!(store.status().last_reconciliation_error.is_none());
    }

    #[test]
    fn contiguous_merges_flag_nothing() {
        let store = MarketStore::new();
        for hour in 0..3 {
            store.merge_candle("BTC", Timeframe::Hour1, candle(hour));
        }
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::Verified
        );
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_some());
    }

    #[test]
    fn chart_snapshots_expose_health_revisions_and_invalidations() {
        let store = MarketStore::new();
        let missing = store.candle_snapshot("BTC", Timeframe::Hour1, 0);
        assert_eq!(missing.health, CandleHealth::Loading);
        assert_eq!(missing.revision, 0);
        assert_eq!(serde_json::to_string(&CandleHealth::Ready).expect("serializable"), "\"ready\"");
        assert_eq!(
            serde_json::to_string(&CandleHealth::Verifying).expect("serializable"),
            "\"verifying\""
        );

        let mut updates = store.subscribe_candle_updates();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1), candle(2)]);
        let ready = store.candle_snapshot("BTC", Timeframe::Hour1, 2 * 3_600_000);
        assert_eq!(ready.health, CandleHealth::Ready);
        assert!(ready.revision > missing.revision);
        let stale = store.candle_snapshot("BTC", Timeframe::Hour1, 3 * 3_600_000);
        assert_eq!(stale.health, CandleHealth::Stale);
        assert_eq!(stale.revision, ready.revision);
        updates.try_recv().expect("seed invalidation");

        store.mark_streamed_for_verification(&[Timeframe::Hour1]);
        let verifying = store.candle_snapshot("BTC", Timeframe::Hour1, 2 * 3_600_000);
        assert_eq!(verifying.health, CandleHealth::Verifying);
        assert!(verifying.revision > ready.revision);
        let disconnect_update = updates.try_recv().expect("disconnect invalidation");
        assert_eq!(disconnect_update.coin, "BTC");
        assert_eq!(disconnect_update.timeframe, Timeframe::Hour1);

        let health = store.series_health("BTC", Timeframe::Hour1);
        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(1), candle(2)],
            ReconcileMode::VerifyOverlap,
            health.repair_epoch,
            health.write_revision,
        );
        let repaired = store.candle_snapshot("BTC", Timeframe::Hour1, 2 * 3_600_000);
        assert_eq!(repaired.health, CandleHealth::Ready);
        assert!(repaired.revision > verifying.revision);
        updates.try_recv().expect("REST invalidation");

        store.merge_candle("BTC", Timeframe::Hour1, candle(4));
        let repairing = store.candle_snapshot("BTC", Timeframe::Hour1, 4 * 3_600_000);
        assert_eq!(repairing.health, CandleHealth::Repairing);
        assert!(repairing.revision > repaired.revision);
        updates.try_recv().expect("structural repair invalidation");
    }

    #[test]
    fn a_gap_remains_unavailable_until_rest_reconciles_it() {
        let store = MarketStore::new();
        store.merge_candle("BTC", Timeframe::Hour1, candle(0));
        store.merge_candle("BTC", Timeframe::Hour1, candle(2));
        assert_eq!(store.series_health("BTC", Timeframe::Hour1).integrity, CandleIntegrity::Repair);
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_none());

        let health = store.series_health("BTC", Timeframe::Hour1);
        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(1), candle(2)],
            ReconcileMode::Merge,
            health.repair_epoch,
            health.write_revision,
        );
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::Verified
        );
        assert!(store.current_candles("BTC", Timeframe::Hour1, 2 * 3_600_000).is_some());
    }

    #[test]
    fn non_contiguous_backfills_require_repair() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(2)]);

        assert_eq!(store.series_health("BTC", Timeframe::Hour1).integrity, CandleIntegrity::Repair);
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
    fn disconnected_streams_remain_unavailable_until_overlap_verification() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0)]);
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);
        store.merge_candle("BTC", Timeframe::Hour1, candle(0));
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::VerifyOverlap
        );
        assert!(store.current_candles("BTC", Timeframe::Hour1, 0).is_none());

        let health = store.series_health("BTC", Timeframe::Hour1);
        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0)],
            ReconcileMode::VerifyOverlap,
            health.repair_epoch,
            health.write_revision,
        );
        assert!(store.current_candles("BTC", Timeframe::Hour1, 0).is_some());
    }

    #[test]
    fn a_verification_mark_survives_a_reconcile_that_was_planned_before_it() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1)]);
        let planned = store.series_health("BTC", Timeframe::Hour1);
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);

        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0), candle(1)],
            ReconcileMode::Replace,
            planned.repair_epoch,
            planned.write_revision,
        );
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::VerifyOverlap
        );

        let fresh = store.series_health("BTC", Timeframe::Hour1);
        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0), candle(1)],
            ReconcileMode::VerifyOverlap,
            fresh.repair_epoch,
            fresh.write_revision,
        );
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::Verified
        );
    }

    #[test]
    fn inconclusive_overlap_remains_pending_and_a_proven_gap_escalates() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1), candle(2)]);
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);
        let pending = store.series_health("BTC", Timeframe::Hour1);
        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(2)],
            ReconcileMode::VerifyOverlap,
            pending.repair_epoch,
            pending.write_revision,
        );
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::VerifyOverlap
        );

        store.set_candles("ETH".to_owned(), Timeframe::Hour1, vec![candle(0)]);
        store.mark_streamed_for_verification(&[Timeframe::Hour1]);
        let gapped = store.series_health("ETH", Timeframe::Hour1);
        store.reconcile_candles(
            "ETH".to_owned(),
            Timeframe::Hour1,
            vec![candle(2), candle(3)],
            ReconcileMode::VerifyOverlap,
            gapped.repair_epoch,
            gapped.write_revision,
        );
        assert_eq!(store.series_health("ETH", Timeframe::Hour1).integrity, CandleIntegrity::Repair);
    }

    #[test]
    fn initial_rest_backfill_preserves_a_streamed_forming_bar() {
        let store = MarketStore::new();
        let planned = store.series_health("BTC", Timeframe::Hour1);
        let mut streamed = candle(2);
        streamed.close = Decimal::TWO;
        store.merge_candle("BTC", Timeframe::Hour1, streamed);

        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0), candle(1), candle(2)],
            ReconcileMode::Replace,
            planned.repair_epoch,
            planned.write_revision,
        );
        let reconciled = store.candles("BTC", Timeframe::Hour1).expect("reconciled buffer");

        assert_eq!(reconciled.len(), 3);
        assert_eq!(reconciled.last().expect("forming bar").close, Decimal::TWO);
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::Verified
        );
    }

    #[test]
    fn rest_reconcile_updates_history_without_regressing_the_streamed_tail() {
        let store = MarketStore::new();
        store.set_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0), candle(1), candle(2), candle(3)],
        );
        let planned = store.series_health("BTC", Timeframe::Hour1);
        let mut corrected_history = candle(0);
        corrected_history.close = Decimal::from(3);
        let mut late_closed = candle(2);
        late_closed.close = Decimal::TWO;
        let mut forming = candle(3);
        forming.close = Decimal::TWO;
        store.merge_candle("BTC", Timeframe::Hour1, late_closed);
        store.merge_candle("BTC", Timeframe::Hour1, forming);

        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![corrected_history, candle(1), candle(2), candle(3), candle(4)],
            ReconcileMode::Replace,
            planned.repair_epoch,
            planned.write_revision,
        );
        let reconciled = store.candles("BTC", Timeframe::Hour1).expect("reconciled buffer");

        assert_eq!(reconciled.len(), 5);
        assert_eq!(reconciled.first().expect("history").close, Decimal::from(3));
        assert_eq!(reconciled.get(2).expect("late closed bar").close, Decimal::TWO);
        assert_eq!(reconciled.get(3).expect("streamed tail").close, Decimal::TWO);
        assert_eq!(reconciled.last().expect("new REST bar").open_time, candle(4).open_time);
    }

    #[test]
    fn rest_reconcile_fills_a_streamed_tail_gap_without_overwriting_it() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(2), candle(3)]);
        let planned = store.series_health("BTC", Timeframe::Hour1);
        let mut forming = candle(3);
        forming.close = Decimal::TWO;
        store.merge_candle("BTC", Timeframe::Hour1, forming);

        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0), candle(1), candle(2), candle(3)],
            ReconcileMode::Replace,
            planned.repair_epoch,
            planned.write_revision,
        );

        let reconciled = store.candles("BTC", Timeframe::Hour1).expect("reconciled buffer");
        assert_eq!(reconciled.len(), 4);
        assert_eq!(reconciled.last().expect("forming bar").close, Decimal::TWO);
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::Verified
        );
    }

    #[test]
    fn sparse_reconcile_provisions_only_the_current_flat_forming_bar() {
        let store = MarketStore::new();
        let hour = Timeframe::Hour1.duration_ms();
        let mut previous = candle(1);
        previous.close = Decimal::TWO;
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), previous]);

        assert!(store.provision_forming_candle_after_reconcile("BTC", Timeframe::Hour1, 2 * hour));
        let candles = store.candles("BTC", Timeframe::Hour1).expect("provisioned buffer");
        let forming = candles.last().expect("provisional forming bar");
        assert_eq!(forming.open_time.timestamp_millis(), 2 * hour);
        assert_eq!(forming.open, Decimal::TWO);
        assert_eq!(forming.high, Decimal::TWO);
        assert_eq!(forming.low, Decimal::TWO);
        assert_eq!(forming.close, Decimal::TWO);
        assert_eq!(forming.volume, Decimal::ZERO);
        assert_eq!(completed_prefix_len(&candles, Timeframe::Hour1, 2 * hour + 1), 2);
        assert!(!store.provision_forming_candle_after_reconcile("BTC", Timeframe::Hour1, 2 * hour));

        store.set_candles("ETH".to_owned(), Timeframe::Hour1, vec![candle(0)]);
        assert!(!store.provision_forming_candle_after_reconcile("ETH", Timeframe::Hour1, 2 * hour));

        store.set_candles("SOL".to_owned(), Timeframe::Hour1, vec![candle(0), candle(2)]);
        assert!(!store.provision_forming_candle_after_reconcile("SOL", Timeframe::Hour1, 3 * hour));
    }

    #[test]
    fn provisional_candle_uses_a_stream_correction_that_arrived_during_rest() {
        let store = MarketStore::new();
        let hour = Timeframe::Hour1.duration_ms();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1)]);
        let planned = store.series_health("BTC", Timeframe::Hour1);
        let mut corrected = candle(1);
        corrected.close = Decimal::TWO;
        store.merge_candle("BTC", Timeframe::Hour1, corrected);

        store.reconcile_candles(
            "BTC".to_owned(),
            Timeframe::Hour1,
            vec![candle(0), candle(1)],
            ReconcileMode::Merge,
            planned.repair_epoch,
            planned.write_revision,
        );
        assert!(store.provision_forming_candle_after_reconcile("BTC", Timeframe::Hour1, 2 * hour));

        let candles = store.candles("BTC", Timeframe::Hour1).expect("provisioned buffer");
        assert_eq!(candles.last().expect("forming bar").close, Decimal::TWO);
    }

    #[test]
    fn late_final_update_of_a_recently_closed_bar_replaces_it() {
        let store = MarketStore::new();
        store.merge_candle("BTC", Timeframe::Hour1, candle(0));
        store.merge_candle("BTC", Timeframe::Hour1, candle(1));
        let mut late = candle(0);
        late.close = Decimal::TWO;

        store.merge_candle("BTC", Timeframe::Hour1, late);

        let candles = store.candles("BTC", Timeframe::Hour1).expect("buffer");
        assert_eq!(candles.first().expect("closed bar").close, Decimal::TWO);
        assert_eq!(
            store.series_health("BTC", Timeframe::Hour1).integrity,
            CandleIntegrity::Verified
        );
    }

    #[test]
    fn updates_older_than_the_replace_window_are_ignored() {
        let store = MarketStore::new();
        for hour in 0..5 {
            store.merge_candle("BTC", Timeframe::Hour1, candle(hour));
        }
        let mut stale = candle(0);
        stale.close = Decimal::TWO;

        store.merge_candle("BTC", Timeframe::Hour1, stale);

        let candles = store.candles("BTC", Timeframe::Hour1).expect("buffer");
        assert_eq!(candles.first().expect("old bar").close, Decimal::ONE);
    }

    #[test]
    fn a_buffer_one_bar_ahead_of_now_is_current() {
        let store = MarketStore::new();
        store.set_candles("BTC".to_owned(), Timeframe::Hour1, vec![candle(0), candle(1)]);

        assert!(store.current_candles("BTC", Timeframe::Hour1, 30 * 60_000).is_some());
        assert!(store.current_candles("BTC", Timeframe::Hour1, 3 * 3_600_000).is_none());
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
