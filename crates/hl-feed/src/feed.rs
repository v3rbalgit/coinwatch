//! Fan-out event distribution: per-symbol and global broadcast channels.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Notify, broadcast};

use crate::event::MarketEvent;

const CHANNEL_CAPACITY: usize = 256;
const GLOBAL_CHANNEL_CAPACITY: usize = 4096;

/// Distributes [`MarketEvent`]s to consumers via a per-symbol channel and a global
/// channel. Events are shared as `Arc` so a fan-out never copies the payload. Cheap
/// to clone — every field is behind an `Arc` — so adapters and consumers hold the
/// same feed.
#[derive(Clone)]
pub struct MarketFeed {
    channels: Arc<DashMap<String, broadcast::Sender<Arc<MarketEvent>>>>,
    global: broadcast::Sender<Arc<MarketEvent>>,
    notify: Arc<Notify>,
}

impl MarketFeed {
    /// Create an empty feed.
    #[must_use]
    pub fn new() -> Self {
        let (global, _) = broadcast::channel(GLOBAL_CHANNEL_CAPACITY);
        Self {
            channels: Arc::new(DashMap::new()),
            global,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Subscribe to events for a single symbol, creating its channel on first use.
    #[must_use]
    pub fn subscribe(&self, symbol: &str) -> broadcast::Receiver<Arc<MarketEvent>> {
        self.channels
            .entry(symbol.to_owned())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .value()
            .subscribe()
    }

    /// Subscribe to every event across all symbols.
    #[must_use]
    pub fn subscribe_all(&self) -> broadcast::Receiver<Arc<MarketEvent>> {
        self.global.subscribe()
    }

    /// A handle pulsed on every published event, for change-driven consumers that
    /// prefer to poll shared state rather than drain a channel.
    #[must_use]
    pub fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// Publish an event to its per-symbol channel (only when it has receivers, to
    /// skip the `Arc` bump) and to the global channel, then pulse the notify.
    pub fn publish(&self, event: MarketEvent) {
        let event = Arc::new(event);
        if let Some(tx) = self.channels.get(event.symbol())
            && tx.receiver_count() > 0
        {
            let _ = tx.send(Arc::clone(&event));
        }
        let _ = self.global.send(event);
        self.notify.notify_waiters();
    }
}

impl Default for MarketFeed {
    fn default() -> Self {
        Self::new()
    }
}
