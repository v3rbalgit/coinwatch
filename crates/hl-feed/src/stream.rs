//! The Hyperliquid WebSocket adapter: one session streaming prices and candles.
//!
//! Subscribes to `fastAssetCtxs` for canonical mark prices and reconciles a
//! bounded global candle set plus the active chart series. Marks publish as
//! [`MarketEvent::Mark`] and candles as [`MarketEvent::Candle`] onto the shared
//! [`MarketFeed`]; candles are also merged into the [`MarketStore`] for reads.
//! Reconnects with exponential backoff, re-subscribing each session.

use std::collections::{BTreeSet, HashMap};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use domain::{Candle, Timeframe};
use flate2::read::DeflateDecoder;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::error::FeedError;
use crate::event::{CandleEvent, MarkPriceEvent, MarketEvent};
use crate::feed::MarketFeed;
use crate::market_store::{ConnectionState, MarketStore};
use crate::rest::to_decimal;

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const PING_INTERVAL: Duration = Duration::from_secs(15);
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
/// A session at least this old failed on its own, not as part of a failure streak.
const STABLE_SESSION: Duration = Duration::from_secs(60);
const UNIVERSE_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const WS_SUBSCRIPTION_LIMIT: usize = 1_000;
const BASE_SUBSCRIPTIONS: [Subscription; 1] = [Subscription::FastAssetCtxs];
const BASE_SUBSCRIPTION_COUNT: usize = BASE_SUBSCRIPTIONS.len();

/// Streams live Hyperliquid market data (prices + candles) into a shared
/// [`MarketFeed`] and [`MarketStore`]. Construct, take a [`feed`](Self::feed) to
/// subscribe on, then [`connect`](Self::connect). The task reconnects on its own;
/// requires a process-wide rustls crypto provider (installed by the binary).
pub struct HyperliquidAdapter {
    feed: MarketFeed,
    store: Arc<MarketStore>,
    candle_intervals: Vec<Timeframe>,
    task: Option<JoinHandle<()>>,
}

impl HyperliquidAdapter {
    /// Create an adapter that streams candles for `candle_intervals` and merges
    /// them into `store`.
    #[must_use]
    pub fn new(store: Arc<MarketStore>, candle_intervals: Vec<Timeframe>) -> Self {
        Self {
            feed: MarketFeed::new(),
            store,
            candle_intervals,
            task: None,
        }
    }

    /// The shared feed to subscribe on.
    #[must_use]
    pub fn feed(&self) -> MarketFeed {
        self.feed.clone()
    }

    /// Spawn the streaming task; a no-op while already connected.
    pub fn connect(&mut self) {
        if self.task.is_some() {
            return;
        }
        let feed = self.feed.clone();
        let store = Arc::clone(&self.store);
        let intervals = self.candle_intervals.clone();
        self.task = Some(tokio::spawn(ws_loop(feed, store, intervals)));
    }

    /// Stop the streaming task; a later [`connect`](Self::connect) restarts it.
    ///
    /// This bypasses session cleanup and therefore cannot yet update connection
    /// or candle-health state consistently. Keep it unused until a disconnected
    /// state transition is modeled.
    #[deprecated(note = "disconnect does not update connection or candle-health state")]
    pub fn disconnect(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn ws_loop(feed: MarketFeed, store: Arc<MarketStore>, intervals: Vec<Timeframe>) {
    let mut backoff = RECONNECT_BASE;
    loop {
        let started = Instant::now();
        let mut candle_subscriptions = BTreeSet::new();
        let result = run_session(&feed, &store, &intervals, &mut candle_subscriptions).await;
        let session_was_live = store.status().connection == ConnectionState::Live;
        let capacity_exceeded = matches!(&result, Err(FeedError::SubscriptionLimit { .. }));
        match result {
            Ok(()) => backoff = RECONNECT_BASE,
            Err(error) => {
                tracing::warn!(%error, ?backoff, "websocket session ended, reconnecting");
                backoff = if started.elapsed() >= STABLE_SESSION {
                    RECONNECT_BASE
                } else {
                    (backoff * 2).min(RECONNECT_MAX)
                };
            }
        }
        if session_was_live {
            store.mark_series_for_verification(&subscription_series(&candle_subscriptions));
        }
        store.set_streaming(0);
        store.set_connection(ConnectionState::Reconnecting);
        if capacity_exceeded {
            tracing::error!("websocket reconnect paused until subscription capacity is available");
            wait_for_subscription_capacity(&store, &intervals).await;
            backoff = RECONNECT_BASE;
        } else {
            tokio::time::sleep(backoff).await;
        }
    }
}

async fn wait_for_subscription_capacity(store: &MarketStore, intervals: &[Timeframe]) {
    loop {
        let desired = desired_candle_subscriptions(store, intervals);
        if validate_subscription_capacity(desired.len()).is_ok() {
            return;
        }
        tokio::time::sleep(UNIVERSE_CHECK_INTERVAL).await;
    }
}

async fn run_session(
    feed: &MarketFeed,
    store: &MarketStore,
    intervals: &[Timeframe],
    candle_subscriptions: &mut BTreeSet<Subscription>,
) -> Result<(), FeedError> {
    let (stream, _) = connect_async(WS_URL).await?;
    let (mut write, mut read) = stream.split();
    candle_subscriptions.clear();
    let desired = desired_candle_subscriptions(store, intervals);
    let (_, initial_subscriptions) = candle_subscription_changes(candle_subscriptions, &desired)?;

    for subscription in &BASE_SUBSCRIPTIONS {
        let payload = subscription_payload(SubscriptionMethod::Subscribe, subscription)?;
        write.send(Message::Text(payload.into())).await?;
    }
    for subscription in initial_subscriptions {
        let payload = subscription_payload(SubscriptionMethod::Subscribe, &subscription)?;
        write.send(Message::Text(payload.into())).await?;
        candle_subscriptions.insert(subscription);
    }
    store.set_streaming(candle_subscriptions.len());
    store.set_connection(ConnectionState::Live);
    tracing::info!(streams = candle_subscriptions.len(), "websocket connected");

    let mut ping = tokio::time::interval(PING_INTERVAL);
    let mut universe_check = tokio::time::interval(UNIVERSE_CHECK_INTERVAL);

    loop {
        tokio::select! {
            frame = read.next() => {
                let Some(frame) = frame else { return Ok(()) };
                match frame? {
                    Message::Text(text) => publish_frame(&text, feed, store, intervals),
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            _ = ping.tick() => {
                let ping = serde_json::json!({ "method": "ping" }).to_string();
                write.send(Message::Text(ping.into())).await?;
            }
            _ = universe_check.tick() => {
                let desired = desired_candle_subscriptions(store, intervals);
                let (removed, added) = candle_subscription_changes(candle_subscriptions, &desired)?;
                if removed.is_empty() && added.is_empty() {
                    continue;
                }
                let removed_count = removed.len();
                let added_count = added.len();
                for subscription in removed {
                    let payload = subscription_payload(SubscriptionMethod::Unsubscribe, &subscription)?;
                    write.send(Message::Text(payload.into())).await?;
                    candle_subscriptions.remove(&subscription);
                    if let Some((coin, timeframe)) = subscription_series_key(&subscription)
                        && !intervals.contains(&timeframe)
                    {
                        store.remove_unsubscribed_tail(&coin, timeframe, intervals);
                    }
                }
                for subscription in added {
                    let payload = subscription_payload(SubscriptionMethod::Subscribe, &subscription)?;
                    write.send(Message::Text(payload.into())).await?;
                    candle_subscriptions.insert(subscription);
                }
                store.set_streaming(candle_subscriptions.len());
                tracing::debug!(
                    added = added_count,
                    removed = removed_count,
                    streams = candle_subscriptions.len(),
                    "reconciled candle streams"
                );
            }
        }
    }
}

fn desired_candle_subscriptions(store: &MarketStore, intervals: &[Timeframe]) -> BTreeSet<Subscription> {
    let universe = store.universe();
    let mut desired: BTreeSet<_> = universe
        .iter()
        .flat_map(|coin| {
            intervals.iter().map(move |interval| Subscription::Candle {
                coin: coin.coin.clone(),
                interval: interval.as_hl().to_owned(),
            })
        })
        .collect();
    if let Some(active) = store.active_series()
        && universe.iter().any(|market| market.coin == active.coin)
    {
        desired.insert(Subscription::Candle {
            coin: active.coin,
            interval: active.timeframe.as_hl().to_owned(),
        });
    }
    desired
}

fn subscription_series(subscriptions: &BTreeSet<Subscription>) -> Vec<(String, Timeframe)> {
    subscriptions.iter().filter_map(subscription_series_key).collect()
}

fn subscription_series_key(subscription: &Subscription) -> Option<(String, Timeframe)> {
    let Subscription::Candle { coin, interval } = subscription else {
        return None;
    };
    Timeframe::from_hl(interval).map(|timeframe| (coin.clone(), timeframe))
}

fn candle_subscription_changes(
    current: &BTreeSet<Subscription>,
    desired: &BTreeSet<Subscription>,
) -> Result<(Vec<Subscription>, Vec<Subscription>), FeedError> {
    validate_subscription_capacity(desired.len())?;
    let removed = current.difference(desired).cloned().collect();
    let added = desired.difference(current).cloned().collect();
    Ok((removed, added))
}

const fn validate_subscription_capacity(candle_subscriptions: usize) -> Result<(), FeedError> {
    let requested = candle_subscriptions.saturating_add(BASE_SUBSCRIPTION_COUNT);
    if requested > WS_SUBSCRIPTION_LIMIT {
        return Err(FeedError::SubscriptionLimit {
            requested,
            limit: WS_SUBSCRIPTION_LIMIT,
        });
    }
    Ok(())
}

fn subscription_payload(method: SubscriptionMethod, subscription: &Subscription) -> Result<String, serde_json::Error> {
    serde_json::to_string(&SubscriptionRequest { method, subscription })
}

fn publish_frame(text: &str, feed: &MarketFeed, store: &MarketStore, global_timeframes: &[Timeframe]) {
    let message = match serde_json::from_str::<WsMessage>(text) {
        Ok(message) => message,
        Err(error) => {
            tracing::debug!(%error, "unparseable websocket frame");
            return;
        }
    };
    let timestamp = Utc::now();
    match message {
        WsMessage::FastAssetCtxs { data } => {
            let Some(prices) = decode_fast_asset_ctxs(&data) else {
                tracing::debug!("failed to decode fastAssetCtxs frame");
                return;
            };
            for (symbol, ctx) in prices {
                let Some(raw) = ctx.mark_px else {
                    continue;
                };
                if let Ok(price) = raw.parse::<Decimal>() {
                    feed.publish(MarketEvent::Mark(MarkPriceEvent {
                        symbol,
                        price,
                        timestamp,
                    }));
                }
            }
        }
        WsMessage::Candle { data } => {
            if let Some(event) = candle_event(&data)
                && store.merge_subscribed_candle(&event.coin, event.timeframe, event.candle.clone(), global_timeframes)
            {
                feed.publish(MarketEvent::Candle(event));
            }
        }
        WsMessage::Other => {}
    }
}

fn candle_event(data: &WsCandle) -> Option<CandleEvent> {
    let timeframe = Timeframe::from_hl(&data.i)?;
    let open_time = DateTime::from_timestamp_millis(data.t)?;
    Some(CandleEvent {
        coin: data.s.clone(),
        timeframe,
        candle: Candle {
            open_time,
            open: to_decimal(&data.o).ok()?,
            high: to_decimal(&data.h).ok()?,
            low: to_decimal(&data.l).ok()?,
            close: to_decimal(&data.c).ok()?,
            volume: to_decimal(&data.v).ok()?,
        },
    })
}

/// Decode a `fastAssetCtxs` frame: base64 of a raw deflate stream (no zlib
/// wrapper) holding mark updates for only the coins that moved.
fn decode_fast_asset_ctxs(data: &str) -> Option<HashMap<String, FastPrices>> {
    let compressed = BASE64.decode(data).ok()?;
    let mut json = Vec::new();
    DeflateDecoder::new(compressed.as_slice()).read_to_end(&mut json).ok()?;
    serde_json::from_slice(&json).ok()
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    method: SubscriptionMethod,
    subscription: &'a Subscription,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum SubscriptionMethod {
    Subscribe,
    Unsubscribe,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Subscription {
    FastAssetCtxs,
    Candle { coin: String, interval: String },
}

#[derive(Deserialize)]
#[serde(tag = "channel")]
enum WsMessage {
    #[serde(rename = "fastAssetCtxs")]
    FastAssetCtxs { data: String },
    #[serde(rename = "candle")]
    Candle { data: Box<WsCandle> },
    #[serde(other)]
    Other,
}

/// Mark prices from a `fastAssetCtxs` frame; omitted fields did not change.
#[derive(Deserialize)]
struct FastPrices {
    #[serde(rename = "markPx")]
    mark_px: Option<String>,
}

/// A Hyperliquid WS candle payload (`o`/`h`/`l`/`c`/`v` arrive as JSON strings).
#[derive(Deserialize)]
struct WsCandle {
    t: i64,
    s: String,
    i: String,
    o: serde_json::Value,
    h: serde_json::Value,
    l: serde_json::Value,
    c: serde_json::Value,
    v: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use domain::Timeframe;

    use super::{
        BASE_SUBSCRIPTION_COUNT, Subscription, SubscriptionMethod, UNIVERSE_CHECK_INTERVAL, WS_SUBSCRIPTION_LIMIT,
        candle_subscription_changes, decode_fast_asset_ctxs, desired_candle_subscriptions, subscription_payload,
        validate_subscription_capacity, wait_for_subscription_capacity,
    };
    use crate::error::FeedError;
    use crate::{ActiveSeries, MarketStore, UniverseCoin};

    fn candle_subscription(coin: &str, timeframe: Timeframe) -> Subscription {
        Subscription::Candle {
            coin: coin.to_owned(),
            interval: timeframe.as_hl().to_owned(),
        }
    }

    fn universe(count: usize) -> Vec<UniverseCoin> {
        (0..count)
            .map(|index| UniverseCoin {
                coin: format!("COIN{index}"),
                day_volume_usd: 0.0,
                mark_px: None,
                price_decimals: 0,
                prev_day_px: None,
                funding: None,
                open_interest: None,
            })
            .collect()
    }

    #[test]
    fn decodes_fast_asset_ctxs_frame() {
        let data = "q1ZyCnFWsqpWyk0syg6oULJSMjM1NjHQM1DSUcrNTIGJGFvqmSrV6ii5hniAFUNlDC3NLPRMQBLB/j4oppib6ZlagCQqKqusHB0DfJC1GRtb6IElHSyNUHQZ6BkamSPZDOYbgVS6+Lu7IhmRV5qTowPXB+LV1gIA";
        let prices = decode_fast_asset_ctxs(data).expect("valid frame");
        let btc = prices.get("BTC").expect("BTC present");
        assert_eq!(btc.mark_px.as_deref(), Some("65340.0"));
        let doge = prices.get("DOGE").expect("DOGE present");
        assert!(doge.mark_px.is_none());
    }

    #[test]
    fn candle_subscription_changes_follow_universe_additions_and_removals() {
        let current = BTreeSet::from([
            candle_subscription("BTC", Timeframe::Hour1),
            candle_subscription("ETH", Timeframe::Hour1),
        ]);
        let desired = BTreeSet::from([
            candle_subscription("ETH", Timeframe::Hour1),
            candle_subscription("SOL", Timeframe::Hour1),
        ]);

        let (removed, added) = candle_subscription_changes(&current, &desired).expect("capacity available");

        assert_eq!(removed, vec![candle_subscription("BTC", Timeframe::Hour1)]);
        assert_eq!(added, vec![candle_subscription("SOL", Timeframe::Hour1)]);
    }

    #[test]
    fn desired_subscriptions_add_only_the_active_non_global_series() {
        let store = MarketStore::new();
        store.set_universe(universe(2));
        store.set_active_series(Some(ActiveSeries {
            coin: "COIN0".to_owned(),
            timeframe: Timeframe::Day1,
        }));

        let first = desired_candle_subscriptions(&store, &[Timeframe::Hour1]);
        assert_eq!(first.len(), 3);
        assert!(first.contains(&candle_subscription("COIN0", Timeframe::Day1)));
        assert!(!first.contains(&candle_subscription("COIN1", Timeframe::Day1)));

        store.set_active_series(Some(ActiveSeries {
            coin: "COIN1".to_owned(),
            timeframe: Timeframe::Week1,
        }));
        let second = desired_candle_subscriptions(&store, &[Timeframe::Hour1]);
        let (removed, added) = candle_subscription_changes(&first, &second).expect("capacity available");
        assert_eq!(removed, vec![candle_subscription("COIN0", Timeframe::Day1)]);
        assert_eq!(added, vec![candle_subscription("COIN1", Timeframe::Week1)]);

        store.set_active_series(None);
        let cleared = desired_candle_subscriptions(&store, &[Timeframe::Hour1]);
        assert_eq!(cleared.len(), 2);
    }

    #[test]
    fn a_new_session_resubscribes_the_complete_desired_set() {
        let desired = BTreeSet::from([
            candle_subscription("BTC", Timeframe::Hour1),
            candle_subscription("BTC", Timeframe::Hour4),
        ]);

        let (removed, first_session) =
            candle_subscription_changes(&BTreeSet::new(), &desired).expect("capacity available");
        let (_, reconnected_session) =
            candle_subscription_changes(&BTreeSet::new(), &desired).expect("capacity available");

        assert!(removed.is_empty());
        assert_eq!(first_session, desired.iter().cloned().collect::<Vec<_>>());
        assert_eq!(reconnected_session, first_session);
    }

    #[test]
    fn subscription_capacity_is_rejected_before_building_requests() {
        let candle_capacity = WS_SUBSCRIPTION_LIMIT - BASE_SUBSCRIPTION_COUNT;
        assert!(validate_subscription_capacity(candle_capacity).is_ok());
        let desired: BTreeSet<Subscription> = (0..=candle_capacity)
            .map(|index| candle_subscription(&format!("COIN{index}"), Timeframe::Hour1))
            .collect();

        let error = candle_subscription_changes(&BTreeSet::new(), &desired)
            .expect_err("capacity must be enforced before returning additions");

        assert!(matches!(
            error,
            FeedError::SubscriptionLimit {
                requested,
                limit: WS_SUBSCRIPTION_LIMIT,
            } if requested == WS_SUBSCRIPTION_LIMIT + 1
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_pause_resumes_after_the_universe_fits() {
        let candle_capacity = WS_SUBSCRIPTION_LIMIT - BASE_SUBSCRIPTION_COUNT;
        let store = MarketStore::new();
        store.set_universe(universe(candle_capacity + 1));
        let wait_store = Arc::clone(&store);
        let waiting = tokio::spawn(async move {
            wait_for_subscription_capacity(&wait_store, &[Timeframe::Hour1]).await;
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        store.set_universe(universe(candle_capacity));
        tokio::time::advance(UNIVERSE_CHECK_INTERVAL).await;

        waiting.await.expect("capacity wait task completes");
    }

    #[test]
    fn unsubscribe_payload_matches_the_original_subscription() {
        let subscription = candle_subscription("BTC", Timeframe::Hour1);
        let payload =
            subscription_payload(SubscriptionMethod::Unsubscribe, &subscription).expect("serializable subscription");
        let value: serde_json::Value = serde_json::from_str(&payload).expect("valid JSON");

        assert_eq!(
            value,
            serde_json::json!({
                "method": "unsubscribe",
                "subscription": { "type": "candle", "coin": "BTC", "interval": "1h" }
            })
        );
    }
}
