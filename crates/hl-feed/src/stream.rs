//! The Hyperliquid WebSocket adapter: one session streaming prices and candles.
//!
//! Subscribes to `allMids` + `fastAssetCtxs` (prices) and, once the universe is
//! known, `candle` streams for the configured timeframes. Prices publish as
//! [`MarketEvent::Mid`] and candles as [`MarketEvent::Candle`] onto the shared
//! [`MarketFeed`]; candles are also merged into the [`MarketStore`] for reads.
//! Reconnects with exponential backoff, re-subscribing each session.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

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
use crate::event::{CandleEvent, MarketEvent, MidEvent};
use crate::feed::MarketFeed;
use crate::rest::to_decimal;
use crate::store::{ConnectionState, MarketStore};

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const PING_INTERVAL: Duration = Duration::from_secs(15);
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const UNIVERSE_CHECK_INTERVAL: Duration = Duration::from_millis(500);

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
    pub fn disconnect(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn ws_loop(feed: MarketFeed, store: Arc<MarketStore>, intervals: Vec<Timeframe>) {
    let mut backoff = RECONNECT_BASE;
    loop {
        let result = run_session(&feed, &store, &intervals).await;
        let session_was_live = store.status().connection == ConnectionState::Live;
        match result {
            Ok(()) => backoff = RECONNECT_BASE,
            Err(error) => {
                tracing::warn!(%error, ?backoff, "websocket session ended, reconnecting");
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        }
        if session_was_live {
            store.mark_streamed_for_reconciliation(&intervals);
        }
        store.set_connection(ConnectionState::Reconnecting);
        tokio::time::sleep(backoff).await;
    }
}

async fn run_session(feed: &MarketFeed, store: &MarketStore, intervals: &[Timeframe]) -> Result<(), FeedError> {
    let (stream, _) = connect_async(WS_URL).await?;
    let (mut write, mut read) = stream.split();
    store.set_connection(ConnectionState::Live);
    tracing::info!("websocket connected");

    for subscription in [Subscription::AllMids, Subscription::FastAssetCtxs] {
        let subscribe = serde_json::to_string(&Subscribe {
            method: "subscribe",
            subscription,
        })?;
        write.send(Message::Text(subscribe.into())).await?;
    }

    let mut ping = tokio::time::interval(PING_INTERVAL);
    let mut universe_check = tokio::time::interval(UNIVERSE_CHECK_INTERVAL);
    let mut candles_subscribed = false;

    loop {
        tokio::select! {
            frame = read.next() => {
                let Some(frame) = frame else { return Ok(()) };
                match frame? {
                    Message::Text(text) => publish_frame(&text, feed, store),
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            _ = ping.tick() => {
                let ping = serde_json::json!({ "method": "ping" }).to_string();
                write.send(Message::Text(ping.into())).await?;
            }
            _ = universe_check.tick(), if !candles_subscribed => {
                let coins = store.universe();
                if !coins.is_empty() {
                    for coin in &coins {
                        for &interval in intervals {
                            let subscription = Subscription::Candle {
                                coin: coin.coin.clone(),
                                interval: interval.as_hl().to_owned(),
                            };
                            let subscribe = serde_json::to_string(&Subscribe { method: "subscribe", subscription })?;
                            write.send(Message::Text(subscribe.into())).await?;
                        }
                    }
                    let streams = coins.len().saturating_mul(intervals.len());
                    candles_subscribed = true;
                    store.set_streaming(streams);
                    tracing::debug!(streams, "subscribed candle streams");
                }
            }
        }
    }
}

fn publish_frame(text: &str, feed: &MarketFeed, store: &MarketStore) {
    let message = match serde_json::from_str::<WsMessage>(text) {
        Ok(message) => message,
        Err(error) => {
            tracing::debug!(%error, "unparseable websocket frame");
            return;
        }
    };
    let timestamp = Utc::now();
    match message {
        WsMessage::AllMids { data } => {
            for (symbol, price) in data.mids {
                if let Ok(price) = price.parse::<Decimal>() {
                    feed.publish(MarketEvent::Mid(MidEvent {
                        symbol,
                        price,
                        timestamp,
                    }));
                }
            }
        }
        WsMessage::FastAssetCtxs { data } => {
            let Some(prices) = decode_fast_asset_ctxs(&data) else {
                tracing::debug!("failed to decode fastAssetCtxs frame");
                return;
            };
            for (symbol, ctx) in prices {
                let Some(raw) = ctx.mark_px.or(ctx.mid_px) else {
                    continue;
                };
                if let Ok(price) = raw.parse::<Decimal>() {
                    feed.publish(MarketEvent::Mid(MidEvent {
                        symbol,
                        price,
                        timestamp,
                    }));
                }
            }
        }
        WsMessage::Candle { data } => {
            if let Some(event) = candle_event(&data) {
                store.merge_candle(&event.coin, event.timeframe, event.candle.clone());
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
/// wrapper) holding a coin -> mark/mid map for only the coins that moved.
fn decode_fast_asset_ctxs(data: &str) -> Option<HashMap<String, FastPrices>> {
    let compressed = BASE64.decode(data).ok()?;
    let mut json = Vec::new();
    DeflateDecoder::new(compressed.as_slice()).read_to_end(&mut json).ok()?;
    serde_json::from_slice(&json).ok()
}

#[derive(Serialize)]
struct Subscribe {
    method: &'static str,
    subscription: Subscription,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Subscription {
    AllMids,
    FastAssetCtxs,
    Candle { coin: String, interval: String },
}

#[derive(Deserialize)]
#[serde(tag = "channel")]
enum WsMessage {
    #[serde(rename = "allMids")]
    AllMids { data: AllMidsData },
    #[serde(rename = "fastAssetCtxs")]
    FastAssetCtxs { data: String },
    #[serde(rename = "candle")]
    Candle { data: Box<WsCandle> },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct AllMidsData {
    mids: HashMap<String, String>,
}

/// Mark/mid prices from a `fastAssetCtxs` frame; both optional and nullable.
#[derive(Deserialize)]
struct FastPrices {
    #[serde(rename = "markPx")]
    mark_px: Option<String>,
    #[serde(rename = "midPx")]
    mid_px: Option<String>,
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
    use super::decode_fast_asset_ctxs;

    #[test]
    fn decodes_fast_asset_ctxs_frame() {
        let data = "q1ZyCnFWsqpWyk0syg6oULJSMjM1NjHQM1DSUcrNTIGJGFvqmSrV6ii5hniAFUNlDC3NLPRMQBLB/j4oppib6ZlagCQqKqusHB0DfJC1GRtb6IElHSyNUHQZ6BkamSPZDOYbgVS6+Lu7IhmRV5qTowPXB+LV1gIA";
        let prices = decode_fast_asset_ctxs(data).expect("valid frame");
        let btc = prices.get("BTC").expect("BTC present");
        assert_eq!(btc.mark_px.as_deref(), Some("65340.0"));
        assert_eq!(btc.mid_px.as_deref(), Some("65339.5"));
        let doge = prices.get("DOGE").expect("DOGE present");
        assert!(doge.mark_px.is_none() && doge.mid_px.is_none());
    }
}
