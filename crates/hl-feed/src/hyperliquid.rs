//! Hyperliquid market-data adapter over the public WebSocket.

use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use flate2::read::DeflateDecoder;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::event::{MarketEvent, MidEvent};
use crate::feed::MarketFeed;

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const PING_INTERVAL: Duration = Duration::from_secs(15);
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// Streams live Hyperliquid market data into a shared [`MarketFeed`]. Construct,
/// take a [`feed`](Self::feed) to subscribe on, then [`connect`](Self::connect) to
/// spawn the streaming task. The task is detached and reconnects with exponential
/// backoff, so the adapter may be dropped once connected; [`disconnect`](Self::disconnect)
/// stops it. Requires a process-wide rustls crypto provider (installed by the binary).
pub struct HyperliquidAdapter {
    feed: MarketFeed,
    task: Option<JoinHandle<()>>,
}

impl HyperliquidAdapter {
    /// Create an adapter over a fresh feed.
    #[must_use]
    pub fn new() -> Self {
        Self { feed: MarketFeed::new(), task: None }
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
        self.task = Some(tokio::spawn(ws_loop(self.feed.clone())));
    }

    /// Stop the streaming task; a later [`connect`](Self::connect) restarts it.
    pub fn disconnect(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Default for HyperliquidAdapter {
    fn default() -> Self {
        Self::new()
    }
}

type SessionError = Box<dyn std::error::Error + Send + Sync>;

async fn ws_loop(feed: MarketFeed) {
    let mut backoff = RECONNECT_BASE;
    loop {
        match run_session(&feed).await {
            Ok(()) => backoff = RECONNECT_BASE,
            Err(_) => backoff = (backoff * 2).min(RECONNECT_MAX),
        }
        tokio::time::sleep(backoff).await;
    }
}

async fn run_session(feed: &MarketFeed) -> Result<(), SessionError> {
    let (stream, _) = connect_async(WS_URL).await?;
    let (mut write, mut read) = stream.split();

    // allMids gives every coin a price on a ~5s cadence; fastAssetCtxs pushes only
    // what moved on a ~1s cadence, so prices stay near real-time.
    for subscription in [Subscription::AllMids, Subscription::FastAssetCtxs] {
        let subscribe = serde_json::to_string(&Subscribe { method: "subscribe", subscription })?;
        write.send(Message::Text(subscribe)).await?;
    }

    let mut ping = tokio::time::interval(PING_INTERVAL);
    loop {
        tokio::select! {
            frame = read.next() => {
                let Some(frame) = frame else { return Ok(()) };
                match frame? {
                    Message::Text(text) => publish_frame(&text, feed),
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            _ = ping.tick() => {
                let ping = serde_json::json!({ "method": "ping" }).to_string();
                write.send(Message::Text(ping)).await?;
            }
        }
    }
}

/// Deserialize one text frame and publish the events it carries. Unrecognized
/// channels (subscription acks, pongs) parse to [`WsMessage::Other`] and are
/// ignored; a malformed frame is dropped rather than dropping the connection.
fn publish_frame(text: &str, feed: &MarketFeed) {
    let Ok(message) = serde_json::from_str::<WsMessage>(text) else {
        return;
    };
    let timestamp = Utc::now();
    match message {
        WsMessage::AllMids { data } => {
            for (symbol, price) in data.mids {
                if let Ok(price) = price.parse::<Decimal>() {
                    feed.publish(MarketEvent::Mid(MidEvent { symbol, price, timestamp }));
                }
            }
        }
        WsMessage::FastAssetCtxs { data } => {
            let Some(prices) = decode_fast_asset_ctxs(&data) else { return };
            for (symbol, ctx) in prices {
                let Some(raw) = ctx.mark_px.or(ctx.mid_px) else { continue };
                if let Ok(price) = raw.parse::<Decimal>() {
                    feed.publish(MarketEvent::Mid(MidEvent { symbol, price, timestamp }));
                }
            }
        }
        WsMessage::Other => {}
    }
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
}

#[derive(Deserialize)]
#[serde(tag = "channel")]
enum WsMessage {
    #[serde(rename = "allMids")]
    AllMids { data: AllMidsData },
    #[serde(rename = "fastAssetCtxs")]
    FastAssetCtxs { data: String },
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
