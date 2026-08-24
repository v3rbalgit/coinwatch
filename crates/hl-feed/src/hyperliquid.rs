//! Hyperliquid market-data adapter over the public WebSocket.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
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

    let subscribe = serde_json::to_string(&Subscribe {
        method: "subscribe",
        subscription: Subscription::AllMids,
    })?;
    write.send(Message::Text(subscribe)).await?;

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
    let Ok(WsMessage::AllMids { data }) = serde_json::from_str::<WsMessage>(text) else {
        return;
    };
    let timestamp = Utc::now();
    for (symbol, price) in data.mids {
        let Ok(price) = price.parse::<Decimal>() else { continue };
        feed.publish(MarketEvent::Mid(MidEvent { symbol, price, timestamp }));
    }
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
}

#[derive(Deserialize)]
#[serde(tag = "channel")]
enum WsMessage {
    #[serde(rename = "allMids")]
    AllMids { data: AllMidsData },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct AllMidsData {
    mids: HashMap<String, String>,
}
