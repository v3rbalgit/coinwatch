//! Canonical domain vocabulary shared by every crate.
//!
//! All money is [`rust_decimal::Decimal`] (fixed-point, no binary-float drift).
//! All time is UTC [`chrono::DateTime`].

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Supported chains. `Other` keeps ingestion resilient to new networks without
/// a code change; adapters decide what they actually support.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Network {
    Base,
    Arbitrum,
    Ethereum,
    Other(String),
}

impl Network {
    /// URL/path slug used by data providers (GeckoTerminal-style: Ethereum = "eth").
    pub fn slug(&self) -> &str {
        match self {
            Network::Base => "base",
            Network::Arbitrum => "arbitrum",
            Network::Ethereum => "eth",
            Network::Other(s) => s.as_str(),
        }
    }

    /// Inverse of [`Network::slug`]; unknown slugs round-trip through `Other`.
    pub fn from_slug(slug: &str) -> Network {
        match slug {
            "base" => Network::Base,
            "arbitrum" => Network::Arbitrum,
            "eth" => Network::Ethereum,
            other => Network::Other(other.to_string()),
        }
    }
}

/// A DEX pool address on a given network — the atomic unit of a price series.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PoolId(pub String);

/// A tradable instrument: a pool plus human-readable metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub network: Network,
    pub pool: PoolId,
    pub name: String,
    /// Base-token contract address, for joining on-chain data. `None` when unknown
    /// (e.g. reconstructed from an older store row).
    pub token: Option<String>,
}

/// Bar aggregation interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    Minute,
    Hour,
    Day,
}

/// A single OHLCV bar. `volume` is quote-denominated (USD) per the provider.
///
/// Invariant (checked at ingestion, not construction): `low <= min(open, close)`
/// and `high >= max(open, close)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub ts: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl Bar {
    /// True when OHLC relationships are self-consistent.
    pub fn ohlc_consistent(&self) -> bool {
        let lo_ok = self.low <= self.open.min(self.close);
        let hi_ok = self.high >= self.open.max(self.close);
        lo_ok && hi_ok
    }
}

/// A target portfolio weight for one instrument at one rebalance point.
/// The sign encodes direction; magnitude is fraction of deployable capital.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetWeight {
    pub instrument: Instrument,
    pub weight: Decimal,
}

/// Per-day on-chain DEX activity for one token (from decoded swaps). Distinct
/// from OHLCV: this is microstructure — trade counts, USD flow, unique traders.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnchainDaily {
    pub date: DateTime<Utc>,
    pub trades: i64,
    pub volume_usd: Decimal,
    pub unique_buyers: i64,
}
