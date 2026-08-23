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
    /// Base L2.
    Base,
    /// Arbitrum One L2.
    Arbitrum,
    /// Ethereum mainnet.
    Ethereum,
    /// Any other network, keyed by its provider slug (e.g. `"hyperliquid"`).
    Other(String),
}

impl Network {
    /// URL/path slug used by data providers (e.g. Ethereum = "eth").
    pub const fn slug(&self) -> &str {
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
    /// Chain the instrument trades on.
    pub network: Network,
    /// Pool / market identifier on that network.
    pub pool: PoolId,
    /// Human-readable symbol (e.g. `"BTC"`).
    pub name: String,
    /// Base-token contract address, for joining on-chain data. `None` when unknown
    /// (e.g. reconstructed from an older store row).
    pub token: Option<String>,
}

/// Bar aggregation interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    /// One-minute bars.
    Minute,
    /// One-hour bars.
    Hour,
    /// Daily bars.
    Day,
}

/// A single OHLCV bar. `volume` is quote-denominated (USD) per the provider.
///
/// Invariant (checked at ingestion, not construction): `low <= min(open, close)`
/// and `high >= max(open, close)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    /// Bar open timestamp (UTC).
    pub ts: DateTime<Utc>,
    /// Open price.
    pub open: Decimal,
    /// Highest price in the bar.
    pub high: Decimal,
    /// Lowest price in the bar.
    pub low: Decimal,
    /// Close price.
    pub close: Decimal,
    /// Quote-denominated (USD) volume.
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
    /// Instrument the weight applies to.
    pub instrument: Instrument,
    /// Signed fraction of deployable capital (sign = direction).
    pub weight: Decimal,
}

/// One perpetual-futures funding observation (Hyperliquid): the hourly funding
/// rate longs pay shorts (negative = shorts pay longs) and the mark-vs-oracle
/// premium. Funding is both a holding cost and a carry-edge signal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingPoint {
    /// Observation timestamp (UTC).
    pub time: DateTime<Utc>,
    /// Hourly funding rate longs pay shorts (negative = shorts pay longs).
    pub funding_rate: Decimal,
    /// Mark-vs-oracle premium at the observation.
    pub premium: Decimal,
}
