//! `cw-core` — the shared vocabulary every other crate depends on.
//!
//! Holds canonical domain types, the money ([`Decimal`]) and time primitives,
//! and the platform-wide [`CoreError`]. It deliberately contains no I/O and no
//! trait seams — those live in the capability crates (`providers`, `features`,
//! `strategy`, …) so the dependency graph stays a clean DAG.

pub mod error;
pub mod types;

pub use error::{CoreError, Result};
pub use types::{Bar, Instrument, Interval, Network, PoolId, TargetWeight};

/// Re-export so downstream crates share one `Decimal` type.
pub use rust_decimal::Decimal;
