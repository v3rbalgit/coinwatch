//! Shared error type. Every crate returns `cw_core::Result`.

/// Errors surfaced across the platform. Variants intentionally carry owned
/// strings so provider/storage backends can attach context without leaking
/// their concrete error types into the shared vocabulary.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// A seam that is wired but not yet implemented (Phase 1 skeleton).
    #[error("unimplemented: {0}")]
    Unimplemented(&'static str),

    /// A data provider (RPC / API) failed or returned an unexpected shape.
    #[error("data source: {0}")]
    DataSource(String),

    /// The canonical store failed a read/write.
    #[error("storage: {0}")]
    Storage(String),

    /// Input data violated an invariant (e.g. OHLC relationship, lookahead).
    #[error("invalid data: {0}")]
    InvalidData(String),

    /// Feature/strategy configuration was rejected.
    #[error("config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;
