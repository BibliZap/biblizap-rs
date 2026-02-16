//! Cache backend implementations for Lens.org article data
//!
//! This module provides a trait-based cache system with multiple backend implementations:
//! - SQLite (via `sqlite` module)
//! - PostgreSQL (via `postgres` module - TODO)
//!
//! The cache stores two types of relationships:
//! - References (outgoing edges): immutable once fetched
//! - Citations (incoming edges): mutable, need periodic refresh

use super::error::LensError;
use super::lensid::LensId;
use async_trait::async_trait;
use std::collections::HashMap;

#[cfg(feature = "cache-sqlite")]
pub mod sqlite;

#[cfg(feature = "cache-postgres")]
pub mod postgres;

// Re-export the backend types for convenience
#[cfg(feature = "cache-sqlite")]
pub use sqlite::SqliteBackend;

// #[cfg(feature = "cache-postgres")]
// pub use postgres::PostgresBackend;

/// Computes which IDs were not found in the cache (misses)
///
/// # Arguments
/// * `requested` - The IDs that were requested
/// * `hits` - The IDs that were found in the cache
///
/// # Returns
/// A vector of IDs that were requested but not found in the cache
pub fn compute_misses<T>(requested: &[LensId], hits: &HashMap<LensId, T>) -> Vec<LensId> {
    requested
        .iter()
        .filter(|id| !hits.contains_key(id))
        .cloned()
        .collect()
}

/// Trait defining the cache backend interface
///
/// Implementations must be thread-safe (Send + Sync) as they may be used
/// across async tasks.
///
/// This trait is always available (no feature flag required), but implementations
/// (SqliteBackend, PostgresBackend) require their respective feature flags.
#[async_trait]
pub trait CacheBackend: Send + Sync {
    // References (immutable)

    /// Retrieve references (outgoing edges) for the given article IDs
    ///
    /// Returns only the IDs that were found in the cache.
    /// Use `compute_misses` to determine which IDs need to be fetched from the API.
    async fn get_references(
        &self,
        ids: &[LensId],
    ) -> Result<HashMap<LensId, Vec<LensId>>, LensError>;

    /// Store references (outgoing edges) for articles
    ///
    /// References are immutable - if an ID already exists in the cache,
    /// the new data is ignored (ON CONFLICT DO NOTHING behavior).
    async fn store_references(&self, batch: &[(LensId, Vec<LensId>)]) -> Result<(), LensError>;

    // Citations (with TTL)

    /// Retrieve citations (incoming edges) for the given article IDs
    ///
    /// Returns citations as LensIds. Citations may be stale and should be
    /// refreshed periodically based on `fetched_at` timestamps.
    async fn get_citations(
        &self,
        ids: &[LensId],
    ) -> Result<HashMap<LensId, Vec<LensId>>, LensError>;

    /// Store citations (incoming edges) for articles
    ///
    /// Citations are mutable - if an ID already exists, the data and timestamp
    /// are updated (ON CONFLICT DO UPDATE behavior).
    async fn store_citations(&self, batch: &[(LensId, Vec<LensId>)]) -> Result<(), LensError>;

    // Maintenance

    /// Clear all cached data (both references and citations)
    async fn clear(&self) -> Result<(), LensError>;
}
