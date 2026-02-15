//! Caching layer for Lens.org API responses.
//!
//! This module provides a persistent cache for citations, references, and article metadata
//! to dramatically reduce API calls and improve performance. Designed for production use
//! with 500+ concurrent users.
//!
//! # Features
//!
//! - **Multi-database support**: SQLite (development) and PostgreSQL (production)
//! - **Batch operations**: Query hundreds of articles in a single SQL query
//! - **JSON storage**: Efficient storage of citation/reference lists
//! - **High concurrency**: Optimized for 500+ concurrent readers
//!
//! # Usage
//!
//! ```ignore
//! use biblizap_rs::lens::cache::LensCache;
//!
//! // SQLite for development
//! let cache = LensCache::new_sqlite("cache.db").await?;
//!
//! // PostgreSQL for production
//! let cache = LensCache::new_postgres("postgresql://localhost/biblizap").await?;
//!
//! // Batch query
//! let result = cache.get_relationships_batch(&ids).await?;
//! println!("Cache hits: {}, misses: {}", result.hits.len(), result.misses.len());
//! ```

use super::error::LensError;
use super::lensid::LensId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[cfg(feature = "cache-sqlite")]
use sqlx::sqlite::SqlitePool;

#[cfg(feature = "cache-postgres")]
use sqlx::postgres::PgPool;

#[cfg(feature = "cache")]
use sqlx::Row;

/// Result of a batch cache query, separating hits from misses.
#[derive(Debug, Clone)]
pub struct CacheResult<T> {
    /// Articles found in cache (lens_id → data)
    pub hits: HashMap<LensId, T>,
    /// Articles not found in cache (need to fetch from API)
    pub misses: Vec<LensId>,
}

/// Cached relationship data for an article (citations and references).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CachedArticleData {
    /// List of articles that cite this article (incoming edges)
    pub citations: Vec<String>,
    /// List of articles that this article references (outgoing edges)
    pub references: Vec<String>,
}

/// Cached complete article metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedArticle {
    pub lens_id: String,
    pub title: Option<String>,
    pub year_published: Option<i32>,
    pub abstract_text: Option<String>,
    pub authors_json: Option<String>,
    pub source_json: Option<String>,
    pub external_ids_json: Option<String>,
    pub scholarly_citations_count: Option<i32>,
}

/// Cache statistics for monitoring performance.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub api_calls_saved: u64,
}

impl CacheStats {
    /// Calculate cache hit rate as a percentage.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            (self.hits as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(all(
    feature = "cache",
    feature = "cache-sqlite",
    not(feature = "cache-postgres")
))]
/// Persistent cache for Lens.org API responses using SQLite.
///
/// Stores citations, references, and article metadata to reduce API calls
/// and improve performance for concurrent users.
pub struct LensCache {
    pool: SqlitePool,
}

#[cfg(all(feature = "cache", feature = "cache-postgres"))]
/// Persistent cache for Lens.org API responses using PostgreSQL.
///
/// Stores citations, references, and article metadata to reduce API calls
/// and improve performance for concurrent users.
pub struct LensCache {
    pool: PgPool,
}

#[cfg(feature = "cache")]
impl LensCache {
    /// Create a new cache with SQLite backend.
    ///
    /// # Arguments
    ///
    /// * `database_url` - Path to SQLite database file (e.g., "cache.db" or "sqlite:cache.db")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cache = LensCache::new_sqlite("cache.db").await?;
    /// ```
    #[cfg(feature = "cache-sqlite")]
    pub async fn new_sqlite(database_url: &str) -> Result<Self, LensError> {
        let url = if database_url.starts_with("sqlite:") {
            database_url.to_string()
        } else {
            format!("sqlite:{}?mode=rwc", database_url)
        };

        let pool = SqlitePool::connect(&url).await?;
        Self::run_migrations(&pool).await?;
        Self::optimize_sqlite(&pool).await?;

        Ok(Self { pool })
    }

    /// Create a new cache with PostgreSQL backend.
    ///
    /// # Arguments
    ///
    /// * `database_url` - PostgreSQL connection string (e.g., "postgresql://user:pass@localhost/db")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cache = LensCache::new_postgres("postgresql://localhost/biblizap").await?;
    /// ```
    #[cfg(feature = "cache-postgres")]
    pub async fn new_postgres(database_url: &str) -> Result<Self, LensError> {
        let pool = PgPool::connect(database_url).await?;
        Self::run_migrations(&pool).await?;

        Ok(Self { pool })
    }

    /// Run database migrations to set up schema.
    #[cfg(feature = "cache-sqlite")]
    async fn run_migrations(pool: &SqlitePool) -> Result<(), LensError> {
        // Create articles_cache table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS articles_cache (
                lens_id TEXT PRIMARY KEY,
                citations_json TEXT,
                references_json TEXT,
                article_json TEXT,
                cached_at INTEGER DEFAULT (unixepoch())
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Create index on cached_at for cleanup queries
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cached_at ON articles_cache(cached_at)")
            .execute(pool)
            .await?;

        Ok(())
    }

    #[cfg(feature = "cache-postgres")]
    async fn run_migrations(pool: &PgPool) -> Result<(), LensError> {
        // Create articles_cache table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS articles_cache (
                lens_id TEXT PRIMARY KEY,
                citations_json TEXT,
                references_json TEXT,
                article_json TEXT,
                cached_at BIGINT DEFAULT (CAST(EXTRACT(EPOCH FROM NOW()) AS BIGINT))
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Create index on cached_at for cleanup queries
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cached_at ON articles_cache(cached_at)")
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Optimize SQLite for better concurrent read performance.
    #[cfg(feature = "cache-sqlite")]
    async fn optimize_sqlite(pool: &SqlitePool) -> Result<(), LensError> {
        // Enable WAL mode for better concurrency
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(pool)
            .await?;

        // Optimize for speed
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(pool)
            .await?;

        // Use memory for temp tables
        sqlx::query("PRAGMA temp_store = MEMORY")
            .execute(pool)
            .await?;

        // Set larger cache size (64MB)
        sqlx::query("PRAGMA cache_size = -64000")
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Batch query for citations and references.
    ///
    /// Returns cache hits and misses in a single query. This is the primary
    /// method for retrieving cached data efficiently.
    ///
    /// # Arguments
    ///
    /// * `ids` - Slice of LensIds to query
    ///
    /// # Returns
    ///
    /// `CacheResult` with hits (found in cache) and misses (need API fetch)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = cache.get_relationships_batch(&ids).await?;
    ///
    /// // Process hits immediately
    /// for (id, data) in result.hits {
    ///     println!("{} has {} citations", id.as_ref(), data.citations.len());
    /// }
    ///
    /// // Fetch misses from API
    /// if !result.misses.is_empty() {
    ///     let fresh = fetch_from_api(&result.misses).await?;
    ///     cache.store_relationships_batch(&fresh).await?;
    /// }
    /// ```
    pub async fn get_relationships_batch(
        &self,
        ids: &[LensId],
    ) -> Result<CacheResult<CachedArticleData>, LensError> {
        if ids.is_empty() {
            return Ok(CacheResult {
                hits: HashMap::new(),
                misses: Vec::new(),
            });
        }

        // Build parameterized query with placeholders
        let placeholders = (1..=ids.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");

        let query_str = format!(
            "SELECT lens_id, citations_json, references_json FROM articles_cache WHERE lens_id IN ({})",
            placeholders
        );

        // Build query and bind parameters
        let mut query = sqlx::query(&query_str);
        for id in ids {
            query = query.bind(id.as_ref());
        }

        let rows = query.fetch_all(&self.pool).await?;

        // Parse results
        let mut hits = HashMap::new();
        for row in rows {
            let lens_id: String = row.try_get("lens_id")?;
            let citations_json: Option<String> = row.try_get("citations_json").ok();
            let references_json: Option<String> = row.try_get("references_json").ok();

            let data = CachedArticleData {
                citations: citations_json
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default(),
                references: references_json
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default(),
            };

            if let Ok(id) = LensId::try_from(lens_id.as_str()) {
                hits.insert(id, data);
            }
        }

        // Find misses
        let misses = ids
            .iter()
            .filter(|id| !hits.contains_key(id))
            .cloned()
            .collect();

        Ok(CacheResult { hits, misses })
    }

    /// Store citations and references for a single article.
    ///
    /// # Arguments
    ///
    /// * `id` - Article lens_id
    /// * `citations` - List of articles that cite this article
    /// * `references` - List of articles this article references
    #[cfg(feature = "cache-sqlite")]
    pub async fn store_relationships(
        &self,
        id: &LensId,
        citations: &[LensId],
        references: &[LensId],
    ) -> Result<(), LensError> {
        let citations_json =
            serde_json::to_string(&citations.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                .map_err(|e| LensError::SerdeJson(e))?;

        let references_json =
            serde_json::to_string(&references.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                .map_err(|e| LensError::SerdeJson(e))?;

        sqlx::query(
            r#"
            INSERT INTO articles_cache (lens_id, citations_json, references_json, cached_at)
            VALUES (?, ?, ?, unixepoch())
            ON CONFLICT (lens_id) DO UPDATE SET
                citations_json = excluded.citations_json,
                references_json = excluded.references_json,
                cached_at = unixepoch()
            "#,
        )
        .bind(id.as_ref())
        .bind(citations_json)
        .bind(references_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    #[cfg(feature = "cache-postgres")]
    pub async fn store_relationships(
        &self,
        id: &LensId,
        citations: &[LensId],
        references: &[LensId],
    ) -> Result<(), LensError> {
        let citations_json =
            serde_json::to_string(&citations.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                .map_err(|e| LensError::SerdeJson(e))?;

        let references_json =
            serde_json::to_string(&references.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                .map_err(|e| LensError::SerdeJson(e))?;

        sqlx::query(
            r#"
            INSERT INTO articles_cache (lens_id, citations_json, references_json, cached_at)
            VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT)
            ON CONFLICT (lens_id) DO UPDATE SET
                citations_json = EXCLUDED.citations_json,
                references_json = EXCLUDED.references_json,
                cached_at = EXTRACT(EPOCH FROM NOW())::BIGINT
            "#,
        )
        .bind(id.as_ref())
        .bind(citations_json)
        .bind(references_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Store relationships for multiple articles in a batch.
    ///
    /// More efficient than calling `store_relationships` multiple times.
    ///
    /// # Arguments
    ///
    /// * `articles` - List of (id, citations, references) tuples
    #[cfg(feature = "cache-sqlite")]
    pub async fn store_relationships_batch(
        &self,
        articles: &[(LensId, Vec<LensId>, Vec<LensId>)],
    ) -> Result<(), LensError> {
        if articles.is_empty() {
            return Ok(());
        }

        // Use a transaction for batch insert
        let mut tx = self.pool.begin().await?;

        for (id, citations, references) in articles {
            let citations_json =
                serde_json::to_string(&citations.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                    .map_err(|e| LensError::SerdeJson(e))?;

            let references_json =
                serde_json::to_string(&references.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                    .map_err(|e| LensError::SerdeJson(e))?;

            sqlx::query(
                r#"
                INSERT INTO articles_cache (lens_id, citations_json, references_json, cached_at)
                VALUES (?, ?, ?, unixepoch())
                ON CONFLICT (lens_id) DO UPDATE SET
                    citations_json = excluded.citations_json,
                    references_json = excluded.references_json,
                    cached_at = unixepoch()
                "#,
            )
            .bind(id.as_ref())
            .bind(citations_json)
            .bind(references_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    #[cfg(feature = "cache-postgres")]
    pub async fn store_relationships_batch(
        &self,
        articles: &[(LensId, Vec<LensId>, Vec<LensId>)],
    ) -> Result<(), LensError> {
        if articles.is_empty() {
            return Ok(());
        }

        // Use a transaction for batch insert
        let mut tx = self.pool.begin().await?;

        for (id, citations, references) in articles {
            let citations_json =
                serde_json::to_string(&citations.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                    .map_err(|e| LensError::SerdeJson(e))?;

            let references_json =
                serde_json::to_string(&references.iter().map(|id| id.as_ref()).collect::<Vec<_>>())
                    .map_err(|e| LensError::SerdeJson(e))?;

            sqlx::query(
                r#"
                INSERT INTO articles_cache (lens_id, citations_json, references_json, cached_at)
                VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT)
                ON CONFLICT (lens_id) DO UPDATE SET
                    citations_json = EXCLUDED.citations_json,
                    references_json = EXCLUDED.references_json,
                    cached_at = EXTRACT(EPOCH FROM NOW())::BIGINT
                "#,
            )
            .bind(id.as_ref())
            .bind(citations_json)
            .bind(references_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Get complete article metadata for multiple articles.
    pub async fn get_articles_batch(
        &self,
        ids: &[LensId],
    ) -> Result<CacheResult<CachedArticle>, LensError> {
        if ids.is_empty() {
            return Ok(CacheResult {
                hits: HashMap::new(),
                misses: Vec::new(),
            });
        }

        let placeholders = (1..=ids.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");

        let query_str = format!(
            "SELECT lens_id, article_json FROM articles_cache WHERE lens_id IN ({}) AND article_json IS NOT NULL",
            placeholders
        );

        let mut query = sqlx::query(&query_str);
        for id in ids {
            query = query.bind(id.as_ref());
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut hits = HashMap::new();
        for row in rows {
            let lens_id: String = row.try_get("lens_id")?;
            let article_json: Option<String> = row.try_get("article_json").ok();

            if let Some(json) = article_json {
                if let Ok(article) = serde_json::from_str::<CachedArticle>(&json) {
                    if let Ok(id) = LensId::try_from(lens_id.as_str()) {
                        hits.insert(id, article);
                    }
                }
            }
        }

        let misses = ids
            .iter()
            .filter(|id| !hits.contains_key(id))
            .cloned()
            .collect();

        Ok(CacheResult { hits, misses })
    }

    /// Store complete article metadata.
    #[cfg(feature = "cache-sqlite")]
    pub async fn store_article(&self, article: &CachedArticle) -> Result<(), LensError> {
        let article_json = serde_json::to_string(article).map_err(|e| LensError::SerdeJson(e))?;

        sqlx::query(
            r#"
            INSERT INTO articles_cache (lens_id, article_json, cached_at)
            VALUES (?, ?, unixepoch())
            ON CONFLICT (lens_id) DO UPDATE SET
                article_json = excluded.article_json,
                cached_at = unixepoch()
            "#,
        )
        .bind(&article.lens_id)
        .bind(article_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    #[cfg(feature = "cache-postgres")]
    pub async fn store_article(&self, article: &CachedArticle) -> Result<(), LensError> {
        let article_json = serde_json::to_string(article).map_err(|e| LensError::SerdeJson(e))?;

        sqlx::query(
            r#"
            INSERT INTO articles_cache (lens_id, article_json, cached_at)
            VALUES ($1, $2, EXTRACT(EPOCH FROM NOW())::BIGINT)
            ON CONFLICT (lens_id) DO UPDATE SET
                article_json = EXCLUDED.article_json,
                cached_at = EXTRACT(EPOCH FROM NOW())::BIGINT
            "#,
        )
        .bind(&article.lens_id)
        .bind(article_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get cache statistics (total entries, oldest entry, etc.)
    pub async fn get_stats(&self) -> Result<CacheStats, LensError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM articles_cache")
            .fetch_one(&self.pool)
            .await?;

        let count: i64 = row.try_get("count")?;

        Ok(CacheStats {
            hits: 0,
            misses: 0,
            api_calls_saved: count as u64,
        })
    }

    /// Clear all cached data.
    ///
    /// ⚠️ Use with caution - this deletes all cached data!
    pub async fn clear(&self) -> Result<(), LensError> {
        sqlx::query("DELETE FROM articles_cache")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete entries older than specified days.
    ///
    /// Useful for cache invalidation strategy.
    ///
    /// # Arguments
    ///
    /// * `days` - Delete entries older than this many days
    pub async fn delete_older_than(&self, days: i64) -> Result<u64, LensError> {
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - (days * 24 * 60 * 60);

        let result = sqlx::query("DELETE FROM articles_cache WHERE cached_at < $1")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }
}

// Provide a no-op cache when feature is disabled
#[cfg(not(feature = "cache"))]
/// Stub implementation when cache feature is disabled.
pub struct LensCache;

#[cfg(not(feature = "cache"))]
impl LensCache {
    /// Returns an error indicating cache is not enabled.
    pub async fn new_sqlite(_database_url: &str) -> Result<Self, LensError> {
        Err(LensError::NoArticlesFound) // Placeholder error
    }

    /// Returns an error indicating cache is not enabled.
    pub async fn new_postgres(_database_url: &str) -> Result<Self, LensError> {
        Err(LensError::NoArticlesFound) // Placeholder error
    }
}

#[cfg(all(test, feature = "cache"))]
mod tests {
    use super::*;

    async fn create_test_cache() -> LensCache {
        // Use in-memory SQLite for tests
        LensCache::new_sqlite(":memory:").await.unwrap()
    }

    fn test_lens_id(id: &str) -> LensId {
        LensId::try_from(id).unwrap()
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = create_test_cache().await;
        let ids = vec![test_lens_id("020-200-401-307-33X")];

        let result = cache.get_relationships_batch(&ids).await.unwrap();

        assert_eq!(result.hits.len(), 0);
        assert_eq!(result.misses.len(), 1);
        assert_eq!(result.misses[0].as_ref(), "020-200-401-307-33X");
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let cache = create_test_cache().await;
        let id = test_lens_id("020-200-401-307-33X");
        let citations = vec![test_lens_id("111-111-111-111-111")];
        let references = vec![test_lens_id("222-222-222-222-222")];

        // Store data
        cache
            .store_relationships(&id, &citations, &references)
            .await
            .unwrap();

        // Query back
        let result = cache.get_relationships_batch(&[id.clone()]).await.unwrap();

        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.misses.len(), 0);
        assert!(result.hits.contains_key(&id));

        let data = result.hits.get(&id).unwrap();
        assert_eq!(data.citations.len(), 1);
        assert_eq!(data.references.len(), 1);
        assert_eq!(data.citations[0], "111-111-111-111-111");
        assert_eq!(data.references[0], "222-222-222-222-222");
    }

    #[tokio::test]
    async fn test_batch_query_mixed() {
        let cache = create_test_cache().await;

        let id1 = test_lens_id("020-200-401-307-33X");
        let id2 = test_lens_id("050-708-976-791-252");
        let id3 = test_lens_id("999-999-999-999-999");

        // Store only id1 and id2
        cache
            .store_relationships(&id1, &vec![test_lens_id("111-111-111-111-111")], &vec![])
            .await
            .unwrap();

        cache
            .store_relationships(&id2, &vec![], &vec![test_lens_id("222-222-222-222-222")])
            .await
            .unwrap();

        // Query all three
        let result = cache
            .get_relationships_batch(&[id1.clone(), id2.clone(), id3.clone()])
            .await
            .unwrap();

        // Should have 2 hits, 1 miss
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.misses.len(), 1);
        assert!(result.hits.contains_key(&id1));
        assert!(result.hits.contains_key(&id2));
        assert_eq!(result.misses[0], id3);
    }

    #[tokio::test]
    async fn test_batch_store() {
        let cache = create_test_cache().await;

        let id1 = test_lens_id("020-200-401-307-33X");
        let id2 = test_lens_id("050-708-976-791-252");

        let batch = vec![
            (
                id1.clone(),
                vec![test_lens_id("111-111-111-111-111")],
                vec![],
            ),
            (
                id2.clone(),
                vec![],
                vec![test_lens_id("222-222-222-222-222")],
            ),
        ];

        cache.store_relationships_batch(&batch).await.unwrap();

        // Verify both were stored
        let result = cache
            .get_relationships_batch(&[id1.clone(), id2.clone()])
            .await
            .unwrap();

        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.misses.len(), 0);
    }

    #[tokio::test]
    async fn test_empty_query() {
        let cache = create_test_cache().await;
        let result = cache.get_relationships_batch(&[]).await.unwrap();

        assert_eq!(result.hits.len(), 0);
        assert_eq!(result.misses.len(), 0);
    }

    #[tokio::test]
    async fn test_update_existing() {
        let cache = create_test_cache().await;
        let id = test_lens_id("020-200-401-307-33X");

        // Store initial data
        cache
            .store_relationships(&id, &vec![test_lens_id("111-111-111-111-111")], &vec![])
            .await
            .unwrap();

        // Update with new data
        cache
            .store_relationships(
                &id,
                &vec![
                    test_lens_id("111-111-111-111-111"),
                    test_lens_id("222-222-222-222-222"),
                ],
                &vec![test_lens_id("333-333-333-333-333")],
            )
            .await
            .unwrap();

        // Verify updated data
        let result = cache.get_relationships_batch(&[id.clone()]).await.unwrap();
        let data = result.hits.get(&id).unwrap();

        assert_eq!(data.citations.len(), 2);
        assert_eq!(data.references.len(), 1);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let cache = create_test_cache().await;
        let id = test_lens_id("020-200-401-307-33X");

        // Store data
        cache
            .store_relationships(&id, &vec![test_lens_id("111-111-111-111-111")], &vec![])
            .await
            .unwrap();

        // Verify it exists
        let result = cache.get_relationships_batch(&[id.clone()]).await.unwrap();
        assert_eq!(result.hits.len(), 1);

        // Clear cache
        cache.clear().await.unwrap();

        // Verify it's gone
        let result = cache.get_relationships_batch(&[id.clone()]).await.unwrap();
        assert_eq!(result.hits.len(), 0);
        assert_eq!(result.misses.len(), 1);
    }
}
