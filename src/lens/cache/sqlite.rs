//! SQLite backend implementation for the Lens cache

use crate::lens::error::LensError;
use crate::lens::lensid::LensId;
use sqlx::SqlitePool;
use std::collections::HashMap;

use super::CacheBackend;

#[derive(sqlx::FromRow)]
struct ReferencesRow {
    pub lens_id: String,
    pub references_json: String,
}

impl ReferencesRow {
    fn extract(self) -> Result<(LensId, Vec<LensId>), LensError> {
        let lens_id = LensId::try_from(self.lens_id.as_str())?;
        let relationships: Vec<LensId> = serde_json::from_str(&self.references_json)
            .ok()
            .unwrap_or_default();

        Ok((lens_id, relationships))
    }
}

#[derive(sqlx::FromRow)]
struct CitationsRow {
    pub lens_id: String,
    pub citations_json: String,
}

impl CitationsRow {
    fn extract(self) -> Result<(LensId, Vec<LensId>), LensError> {
        let lens_id = LensId::try_from(self.lens_id.as_str())?;
        let citations: Vec<LensId> = serde_json::from_str(&self.citations_json)
            .ok()
            .unwrap_or_default();

        Ok((lens_id, citations))
    }
}

/// SQLite-based cache backend
///
/// Uses two tables:
/// - `article_references`: stores immutable outgoing edges
/// - `article_citations`: stores mutable incoming edges with timestamps
///
/// Optimized for bulk operations with:
/// - Chunked multi-row inserts (respects SQLite parameter limits)
/// - Single-transaction commits
/// - WAL mode for better concurrency
/// - JSON-based queries to avoid parameter count limits
pub struct SqliteBackend {
    pool: SqlitePool,
}

impl CacheBackend for SqliteBackend {
    async fn get_references(
        &self,
        ids: &[LensId],
    ) -> Result<HashMap<LensId, Vec<LensId>>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let ids_json = Self::ids_to_json(ids)?;

        let rows: Vec<ReferencesRow> = sqlx::query_as(
            r#"
                SELECT lens_id, references_json
                FROM article_references
                WHERE lens_id IN (SELECT value FROM json_each(?))
            "#,
        )
        .bind(&ids_json)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract()).collect()
    }

    async fn store_references(&self, batch: &[(LensId, Vec<LensId>)]) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 333;

        // Start transaction for all chunks
        let mut tx = self.pool.begin().await?;

        let rough_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for chunk in batch.chunks(CHUNK_SIZE) {
            // Pre-serialize all JSON (handles errors before building query)
            let rows: Vec<(String, String, i64)> = chunk
                .iter()
                .map(|(id, refs)| {
                    let id_str = id.as_ref().to_string();
                    let refs_json = serde_json::to_string(refs)?;

                    Ok((id_str, refs_json, rough_timestamp))
                })
                .collect::<Result<Vec<_>, LensError>>()?;

            // Build multi-row INSERT
            let mut builder = sqlx::QueryBuilder::new(
                "INSERT INTO article_references (lens_id, references_json, fetched_at) ",
            );

            builder.push_values(rows, |mut b, (id_str, refs_json, timestamp)| {
                b.push_bind(id_str)
                    .push_bind(refs_json)
                    .push_bind(timestamp);
            });

            // References are immutable, so just ignore conflicts
            builder.push(" ON CONFLICT (lens_id) DO NOTHING");

            builder.build().execute(&mut *tx).await?;
        }

        // Commit once at the end (single fsync)
        tx.commit().await?;

        Ok(())
    }

    async fn get_citations(
        &self,
        ids: &[LensId],
    ) -> Result<HashMap<LensId, Vec<LensId>>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let ids_json = Self::ids_to_json(ids)?;

        let rows: Vec<CitationsRow> = sqlx::query_as(
            r#"
                SELECT lens_id, citations_json
                FROM article_citations
                WHERE lens_id IN (SELECT value FROM json_each(?))
            "#,
        )
        .bind(&ids_json)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract()).collect()
    }

    async fn store_citations(&self, batch: &[(LensId, Vec<LensId>)]) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 333;

        // Start transaction for all chunks
        let mut tx = self.pool.begin().await?;

        let rough_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for chunk in batch.chunks(CHUNK_SIZE) {
            // Pre-serialize all JSON (handles errors before building query)
            let rows: Vec<(String, String, i64)> = chunk
                .iter()
                .map(|(id, citations)| {
                    let id_str = id.as_ref().to_string();
                    let citations_json = serde_json::to_string(citations)?;

                    Ok((id_str, citations_json, rough_timestamp))
                })
                .collect::<Result<Vec<_>, LensError>>()?;

            // Build multi-row INSERT
            let mut builder = sqlx::QueryBuilder::new(
                "INSERT INTO article_citations (lens_id, citations_json, fetched_at) ",
            );

            builder.push_values(rows, |mut b, (id_str, citations_json, timestamp)| {
                b.push_bind(id_str)
                    .push_bind(citations_json)
                    .push_bind(timestamp);
            });

            // For citations, we want to update with fresh data on conflict
            builder.push(
                " ON CONFLICT (lens_id) DO UPDATE SET citations_json = excluded.citations_json, fetched_at = excluded.fetched_at",
            );

            builder.build().execute(&mut *tx).await?;
        }

        // Commit once at the end (single fsync)
        tx.commit().await?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), LensError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM article_references")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM article_citations")
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}

impl SqliteBackend {
    /// Create a new SQLite backend from a connection URL
    ///
    /// # Arguments
    /// * `url` - SQLite connection URL (e.g., "sqlite::memory:" or "sqlite:cache.db")
    ///
    /// # Example
    /// ```ignore
    /// let backend = SqliteBackend::new("sqlite::memory:").await?;
    /// ```
    pub async fn from_url(url: &str) -> Result<Self, LensError> {
        let pool = SqlitePool::connect(url).await?;
        Self::run_migrations(&pool).await?;
        Self::optimize_sqlite(&pool).await?;

        Ok(Self { pool })
    }

    /// Run database migrations (creates tables if they don't exist)
    async fn run_migrations(pool: &SqlitePool) -> Result<(), LensError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS article_references (
                lens_id TEXT PRIMARY KEY,
                references_json TEXT NOT NULL,
                fetched_at INTEGER NOT NULL DEFAULT (unixepoch())
            ) WITHOUT ROWID
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS article_citations (
                lens_id TEXT PRIMARY KEY,
                citations_json TEXT NOT NULL,
                fetched_at INTEGER NOT NULL DEFAULT (unixepoch())
            ) WITHOUT ROWID
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_citations_fetched ON article_citations(fetched_at)",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Apply SQLite-specific optimizations
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

    /// Convert a slice of LensIds to a JSON string for use in queries
    ///
    /// This allows us to pass many IDs in a single parameter using json_each()
    fn ids_to_json(ids: &[LensId]) -> Result<String, LensError> {
        let json = serde_json::to_string(&ids.iter().map(|id| id.as_ref()).collect::<Vec<_>>())?;
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lens::cache::compute_misses;

    #[tokio::test]
    async fn test_store_and_get_references() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        // Create test data
        let id1 = LensId::from(12345678901234);
        let id2 = LensId::from(98765432109876);
        let id3 = LensId::from(11111111111111);

        let refs1 = vec![LensId::from(1), LensId::from(2), LensId::from(3)];
        let refs2 = vec![LensId::from(4), LensId::from(5)];

        let batch = vec![(id1.clone(), refs1.clone()), (id2.clone(), refs2.clone())];

        // Store references
        backend.store_references(&batch).await?;

        // Retrieve references
        let result = backend.get_references(&[id1.clone(), id2.clone()]).await?;

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&id1).unwrap(), &refs1);
        assert_eq!(result.get(&id2).unwrap(), &refs2);

        // Query non-existent ID
        let result = backend.get_references(&[id3.clone()]).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_references_immutable() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        let id1 = LensId::from(12345678901234);
        let refs1 = vec![LensId::from(1), LensId::from(2)];
        let refs2 = vec![LensId::from(3), LensId::from(4)];

        // Store initial references
        backend
            .store_references(&[(id1.clone(), refs1.clone())])
            .await?;

        // Try to store different references for the same ID (should be ignored)
        backend
            .store_references(&[(id1.clone(), refs2.clone())])
            .await?;

        // Should still return the original references
        let result = backend.get_references(&[id1.clone()]).await?;
        assert_eq!(result.get(&id1).unwrap(), &refs1);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_get_citations() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        let id1 = LensId::from(12345678901234);
        let id2 = LensId::from(98765432109876);
        let id3 = LensId::from(11111111111111);

        let cites1 = vec![LensId::from(10), LensId::from(20), LensId::from(30)];
        let cites2 = vec![LensId::from(40), LensId::from(50)];

        let batch = vec![(id1.clone(), cites1.clone()), (id2.clone(), cites2.clone())];

        // Store citations
        backend.store_citations(&batch).await?;

        // Retrieve citations
        let result = backend.get_citations(&[id1.clone(), id2.clone()]).await?;

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&id1).unwrap(), &cites1);
        assert_eq!(result.get(&id2).unwrap(), &cites2);

        // Query non-existent ID
        let result = backend.get_citations(&[id3.clone()]).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_citations_updates_on_conflict() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        let id1 = LensId::from(12345678901234);
        let cites1 = vec![LensId::from(10), LensId::from(20)];
        let cites2 = vec![LensId::from(30), LensId::from(40), LensId::from(50)];

        // Store initial citations
        backend
            .store_citations(&[(id1.clone(), cites1.clone())])
            .await?;

        // Store updated citations (should replace)
        backend
            .store_citations(&[(id1.clone(), cites2.clone())])
            .await?;

        // Should return the updated citations
        let result = backend.get_citations(&[id1.clone()]).await?;
        assert_eq!(result.get(&id1).unwrap(), &cites2);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_input_handling() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        // Test empty get_references
        let result = backend.get_references(&[]).await?;
        assert_eq!(result.len(), 0);

        // Test empty store_references
        backend.store_references(&[]).await?;

        // Test empty get_citations
        let result = backend.get_citations(&[]).await?;
        assert_eq!(result.len(), 0);

        // Test empty store_citations
        backend.store_citations(&[]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_clear() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        let id1 = LensId::from(12345678901234);
        let id2 = LensId::from(98765432109876);

        let refs = vec![LensId::from(1), LensId::from(2)];
        let cites = vec![LensId::from(10), LensId::from(20)];

        // Store some data
        backend
            .store_references(&[(id1.clone(), refs.clone())])
            .await?;
        backend
            .store_citations(&[(id2.clone(), cites.clone())])
            .await?;

        // Verify data exists
        let refs_result = backend.get_references(&[id1.clone()]).await?;
        let cites_result = backend.get_citations(&[id2.clone()]).await?;
        assert_eq!(refs_result.len(), 1);
        assert_eq!(cites_result.len(), 1);

        // Clear all data
        backend.clear().await?;

        // Verify data is gone
        let refs_result = backend.get_references(&[id1.clone()]).await?;
        let cites_result = backend.get_citations(&[id2.clone()]).await?;
        assert_eq!(refs_result.len(), 0);
        assert_eq!(cites_result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_bulk_insert_chunking() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        // Create a batch larger than CHUNK_SIZE (333)
        let mut batch = Vec::new();
        for i in 0..500 {
            let id = LensId::from(10000000000000 + i);
            let refs = vec![LensId::from(i), LensId::from(i + 1)];
            batch.push((id, refs));
        }

        // Store large batch
        backend.store_references(&batch).await?;

        // Verify all were stored
        let ids: Vec<LensId> = batch.iter().map(|(id, _)| id.clone()).collect();
        let result = backend.get_references(&ids).await?;
        assert_eq!(result.len(), 500);

        Ok(())
    }

    #[tokio::test]
    async fn test_compute_misses() -> Result<(), LensError> {
        let backend = SqliteBackend::from_url("sqlite::memory:").await?;

        let id1 = LensId::from(12345678901234);
        let id2 = LensId::from(98765432109876);
        let id3 = LensId::from(11111111111111);
        let id4 = LensId::from(22222222222222);

        // Store only id1 and id2
        let refs = vec![LensId::from(1)];
        backend
            .store_references(&[(id1.clone(), refs.clone())])
            .await?;
        backend
            .store_references(&[(id2.clone(), refs.clone())])
            .await?;

        // Request id1, id2, id3, id4
        let requested = vec![id1.clone(), id2.clone(), id3.clone(), id4.clone()];
        let hits = backend.get_references(&requested).await?;

        // Compute misses
        let misses = compute_misses(&requested, &hits);

        assert_eq!(hits.len(), 2);
        assert_eq!(misses.len(), 2);
        assert!(misses.contains(&id3));
        assert!(misses.contains(&id4));
        assert!(!misses.contains(&id1));
        assert!(!misses.contains(&id2));

        Ok(())
    }
}
