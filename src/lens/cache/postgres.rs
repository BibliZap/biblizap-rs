//! PostgreSQL backend implementation for the Lens cache

use crate::lens::article::Article;
use crate::lens::error::LensError;
use crate::lens::lensid::LensId;
use async_trait::async_trait;
use sqlx::PgPool;
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

    fn extract_string(self) -> Result<(String, Vec<LensId>), LensError> {
        let relationships: Vec<LensId> = serde_json::from_str(&self.references_json)
            .ok()
            .unwrap_or_default();

        Ok((self.lens_id, relationships))
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

    fn extract_string(self) -> Result<(String, Vec<LensId>), LensError> {
        let citations: Vec<LensId> = serde_json::from_str(&self.citations_json)
            .ok()
            .unwrap_or_default();

        Ok((self.lens_id, citations))
    }
}

#[derive(sqlx::FromRow)]
struct ArticleRow {
    pub lens_id: String,
    pub article_json: String,
}

impl ArticleRow {
    fn extract(self) -> Result<(LensId, Article), LensError> {
        let lens_id = LensId::try_from(self.lens_id.as_str())?;
        let article: Article = serde_json::from_str(&self.article_json)?;

        Ok((lens_id, article))
    }

    fn extract_string(self) -> Result<(String, Article), LensError> {
        let article: Article = serde_json::from_str(&self.article_json)?;

        Ok((self.lens_id, article))
    }
}

/// PostgreSQL-based cache backend
///
/// Uses two tables:
/// - `article_references`: stores immutable outgoing edges
/// - `article_citations`: stores mutable incoming edges with timestamps
///
/// Optimized for bulk operations with:
/// - Chunked multi-row inserts (more generous limits than SQLite)
/// - Single-transaction commits
/// - Native array operations with ANY() for efficient queries
/// - JSONB columns for better performance than TEXT
pub struct PostgresBackend {
    pool: PgPool,
}

#[async_trait]
impl CacheBackend for PostgresBackend {
    async fn get_references(
        &self,
        ids: &[LensId],
    ) -> Result<HashMap<LensId, Vec<LensId>>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        // PostgreSQL allows us to use ANY with an array - more efficient than JSON
        let ids_vec: Vec<String> = ids.iter().map(|id| id.as_ref().to_string()).collect();

        let rows: Vec<ReferencesRow> = sqlx::query_as(
            r#"
                SELECT lens_id, references_json
                FROM article_references
                WHERE lens_id = ANY($1)
            "#,
        )
        .bind(&ids_vec)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract()).collect()
    }

    async fn store_references(&self, batch: &[(LensId, Vec<LensId>)]) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        // PostgreSQL has higher parameter limits, so we can use larger chunks
        // Postgres default max_prepared_transactions is 32767 parameters
        // With 3 params per row: 32767 / 3 = ~10922, use 5000 for safety
        const CHUNK_SIZE: usize = 5000;

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

        // Commit once at the end
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

        // Use PostgreSQL's native array operations
        let ids_vec: Vec<String> = ids.iter().map(|id| id.as_ref().to_string()).collect();

        let rows: Vec<CitationsRow> = sqlx::query_as(
            r#"
                SELECT lens_id, citations_json
                FROM article_citations
                WHERE lens_id = ANY($1)
            "#,
        )
        .bind(&ids_vec)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract()).collect()
    }

    async fn store_citations(&self, batch: &[(LensId, Vec<LensId>)]) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 5000;

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
                " ON CONFLICT (lens_id) DO UPDATE SET citations_json = EXCLUDED.citations_json, fetched_at = EXCLUDED.fetched_at",
            );

            builder.build().execute(&mut *tx).await?;
        }

        // Commit once at the end
        tx.commit().await?;

        Ok(())
    }

    async fn get_article_data(
        &self,
        ids: &[LensId],
    ) -> Result<HashMap<LensId, Article>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        // PostgreSQL allows us to use ANY with an array - more efficient than JSON
        let ids_vec: Vec<String> = ids.iter().map(|id| id.as_ref().to_string()).collect();

        let rows: Vec<ArticleRow> = sqlx::query_as(
            r#"
                SELECT lens_id, article_json
                FROM article_data
                WHERE lens_id = ANY($1)
            "#,
        )
        .bind(&ids_vec)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract()).collect()
    }

    async fn store_article_data(&self, batch: &[(LensId, Article)]) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        // PostgreSQL has higher parameter limits, so we can use larger chunks
        // Postgres default max_prepared_transactions is 32767 parameters
        // With 3 params per row: 32767 / 3 = ~10922, use 5000 for safety
        const CHUNK_SIZE: usize = 5000;

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
                .map(|(id, articles)| {
                    let id_str = id.as_ref().to_string();
                    let articles_json = serde_json::to_string(articles)?;

                    Ok((id_str, articles_json, rough_timestamp))
                })
                .collect::<Result<Vec<_>, LensError>>()?;

            // Build multi-row INSERT
            let mut builder = sqlx::QueryBuilder::new(
                "INSERT INTO article_data (lens_id, article_json, fetched_at) ",
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

        // Commit once at the end
        tx.commit().await?;

        Ok(())
    }

    // Non-LensId methods (for user input: PMID, DOI, etc.)

    async fn get_references_not_lens_id(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, Vec<LensId>>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let ids_vec: Vec<String> = ids.to_vec();

        let rows: Vec<ReferencesRow> = sqlx::query_as(
            r#"
                SELECT string_id as lens_id, references_json
                FROM article_references_not_lens_id
                WHERE string_id = ANY($1)
            "#,
        )
        .bind(&ids_vec)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract_string()).collect()
    }

    async fn store_references_not_lens_id(
        &self,
        batch: &[(String, Vec<LensId>)],
    ) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 5000;

        let mut tx = self.pool.begin().await?;

        let rough_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for chunk in batch.chunks(CHUNK_SIZE) {
            let rows: Vec<(String, String, i64)> = chunk
                .iter()
                .map(|(id, refs)| {
                    let refs_json = serde_json::to_string(refs)?;
                    Ok((id.clone(), refs_json, rough_timestamp))
                })
                .collect::<Result<Vec<_>, LensError>>()?;

            let mut builder = sqlx::QueryBuilder::new(
                "INSERT INTO article_references_not_lens_id (string_id, references_json, fetched_at) ",
            );

            builder.push_values(rows, |mut b, (id_str, refs_json, timestamp)| {
                b.push_bind(id_str)
                    .push_bind(refs_json)
                    .push_bind(timestamp);
            });

            builder.push(" ON CONFLICT (string_id) DO NOTHING");

            builder.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }

    async fn get_citations_not_lens_id(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, Vec<LensId>>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let ids_vec: Vec<String> = ids.to_vec();

        let rows: Vec<CitationsRow> = sqlx::query_as(
            r#"
                SELECT string_id as lens_id, citations_json
                FROM article_citations_not_lens_id
                WHERE string_id = ANY($1)
            "#,
        )
        .bind(&ids_vec)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract_string()).collect()
    }

    async fn store_citations_not_lens_id(
        &self,
        batch: &[(String, Vec<LensId>)],
    ) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 5000;

        let mut tx = self.pool.begin().await?;

        let rough_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for chunk in batch.chunks(CHUNK_SIZE) {
            let rows: Vec<(String, String, i64)> = chunk
                .iter()
                .map(|(id, citations)| {
                    let citations_json = serde_json::to_string(citations)?;
                    Ok((id.clone(), citations_json, rough_timestamp))
                })
                .collect::<Result<Vec<_>, LensError>>()?;

            let mut builder = sqlx::QueryBuilder::new(
                "INSERT INTO article_citations_not_lens_id (string_id, citations_json, fetched_at) ",
            );

            builder.push_values(rows, |mut b, (id_str, citations_json, timestamp)| {
                b.push_bind(id_str)
                    .push_bind(citations_json)
                    .push_bind(timestamp);
            });

            builder.push(
                " ON CONFLICT (string_id) DO UPDATE SET citations_json = excluded.citations_json, fetched_at = excluded.fetched_at",
            );

            builder.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }

    async fn get_article_data_not_lens_id(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, Article>, LensError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let ids_vec: Vec<String> = ids.to_vec();

        let rows: Vec<ArticleRow> = sqlx::query_as(
            r#"
                SELECT string_id as lens_id, article_json
                FROM article_data_not_lens_id
                WHERE string_id = ANY($1)
            "#,
        )
        .bind(&ids_vec)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|x| x.extract_string()).collect()
    }

    async fn store_article_data_not_lens_id(
        &self,
        batch: &[(String, Article)],
    ) -> Result<(), LensError> {
        if batch.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 5000;

        let mut tx = self.pool.begin().await?;

        let rough_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for chunk in batch.chunks(CHUNK_SIZE) {
            let rows: Vec<(String, String, i64)> = chunk
                .iter()
                .map(|(id, article)| {
                    let article_json = serde_json::to_string(article)?;
                    Ok((id.clone(), article_json, rough_timestamp))
                })
                .collect::<Result<Vec<_>, LensError>>()?;

            let mut builder = sqlx::QueryBuilder::new(
                "INSERT INTO article_data_not_lens_id (string_id, article_json, fetched_at) ",
            );

            builder.push_values(rows, |mut b, (id_str, article_json, timestamp)| {
                b.push_bind(id_str)
                    .push_bind(article_json)
                    .push_bind(timestamp);
            });

            builder.push(" ON CONFLICT (string_id) DO NOTHING");

            builder.build().execute(&mut *tx).await?;
        }

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

        sqlx::query("DELETE FROM article_references_not_lens_id")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM article_citations_not_lens_id")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM article_data")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM article_data_not_lens_id")
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}

impl PostgresBackend {
    /// Create a new PostgreSQL backend from a connection URL
    ///
    /// # Arguments
    /// * `url` - PostgreSQL connection URL (e.g., "postgres://user:pass@localhost/dbname")
    ///
    /// # Example
    /// ```ignore
    /// let backend = PostgresBackend::from_url("postgres://localhost/lens_cache").await?;
    /// ```
    pub async fn from_url(url: &str) -> Result<Self, LensError> {
        let pool = PgPool::connect(url).await?;
        let backend = Self { pool };
        backend.run_migrations().await?;
        backend.optimize_postgres().await?;

        Ok(backend)
    }

    /// Run database migrations (creates tables if they don't exist)
    async fn run_migrations(&self) -> Result<(), LensError> {
        // LensId tables (optimized with NoHasher)
        sqlx::query(
            r#"
            CREATE UNLOGGED TABLE IF NOT EXISTS article_references (
                lens_id TEXT PRIMARY KEY,
                references_json TEXT NOT NULL,
                fetched_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE UNLOGGED TABLE IF NOT EXISTS article_citations (
                lens_id TEXT PRIMARY KEY,
                citations_json TEXT NOT NULL,
                fetched_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create index on fetched_at for citations (useful for TTL queries)
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_citations_fetched ON article_citations(fetched_at)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE UNLOGGED TABLE IF NOT EXISTS article_data (
                lens_id TEXT PRIMARY KEY,
                article_json TEXT NOT NULL,
                fetched_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Non-LensId tables (for user input: PMID, DOI, etc.)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS article_references_not_lens_id (
                string_id TEXT PRIMARY KEY,
                references_json TEXT NOT NULL,
                fetched_at BIGINT NOT NULL DEFAULT extract(epoch from now())
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS article_citations_not_lens_id (
                string_id TEXT PRIMARY KEY,
                citations_json TEXT NOT NULL,
                fetched_at BIGINT NOT NULL DEFAULT extract(epoch from now())
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_citations_not_lens_id_fetched ON article_citations_not_lens_id(fetched_at)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS article_data_not_lens_id (
                string_id TEXT PRIMARY KEY,
                article_json TEXT NOT NULL,
                fetched_at BIGINT NOT NULL DEFAULT extract(epoch from now())
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Apply PostgreSQL-specific optimizations
    async fn optimize_postgres(&self) -> Result<(), LensError> {
        // Analyze tables to update statistics for the query planner
        // This is safe to run even if tables are empty
        let _ = sqlx::query("ANALYZE article_references")
            .execute(&self.pool)
            .await;

        let _ = sqlx::query("ANALYZE article_citations")
            .execute(&self.pool)
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lens::cache::compute_misses;

    /// Helper to create an isolated PostgreSQL backend for testing
    ///
    /// Each test gets its own schema with a unique timestamp-based name.
    /// This ensures complete isolation between tests, even across test runs.
    /// Old test schemas remain in the database but don't affect new tests.
    /// Similar to SQLite's `:memory:` - each test is completely isolated.
    ///
    /// Note: Test schemas accumulate in the database. Clean them up manually if needed:
    /// ```sql
    /// SELECT 'DROP SCHEMA IF EXISTS ' || schema_name || ' CASCADE;'
    /// FROM information_schema.schemata
    /// WHERE schema_name LIKE 'test_%';
    /// ```
    async fn create_test_backend() -> Result<PostgresBackend, LensError> {
        let url = std::env::var("TEST_POSTGRES_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres@localhost/lens_test".to_string());

        // Create unique schema name using timestamp + random component
        // This ensures uniqueness even across test runs
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let random = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let schema_name = format!("test_{timestamp}_{random}");

        // First connect to create the schema
        let pool = PgPool::connect(&url).await.map_err(LensError::SqlxError)?;

        // Create isolated schema for this test
        sqlx::query(&format!("CREATE SCHEMA {schema_name}"))
            .execute(&pool)
            .await?;

        pool.close().await;

        // Reconnect with schema in the connection string
        // This ensures ALL connections from the pool use this schema
        let url_with_schema = format!("{url}?options=-c%20search_path%3D{schema_name}");
        let pool = PgPool::connect(&url_with_schema)
            .await
            .map_err(LensError::SqlxError)?;

        // Run migrations in the isolated schema
        let backend = PostgresBackend { pool };
        backend.run_migrations().await?;

        Ok(backend)
    }

    // Note: These tests require a running PostgreSQL instance
    // Set TEST_POSTGRES_DATABASE_URL environment variable to run them
    // Example: TEST_POSTGRES_DATABASE_URL=postgres://postgres:password@localhost/lens_test
    //
    // Each test runs in its own schema, so they can run in parallel without interference!

    #[tokio::test]
    async fn test_store_and_get_references() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

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
        let result = backend.get_references(std::slice::from_ref(&id3)).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_references_immutable() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

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
        let result = backend.get_references(std::slice::from_ref(&id1)).await?;
        assert_eq!(result.get(&id1).unwrap(), &refs1);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_get_citations() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

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
        let result = backend.get_citations(std::slice::from_ref(&id3)).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_citations_updates_on_conflict() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

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
        let result = backend.get_citations(std::slice::from_ref(&id1)).await?;
        assert_eq!(result.get(&id1).unwrap(), &cites2);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_input_handling() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

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
        let backend = create_test_backend().await?;

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
        let refs_result = backend.get_references(std::slice::from_ref(&id1)).await?;
        let cites_result = backend.get_citations(std::slice::from_ref(&id2)).await?;
        assert_eq!(refs_result.len(), 1);
        assert_eq!(cites_result.len(), 1);

        // Clear all data
        backend.clear().await?;

        // Verify data is gone
        let refs_result = backend.get_references(std::slice::from_ref(&id1)).await?;
        let cites_result = backend.get_citations(std::slice::from_ref(&id2)).await?;
        assert_eq!(refs_result.len(), 0);
        assert_eq!(cites_result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_bulk_insert_chunking() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

        // Create a batch larger than SQLite's CHUNK_SIZE (333) but smaller than Postgres's (5000)
        let mut batch = Vec::new();
        for i in 0..1000 {
            let id = LensId::from(10000000000000 + i);
            let refs = vec![LensId::from(i), LensId::from(i + 1)];
            batch.push((id, refs));
        }

        // Store large batch
        backend.store_references(&batch).await?;

        // Verify all were stored
        let ids: Vec<LensId> = batch.iter().map(|(id, _)| id.clone()).collect();
        let result = backend.get_references(&ids).await?;
        assert_eq!(result.len(), 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_compute_misses() -> Result<(), LensError> {
        let backend = create_test_backend().await?;

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

    #[tokio::test]
    async fn test_store_and_get_article_data() -> Result<(), LensError> {
        use crate::lens::article::{Author, ExternalIds, Source};

        let backend = create_test_backend().await?;

        // Create test articles
        let id1 = LensId::from(12345678901234);
        let id2 = LensId::from(98765432109876);
        let id3 = LensId::from(11111111111111);

        let article1 = Article {
            lens_id: id1.clone(),
            title: Some("Test Article 1".to_string()),
            summary: Some("This is a test abstract".to_string()),
            scholarly_citations_count: Some(42),
            external_ids: Some(ExternalIds {
                pmid: vec!["12345".to_string()],
                doi: vec!["10.1234/test".to_string()],
                coreid: vec![],
                pmcid: vec![],
                magid: vec![],
            }),
            authors: Some(vec![Author {
                first_name: Some("John".to_string()),
                initials: Some("J".to_string()),
                last_name: Some("Doe".to_string()),
            }]),
            source: Some(Source {
                publisher: Some("Test Publisher".to_string()),
                title: Some("Test Journal".to_string()),
                kind: Some("journal".to_string()),
            }),
            year_published: Some(2023),
        };

        let article2 = Article {
            lens_id: id2.clone(),
            title: Some("Test Article 2".to_string()),
            summary: None,
            scholarly_citations_count: Some(10),
            external_ids: None,
            authors: None,
            source: None,
            year_published: Some(2024),
        };

        let batch = vec![(id1.clone(), article1), (id2.clone(), article2)];

        // Store articles
        backend.store_article_data(&batch).await?;

        // Retrieve articles
        let result = backend
            .get_article_data(&[id1.clone(), id2.clone()])
            .await?;

        assert_eq!(result.len(), 2);

        let retrieved1 = result.get(&id1).unwrap();
        assert_eq!(retrieved1.title, Some("Test Article 1".to_string()));
        assert_eq!(retrieved1.scholarly_citations_count, Some(42));
        assert_eq!(retrieved1.year_published, Some(2023));
        assert!(retrieved1.external_ids.is_some());

        let retrieved2 = result.get(&id2).unwrap();
        assert_eq!(retrieved2.title, Some("Test Article 2".to_string()));
        assert_eq!(retrieved2.year_published, Some(2024));

        // Query non-existent ID
        let result = backend.get_article_data(std::slice::from_ref(&id3)).await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }
}
