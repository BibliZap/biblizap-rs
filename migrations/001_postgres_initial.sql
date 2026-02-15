-- Initial schema for PostgreSQL cache
-- This stores citations, references, and article metadata as JSON

CREATE TABLE IF NOT EXISTS articles_cache (
    lens_id TEXT PRIMARY KEY,

    -- JSONB for efficient querying (PostgreSQL specific)
    citations_json TEXT,          -- JSON array: ["id1", "id2", ...] - who cites this article
    references_json TEXT,         -- JSON array: ["id1", "id2", ...] - who this article references

    -- Complete article metadata as JSON
    article_json TEXT,

    -- Timestamp for cache invalidation (using BIGINT for consistency)
    cached_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);

-- Index for cache cleanup queries
CREATE INDEX IF NOT EXISTS idx_cached_at ON articles_cache(cached_at);

-- Optional: GIN indexes for JSONB querying (if you convert to JSONB later)
-- CREATE INDEX IF NOT EXISTS idx_citations_gin ON articles_cache USING GIN (citations_json);
-- CREATE INDEX IF NOT EXISTS idx_references_gin ON articles_cache USING GIN (references_json);

-- PostgreSQL-specific optimizations
-- These are set at the database level, not in migrations typically
-- But included here for reference:

-- For better concurrent write performance:
-- ALTER TABLE articles_cache SET (fillfactor = 90);

-- Consider adding partitioning if the cache grows very large:
-- CREATE TABLE articles_cache_2024 PARTITION OF articles_cache
--     FOR VALUES FROM ('2024-01-01'::timestamp) TO ('2025-01-01'::timestamp);
