-- Initial schema for SQLite cache
-- This stores citations, references, and article metadata as JSON

CREATE TABLE IF NOT EXISTS articles_cache (
    lens_id TEXT PRIMARY KEY,

    -- JSON arrays of lens_id strings
    citations_json TEXT,          -- ["id1", "id2", ...] - who cites this article
    references_json TEXT,         -- ["id1", "id2", ...] - who this article references

    -- Complete article metadata as JSON
    article_json TEXT,

    -- Timestamp for cache invalidation
    cached_at INTEGER DEFAULT (unixepoch())
) WITHOUT ROWID;

-- Index for cache cleanup queries
CREATE INDEX IF NOT EXISTS idx_cached_at ON articles_cache(cached_at);

-- SQLite optimizations for concurrent reads
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -64000;  -- 64MB cache
PRAGMA busy_timeout = 5000;  -- 5 second timeout for lock contention
