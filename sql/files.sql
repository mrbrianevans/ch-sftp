CREATE TABLE IF NOT EXISTS files (
     path TEXT PRIMARY KEY,
     size_bytes BIGINT,
     last_modified timestamp,
    crawled_at timestamp,
    ingested_at timestamp
)