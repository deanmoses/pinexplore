-- Raw tables generated from the SQLite cache of scraped web pages.
--
-- Materialized for ad hoc queries alongside the source dumps. No SQL layer reads
-- them; `scripts/web_scrape/web_cache.py` is how the cache is normally searched.
-- See docs/WebCache.md.
--
-- LOCAL-ONLY. rebuild_explore.py skips this layer in --remote mode (you cannot
-- httpfs-ATTACH a SQLite file over R2) and when the cache file is absent. The
-- path is a fixed literal because ATTACH takes no expression.

INSTALL sqlite;
LOAD sqlite;

ATTACH 'ingest_sources/web/cache.sqlite' AS we (TYPE sqlite, READ_ONLY);

CREATE OR REPLACE TABLE web_cache.pages   AS SELECT * FROM we.pages;
CREATE OR REPLACE TABLE web_cache.fetches AS SELECT * FROM we.fetches;

DETACH we;
