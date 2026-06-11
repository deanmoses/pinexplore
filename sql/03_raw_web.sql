-- Raw tables generated from the SQLite cache of scraped web pages.
--
-- This is raw source: later layers can join fetched web evidence 
-- against the catalog. See docs/WebCache.md.
--
-- This layer is LOCAL-ONLY. rebuild_explore.py skips it in --remote mode
-- (you can't httpfs-ATTACH a SQLite file over R2) and when the local cache file
-- is absent (a fresh checkout with no web fetches yet). The build always pulls
-- from R2 to local first, then builds locally — so the path is the fixed local
-- one (ingest_base is always 'ingest_sources' in local mode; ATTACH needs a
-- literal, not an expression).

INSTALL sqlite;
LOAD sqlite;

ATTACH 'ingest_sources/web/cache.sqlite' AS we (TYPE sqlite, READ_ONLY);

CREATE OR REPLACE TABLE web_pages   AS SELECT * FROM we.pages;
CREATE OR REPLACE TABLE web_fetches AS SELECT * FROM we.fetches;

DETACH we;
