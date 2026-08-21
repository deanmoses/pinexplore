-- The schemas every later layer builds into. Nothing lands in `main`.
--
-- Plain CREATEs rather than IF NOT EXISTS: the build drops and recreates the
-- database file every run, so a schema surviving a previous build would mean the
-- drop failed.

CREATE SCHEMA ipdb_raw;
CREATE SCHEMA ipdb_ref;
CREATE SCHEMA ipdb_stg;
CREATE SCHEMA ipdb;

CREATE SCHEMA opdb_raw;
CREATE SCHEMA opdb_ref;
CREATE SCHEMA opdb;

CREATE SCHEMA fandom_raw;
CREATE SCHEMA fandom_stg;

-- The glossaries arrive parsed and are published unchanged, so raw and mart coincide.
CREATE SCHEMA glossary;

-- Materialized from the web-scrape SQLite by the local-only 03 layer.
CREATE SCHEMA web_cache;

CREATE SCHEMA ingest;

CREATE SCHEMA checks;
