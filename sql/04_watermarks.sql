-- What we ingested, and when each artifact claims it was taken.
--
-- Reads raw directly: a watermark is a fact about the artifact, so it must not
-- depend on the layers whose staleness it reports.
--
-- Published in the mart schemas because flippatch needs it, and the mart is the
-- only thing another repo may read.

-- Per ARTIFACT, not per source: IPDB arrives as several Xantari snapshots that
-- staging merges, and "which snapshots went in" is the question this answers.
-- `observed_at` is the scrape's own `LastRefreshDateUtc` -- its claim about its
-- currency, not the date we read it.
CREATE OR REPLACE VIEW ipdb.ingest_watermarks AS
SELECT
  'xantari snapshot'                 AS artifact_kind,
  strftime(snapshot_utc, '%Y-%m-%d') AS artifact,
  snapshot_utc                       AS observed_at,
  count(*)                           AS n_records
FROM ipdb_raw.xantari_model_snapshots
GROUP BY snapshot_utc

UNION ALL

-- The parsed archive.org corpus, one row for the whole file.
--
-- `observed_at` is weaker here than above. This artifact is hundreds of separate
-- captures spanning more than a decade, so no single date describes its
-- contents -- this records only when the corpus last GREW. The date bearing on a
-- given model is `archive_capture_date`, on that model's own row.
SELECT
  'archive page extract',
  'ipdb_archive/models.jsonl',
  max(last_fetched_at),
  count(*)
FROM ipdb_raw.archive_models;


-- Every ingested artifact, one row each, whatever source it came from.
--
-- `observed_at` is NULL where the artifact carries no claim about its own
-- currency -- an honest absence, not a value to fill from the file's mtime.
CREATE OR REPLACE VIEW ingest.watermarks AS
SELECT 'ipdb' AS source, artifact_kind, artifact, observed_at, n_records
FROM ipdb.ingest_watermarks
UNION ALL
SELECT 'opdb', 'export', 'opdb_export_machines.json', NULL, count(*) FROM opdb_raw.machines
UNION ALL
SELECT 'opdb', 'export', 'opdb_export_groups.json', NULL, count(*) FROM opdb_raw.groups
UNION ALL
SELECT 'fandom', 'export', 'fandom_games.json', NULL, count(*) FROM fandom_raw.games
UNION ALL
SELECT 'fandom', 'export', 'fandom_manufacturers.json', NULL, count(*) FROM fandom_raw.manufacturers
UNION ALL
SELECT 'fandom', 'export', 'fandom_persons.json', NULL, count(*) FROM fandom_raw.people
UNION ALL
SELECT 'glossary', 'parsed html', 'ipdb_glossary.json', NULL, count(*) FROM glossary.ipdb
UNION ALL
SELECT 'glossary', 'parsed html', 'kineticist_glossary.json', NULL, count(*) FROM glossary.kineticist
UNION ALL
SELECT 'glossary', 'parsed html', 'pinball_primer_glossary.json', NULL, count(*) FROM glossary.pinball_primer;
