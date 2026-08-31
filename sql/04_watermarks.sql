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
  'ipdb/web_cache/models.jsonl',
  max(last_fetched_at),
  count(*)
FROM ipdb_raw.archive_models

UNION ALL

-- The saved advanced searches, one row for the whole extract.
--
-- `observed_at` is NULL and cannot be otherwise: a results page carries no
-- timestamp, so the artifact makes no claim about its own currency. What dates
-- it is `acquired_on` below, and that is a SIMPLIFICATION rather than a fact --
-- searches are saved whenever one is wanted, so the corpus spans however long
-- that has been going on and one date describes the newest download.
SELECT
  'advanced search results',
  'ipdb/searches/search_results.jsonl',
  NULL,
  count(*)
FROM ipdb_raw.search_results;
COMMENT ON VIEW ipdb.ingest_watermarks IS
  'One row per ingested IPDB artifact with its record count and best available watermark; archive extracts use latest fetch time.';


-- When each MANUALLY DOWNLOADED artifact was acquired -- a fact about our act,
-- distinct from `observed_at`, which is the artifact's claim about itself. The
-- OPDB files carry no self-date, so without this row their currency is
-- unknowable from inside the database.
--
-- Hand-maintained: update the date AND the record count together when dropping
-- in a new download. The count is the forget-tripwire --
-- `checks.artifact_acquisition_log_stale` warns when it stops matching the
-- artifact, which a new download almost always makes it do -- and until it matches
-- again, `ingest.watermarks` withholds the date rather than publish one that
-- describes a previous download. Not derived from file mtime: the files sync
-- through R2, and mtime on another machine dates the sync, not the download.
--
-- Defined here rather than in 05_reference because this file runs first and is
-- the only consumer; the `ref` schema still marks it curated and internal.
CREATE OR REPLACE VIEW ref.artifact_acquisitions AS
SELECT * FROM (VALUES
  ('opdb/opdb_full.json',      DATE '2026-08-22', 4136),
  ('opdb/opdb_changelog.json', DATE '2026-08-22', 51),
  -- Every page was saved the same afternoon, so the date describes the whole
  -- corpus rather than standing in for a range. This row carries more weight
  -- than the OPDB ones above it: `ipdb.model_specialties` publishes it as
  -- `observed_on`, making it the date a patch cites for every Specialty claim,
  -- and there is nowhere else it could come from.
  ('ipdb/searches/search_results.jsonl', DATE '2026-08-30', 11824)
) AS t(artifact, acquired_on, n_records_at_acquisition);

-- Every ingested artifact, one row each, whatever source it came from.
--
-- `observed_at` is NULL where the artifact carries no claim about its own
-- currency -- an honest absence, not a value to fill from the file's mtime.
-- `acquired_on` is NULL where nobody recorded the acquisition, and ALSO where
-- the recorded count no longer matches the artifact -- a log row that stopped
-- matching describes some previous download, and a confidently wrong date is
-- worse than the NULL it would replace. The count gate lives in the join here;
-- the warning that someone should update the log joins on artifact alone, in
-- `checks.artifact_acquisition_log_stale`.
CREATE OR REPLACE VIEW ingest.watermarks AS
SELECT w.source, w.artifact_kind, w.artifact, w.observed_at,
       a.acquired_on, w.n_records
FROM (
SELECT 'ipdb' AS source, artifact_kind, artifact, observed_at, n_records
FROM ipdb.ingest_watermarks
UNION ALL
-- The export states no date about itself and its path is undated, so there is no
-- honest `observed_at`. What stands in is per-row: `updated_at` on
-- `opdb_stg.machines` is OPDB's own dating of each record, and its maximum is a
-- FLOOR on when the export was taken -- not published here, because a floor read
-- as a date is worse than an absence.
SELECT 'opdb', 'export', 'opdb/opdb_full.json', NULL,
  (SELECT count(*) FROM opdb_raw.machine_groups)
    + (SELECT count(*) FROM opdb_raw.machines)
    + (SELECT count(*) FROM opdb_raw.aliases)
UNION ALL
-- Downloaded separately from the export. Might be NEWER. `created_at` here IS
-- OPDB's own claim -- the newest retirement it knows of -- which makes this the
-- one OPDB artifact carrying a real watermark.
SELECT 'opdb', 'id changelog', 'opdb/opdb_changelog.json',
  (SELECT CAST(max(createdAt) AS TIMESTAMP) FROM opdb_raw.changelog),
  (SELECT count(*) FROM opdb_raw.changelog)
UNION ALL
SELECT 'fandom', 'export', 'fandom/fandom_games.json', NULL, count(*) FROM fandom_raw.games
UNION ALL
SELECT 'fandom', 'export', 'fandom/fandom_manufacturers.json', NULL, count(*) FROM fandom_raw.manufacturers
UNION ALL
SELECT 'fandom', 'export', 'fandom/fandom_persons.json', NULL, count(*) FROM fandom_raw.people
UNION ALL
SELECT 'glossary', 'parsed html', 'ipdb_glossary.json', NULL, count(*) FROM glossary.ipdb
UNION ALL
SELECT 'glossary', 'parsed html', 'kineticist_glossary.json', NULL, count(*) FROM glossary.kineticist
UNION ALL
SELECT 'glossary', 'parsed html', 'pinball_primer_glossary.json', NULL, count(*) FROM glossary.pinball_primer
) AS w
LEFT JOIN ref.artifact_acquisitions AS a
  ON a.artifact = w.artifact
 AND a.n_records_at_acquisition = w.n_records;
COMMENT ON VIEW ingest.watermarks IS
  'One row per ingested artifact with its record count, the artifact''s own claimed date (observed_at, NULL when it makes none) and the recorded manual-download date (acquired_on, NULL when unrecorded or when the acquisition log no longer matches the artifact).';
