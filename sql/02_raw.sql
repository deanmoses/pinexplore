-- Raw tables generated from source data files, generally JSON.
-- Tables (not views) so JSON is parsed once at build time.
--
-- Requires `SET VARIABLE ingest_base = '<path-or-url>'`, which
-- scripts/rebuild_explore.py sets -- to 'ingest_sources' locally,
-- or to the R2 URL in --remote mode.

------------------------------------------------------------
-- External data source dumps
------------------------------------------------------------

-- Fandom wiki exports
CREATE OR REPLACE TABLE fandom_raw.games AS
SELECT d.*
FROM (SELECT unnest(games) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom/fandom_games.json'));

CREATE OR REPLACE TABLE fandom_raw.manufacturers AS
SELECT d.*
FROM (SELECT unnest(manufacturers) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom/fandom_manufacturers.json'));

-- `people`, not the source's `persons`: the catalog spells this entity's plural
-- `people`.
CREATE OR REPLACE TABLE fandom_raw.people AS
SELECT d.*
FROM (SELECT unnest(persons) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom/fandom_persons.json'));

-- OPDB (Open Pinball Database) export.
--
-- This is a direct db dump + changelog from OPDB, not a scrape.
-- Every row is dated with `updatedAt`;  the changelog says where a vanished id went.
-- When updating, replace existing dump + changelog files; no need to look at older files, unlike how IPDB works.

--
-- `d.*` because this is upstream's file and it gains fields unannounced: the
-- star carries a new one through to the mart, where `opdb_column_not_snake_case`
-- fails the build until `09_mart.sql` names it.
CREATE OR REPLACE TABLE opdb_raw.machine_groups AS
SELECT d.*
FROM (SELECT unnest(machineGroups) AS d FROM read_json_auto(getvariable('ingest_base') || '/opdb/opdb_full.json'));

CREATE OR REPLACE TABLE opdb_raw.machines AS
SELECT d.*
FROM (SELECT unnest(machines) AS d FROM read_json_auto(getvariable('ingest_base') || '/opdb/opdb_full.json'));

-- Aliases carry a subset of a machine's fields; `opdb_stg.machines` unions them
-- back with the machines.
CREATE OR REPLACE TABLE opdb_raw.aliases AS
SELECT d.*
FROM (SELECT unnest(aliases) AS d FROM read_json_auto(getvariable('ingest_base') || '/opdb/opdb_full.json'));

-- Every id OPDB has retired and what replaced it. CUMULATIVE -- each download
-- restates the whole history -- and downloaded separately from the export, so it
-- normally runs AHEAD and may retire an id the export still lists.
-- `08_source_warning_checks.sql` watches that gap.
CREATE OR REPLACE TABLE opdb_raw.changelog AS
SELECT d.*
FROM (SELECT unnest(data) AS d FROM read_json_auto(getvariable('ingest_base') || '/opdb/opdb_changelog.json'));

-- IPDB (Internet Pinball Database) export — a scrape published as JSON at
-- https://github.com/xantari/Ipdb.Database.
--
-- These are periodic full snapshots kept side by side rather than overwritten, because
-- we're seeing that newer scrapes can omit records (often sequential records, which would
-- indicate a temporary issue like networking);
-- merging them into one row per model is `ipdb_stg.models_merged`.
--
-- Best practice: name a snapshot file for its own `LastRefreshDateUtc`, NOT for the day
-- we downloaded it. Xantari refreshes on an infrequent schedule, so the two can be a
-- year apart.
--
-- Listed explicitly rather than globbed: a glob cannot be expanded over HTTP,
-- which --remote needs, and an explicit list stops a stray file joining the
-- union. Adding a snapshot means editing here AND `make push`, or a fresh
-- checkout will not reproduce the build.
--
-- `union_by_name` because the records are sparse -- two snapshots need not agree
-- on the column set, and a future one may add or drop keys.
CREATE OR REPLACE TABLE ipdb_raw.xantari_model_snapshots AS
SELECT
  -- From the file's header rather than its name, so the date is the scrape's
  -- claim about itself.
  CAST(LastRefreshDateUtc AS TIMESTAMP) AS snapshot_utc,
  d.*
FROM (
  SELECT LastRefreshDateUtc, unnest("Data") AS d
  FROM read_json_auto(
    [
      getvariable('ingest_base') || '/ipdb/xantari/2025_02_01.json',
      getvariable('ingest_base') || '/ipdb/xantari/2026_04_11.json'
    ],
    (maximum_object_size = 67108864),
    (union_by_name = true)
  )
);

-- IPDB machine pages, parsed -- the archive.org side of IPDB.
--
-- Written by `scripts/web_scrape/extract_ipdb_to_jsonl.py` over the machine
-- pages in the web cache, one object per model. Where the xantari export is
-- IPDB's DATABASE in bulk, this is IPDB's rendered PAGE read back, carrying what
-- the export never modelled: a labelled `Project Date` row, a `Production`
-- status, `Specialty`, `Concept by`.
--
-- `sample_size = -1` is not a tuning knob. The rarest fields here sit on a
-- handful of models, so a sampled read types a real struct as NULL and drops it
-- with nothing raising.
--
-- Columns are ENUMERATED where the xantari read above stars. That file is
-- upstream's and gains fields unannounced, so a star is what stops a new field
-- being dropped; this one is ours, so enumerating makes a vanished key fail here
-- rather than downstream.
CREATE OR REPLACE TABLE ipdb_raw.archive_models AS
SELECT
  ipdb_id,
  "name",
  players,
  -- Four date carriers, not interchangeable. `manufacture_date` and
  -- `project_date` are IPDB's own labelled rows; `header_date` is the line
  -- xantari captured into `AdditionalDetails`; `date` is the parser's
  -- manufacture-else-project pick, with `date_source` naming which it took.
  "date",
  date_source,
  header_date,
  manufacture_date,
  project_date,
  manufacturer,
  model_number,
  common_abbreviations,
  mpu,
  type_code,
  type_text,
  production,
  rating,
  themes,
  specialties,
  notable_features,
  toys,
  easter_eggs,
  notes,
  marketing_slogans,
  photos_in,
  "source",
  credits,
  rule_sheets,
  additional_media,
  serial_number_database_url,
  owners_list_url,
  documents,
  -- Labelled page rows the parser has no case for. Empty on every row is
  -- healthy, and it infers as JSON[] then, so ask `len(...) > 0` before reaching
  -- into an element.
  unknown_fields,
  -- `source_url` is the address a patch cites; `raw_url` is where the bytes came
  -- from, a Wayback capture for most of these.
  source_url,
  raw_url,
  archive_capture_date,
  content_sha,
  last_fetched_at
FROM read_json_auto(
  getvariable('ingest_base') || '/ipdb/web_cache/models.jsonl',
  (sample_size = -1)
);

-- IPDB's advanced searches, saved by hand past a bot wall and parsed by
-- `scripts/ipdb/extract_ipdb_searches_to_jsonl.py`.
--
-- A live read of IPDB, months newer than the dump, and the build checks the dump
-- against it. These are not rival field values: `ipdb.models` states what
-- xantari states, bar a title the scrape lost characters from.
--
-- NOT MERGED. A model matched by three searches is three rows, and whether the
-- copies agree is what `ipdb_live_observation_conflict` asks; `live_models`
-- collapses them only because it does.
--
-- What a row's ABSENCE means differs per search, and the page does not always
-- say what it filtered on, so nothing here infers from it. The two cases where
-- absence is meaningful are proven rather than assumed, by
-- `ipdb_specialty_search_incomplete` and `ipdb_dated_model_not_in_year_search`.
CREATE OR REPLACE TABLE ipdb_raw.search_results AS
SELECT
  -- Where the page was saved, and what it filtered on where it says so.
  -- `search_filter` is NULL on the Type and year searches, which echo no form
  -- back; only the Specialty pages state theirs, and they are the only ones a
  -- check needs it from.
  search_kind,
  search_name,
  search_filter,
  ipdb_id,
  "name",
  -- IPDB's date column, split rather than cast to a DATE: it states a month on
  -- most rows and a bare year on hundreds, and a DATE would pad the missing part
  -- to the 1st and read as a day IPDB never stated. `date_is_project_date` is
  -- the `*` IPDB prints to mark a Project Date -- the same distinction
  -- `ipdb_stg.archive_models` goes to the model page for, stated here in
  -- listings that cover thousands of models rather than hundreds.
  date_text,
  date_year,
  date_month,
  date_is_project_date,
  manufacturer,
  manufacturer_full,
  type_code,
  type_text,
  -- `production_text` is kept beside the integer because only the text separates
  -- IPDB saying "none", "few" or "unknown" from IPDB saying nothing; none of
  -- those has an integer.
  production_text,
  production_units,
  production_approximate,
  players,
  model_number,
  n_photos,
  rating_score,
  rating_ratings,
  rating_provisional,
  -- Every results row states the model's WHOLE Specialty set, whatever the page
  -- filtered on, so this is as complete on a year page as on a Specialty one.
  specialties
FROM read_json_auto(
  getvariable('ingest_base') || '/ipdb/searches/search_results.jsonl',
  (sample_size = -1)
);

-- IPDB's Specialties, read off the search form rather than transcribed.
--
-- The live list, riding along with the data read under it. It exists so
-- `ipdb_ref.specialty` -- which is hand-written, and maps each Specialty onto
-- catalog vocabulary -- can be checked against IPDB rather than against someone's
-- memory of IPDB. `ipdb_specialty_list_drifted` compares them both ways, so a
-- Specialty IPDB adds after this download fails the build instead of going
-- silently unmapped.
--
-- Not derivable from the search results: a Specialty no model currently carries
-- appears in the dropdown and nowhere else, which is exactly the case worth
-- catching. `downloaded` is false where no saved page searched for the term.
CREATE OR REPLACE TABLE ipdb_raw.specialties AS
SELECT
  specialty_id,
  specialty,
  source_url,
  downloaded
FROM read_json_auto(
  getvariable('ingest_base') || '/ipdb/searches/specialties.jsonl',
  (sample_size = -1)
);

-- Pinball glossaries, published straight out of the raw layer: nothing
-- transforms them. The one derived view over them is `glossary.compared`.
CREATE OR REPLACE TABLE glossary.ipdb AS
SELECT
  slug,
  "name",
  definition,
  see_also,
  aliases,
  games,
FROM read_json_auto(getvariable('ingest_base') || '/glossary/glossary_ipdb/ipdb_glossary.json', (union_by_name = true));

CREATE OR REPLACE TABLE glossary.kineticist AS
SELECT *
FROM read_json_auto(getvariable('ingest_base') || '/glossary/glossary_kineticist/kineticist_glossary.json');

CREATE OR REPLACE TABLE glossary.pinball_primer AS
SELECT *
FROM read_json_auto(getvariable('ingest_base') || '/glossary/glossary_pinball_primer/pinball_primer_glossary.json');
