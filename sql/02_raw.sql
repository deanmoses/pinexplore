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
      getvariable('ingest_base') || '/ipdb/ipdb_xantari_2025_02_01.json',
      getvariable('ingest_base') || '/ipdb/ipdb_xantari_2026_04_11.json'
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
  getvariable('ingest_base') || '/ipdb/ipdb_archive/models.jsonl',
  (sample_size = -1)
);

-- IPDB's Specialty census -- the advanced search, run once per Specialty.
--
-- Written by `scripts/ipdb/extract_ipdb_specialty_to_jsonl.py` over pages saved
-- by hand from <https://www.ipdb.org/search.pl?searchtype=advanced>, which is
-- behind a bot wall. That module's docstring holds the shape and the four
-- properties of the download it asserts; what matters here is why this source
-- outranks the other two on this one field.
--
-- It is a CENSUS, not a sample. Each search lists every machine IPDB currently
-- classifies under one Specialty, and every result row states the machine's
-- WHOLE specialty set, so the union across the searches is IPDB's complete
-- classification at one moment. That makes absence meaningful: a machine not
-- here has no specialty, which is a fact rather than a gap to backfill.
--
-- Neither other IPDB read can say that. The xantari dump has never carried the
-- field, and the archive pages carry it a page at a time from captures spanning
-- years. So nothing merges: the census REPLACES both as the source of
-- `ipdb.model_specialties`, rather than ranking against them per model. The two
-- ways that could stop being true both fail the build --
-- `ipdb_specialty_xantari_column_appeared` if the dump gains the field, and
-- `ipdb_specialty_vocabulary_drifted` if IPDB's vocabulary moves under the
-- download.
--
-- Columns are enumerated for the same reason `archive_models` above enumerates:
-- this file is ours, so a vanished key should fail here rather than downstream.
CREATE OR REPLACE TABLE ipdb_raw.specialty_census AS
SELECT
  ipdb_id,
  "name",
  -- IPDB's date column, split rather than cast to a DATE: it states a month on
  -- most rows and a bare year on hundreds, and a DATE would pad the missing part
  -- to the 1st and read as a day IPDB never stated. `date_is_project_date` is
  -- the `*` IPDB prints to mark a Project Date -- the same distinction
  -- `ipdb_stg.archive_models` goes to the machine page for, stated here in a
  -- listing that covers thousands of models rather than hundreds.
  date_text,
  date_year,
  date_month,
  date_is_project_date,
  manufacturer,
  manufacturer_full,
  type_code,
  type_text,
  -- `production_text` is kept beside the integer because only the text separates
  -- IPDB saying "none" or "few" from IPDB saying nothing; both have no integer.
  production_text,
  production_units,
  production_approximate,
  players,
  model_number,
  n_photos,
  rating_score,
  rating_ratings,
  rating_provisional,
  -- One struct per assignment, each carrying the id and search URL that evidence
  -- it. `ipdb_stg.model_specialties` unnests them.
  specialties
FROM read_json_auto(
  getvariable('ingest_base') || '/ipdb/ipdb_specialty/census.jsonl',
  (sample_size = -1)
);

-- IPDB's other advanced searches, as saved observations.
--
-- Written by `scripts/ipdb/extract_ipdb_searches_to_jsonl.py` over every saved
-- search page EXCEPT the Specialty ones, which the census above owns. One row
-- per machine per search, so a machine matched by two searches is observed
-- twice; `ipdb_stg.live_observations` unions these with the census and
-- `ipdb_live_observation_conflict` asserts the copies agree.
--
-- These are LIVE READS, months newer than the dump, and their whole job is to
-- give the build something current to check the dump against. They publish no
-- rival field values -- `ipdb.models` still states what xantari states.
--
-- What a search's absence means is NOT recorded, deliberately. Each download is
-- complete for what it filtered on, but the page does not always say what that
-- was (the Type search echoes no filter back), so the corpus holds positive
-- observations only and nothing infers from a machine's absence.
--
-- The `years` searches are the exception, and they earn it COLLECTIVELY rather
-- than one page at a time: tiled 1800 to 2026 without a gap, they match every
-- machine IPDB dates, and a machine IPDB does not date cannot appear in any of
-- them. That makes them complete over the dated universe, which is what
-- `ipdb_dated_model_not_in_year_search` asserts. The property belongs here in
-- SQL rather than in the extract, which reads folders and cannot know what any
-- of them tile.
CREATE OR REPLACE TABLE ipdb_raw.search_observations AS
SELECT
  -- The folder the page was saved in, and the file within it. Provenance for a
  -- filter the page itself may not state.
  search_kind,
  search_name,
  ipdb_id,
  "name",
  date_text,
  date_year,
  date_month,
  date_is_project_date,
  manufacturer,
  manufacturer_full,
  type_code,
  type_text,
  production_text,
  production_units,
  production_approximate,
  players,
  model_number,
  n_photos,
  rating_score,
  rating_ratings,
  rating_provisional,
  -- Bare specialty strings here, where the census carries structs: this corpus
  -- proves nothing about completeness, so a row is a sighting rather than an
  -- assignment and has no search URL to cite.
  specialties
FROM read_json_auto(
  getvariable('ingest_base') || '/ipdb/ipdb_searches/observations.jsonl',
  (sample_size = -1)
);

-- IPDB's Specialty dropdown as the census download found it.
--
-- The live vocabulary, riding along with the data that was read under it. It
-- exists so `ipdb_ref.specialty` -- which is hand-written, and maps each
-- Specialty onto catalog vocabulary -- can be checked against IPDB rather than
-- against someone's memory of IPDB. `ipdb_specialty_vocabulary_drifted` compares
-- them in both directions, so a Specialty IPDB adds after this download fails
-- the build instead of going silently unmapped.
--
-- `downloaded` is false for a term whose own search page was never saved. No
-- such row today, and the extract refuses to write a census where one is
-- reachable from another page's rows.
CREATE OR REPLACE TABLE ipdb_raw.specialty_vocabulary AS
SELECT
  specialty_id,
  specialty,
  source_url,
  downloaded
FROM read_json_auto(
  getvariable('ingest_base') || '/ipdb/ipdb_specialty/vocabulary.jsonl',
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
