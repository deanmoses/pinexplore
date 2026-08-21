-- Raw tables generated from all JSON source data files.
-- Tables (not views) so JSON is parsed once at build time.
--
-- Requires `SET VARIABLE ingest_base = '<path-or-url>'`, which
-- scripts/rebuild_explore.py sets -- to 'ingest_sources' locally, or to the R2
-- URL in --remote mode.

------------------------------------------------------------
-- External source dumps
------------------------------------------------------------

-- Fandom wiki exports
CREATE OR REPLACE TABLE fandom_raw.games AS
SELECT d.*
FROM (SELECT unnest(games) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom_games.json'));

CREATE OR REPLACE TABLE fandom_raw.manufacturers AS
SELECT d.*
FROM (SELECT unnest(manufacturers) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom_manufacturers.json'));

-- `people`, not the source's `persons`: the catalog spells this entity's plural
-- `people`.
CREATE OR REPLACE TABLE fandom_raw.people AS
SELECT d.*
FROM (SELECT unnest(persons) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom_persons.json'));

-- OPDB (Open Pinball Database) exports
CREATE OR REPLACE TABLE opdb_raw.groups AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/opdb_export_groups.json');

CREATE OR REPLACE TABLE opdb_raw.machines AS
SELECT
  opdb_id,
  split_part(opdb_id, '-', 1) AS group_id,
  split_part(opdb_id, '-', 2) AS machine_id,
  CASE
    WHEN split_part(opdb_id, '-', 3) = '' THEN NULL
    ELSE split_part(opdb_id, '-', 3)
  END AS alias_id,
  is_machine,
  is_alias,
  "name",
  common_name,
  shortname,
  physical_machine,
  ipdb_id,
  manufacture_date,
  manufacturer,
  "type",
  display,
  player_count,
  features,
  keywords,
  description,
  created_at,
  updated_at,
  images
FROM read_json_auto(getvariable('ingest_base') || '/opdb_export_machines.json');

-- IPDB (Internet Pinball Database) export — xantari/Ipdb.Database scrape.
--
-- Periodic full snapshots kept side by side rather than overwritten; merging
-- them into one row per model is `ipdb_stg.models_merged`.
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
      getvariable('ingest_base') || '/ipdb_xantari.json',
      getvariable('ingest_base') || '/ipdb_xantari_2026_08_19.json'
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
  getvariable('ingest_base') || '/ipdb_archive/models.jsonl',
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
FROM read_json_auto(getvariable('ingest_base') || '/glossary_ipdb/ipdb_glossary.json', (union_by_name = true));

CREATE OR REPLACE TABLE glossary.kineticist AS
SELECT *
FROM read_json_auto(getvariable('ingest_base') || '/glossary_kineticist/kineticist_glossary.json');

CREATE OR REPLACE TABLE glossary.pinball_primer AS
SELECT *
FROM read_json_auto(getvariable('ingest_base') || '/glossary_pinball_primer/pinball_primer_glossary.json');
