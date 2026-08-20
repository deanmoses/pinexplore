-- Raw tables generated from all JSON source data files.
-- No transforms, no joins. Just flatten top-level wrappers where needed.
-- Tables (not views) so JSON is parsed once at build time.
--
-- Requires: SET VARIABLE ingest_base = '<path-or-url>';
-- Local default is set by rebuild_explore.sh.
-- For remote access (e.g. MotherDuck), set to R2 URL before loading:
--   SET VARIABLE ingest_base = 'https://pub-8a5220445534421c879b6ff9ede350f1.r2.dev';

------------------------------------------------------------
-- Pindata catalog data (via pindata export)
------------------------------------------------------------

CREATE OR REPLACE TABLE cabinets AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/cabinet.json');

CREATE OR REPLACE TABLE corporate_entities AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/corporate_entity.json');

CREATE OR REPLACE TABLE credit_roles AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/credit_role.json');

CREATE OR REPLACE TABLE display_subtypes AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/display_subtype.json');

CREATE OR REPLACE TABLE display_types AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/display_type.json');

CREATE OR REPLACE TABLE franchises AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/franchise.json');

CREATE OR REPLACE TABLE game_formats AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/game_format.json');

CREATE OR REPLACE TABLE gameplay_features AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/gameplay_feature.json');

CREATE OR REPLACE TABLE locations AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/location.json');

CREATE OR REPLACE TABLE reward_types AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/reward_type.json');

CREATE OR REPLACE TABLE manufacturers AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/manufacturer.json');

CREATE OR REPLACE TABLE models AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/model.json', (union_by_name = CAST('t' AS BOOLEAN)));

CREATE OR REPLACE TABLE people AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/person.json', (union_by_name = CAST('t' AS BOOLEAN)));

CREATE OR REPLACE TABLE series AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/series.json');

CREATE OR REPLACE TABLE systems AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/system.json');

CREATE OR REPLACE TABLE tags AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/tag.json');

CREATE OR REPLACE TABLE technology_generations AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/technology_generation.json');

CREATE OR REPLACE TABLE technology_subgenerations AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/technology_subgeneration.json');

-- Explicit columns: DuckDB infers aliases as JSON[] instead of VARCHAR[]
-- because of an apostrophe in one alias value ("Ufo's").
CREATE OR REPLACE TABLE themes AS
SELECT * FROM read_json_auto(
  getvariable('ingest_base') || '/pindata/theme.json',
  columns = {slug: 'VARCHAR', name: 'VARCHAR', aliases: 'VARCHAR[]', parents: 'VARCHAR[]'}
);

CREATE OR REPLACE TABLE titles AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/pindata/title.json', (union_by_name = CAST('t' AS BOOLEAN)));

------------------------------------------------------------
-- External source dumps
------------------------------------------------------------

-- Fandom wiki exports
CREATE OR REPLACE TABLE fandom_games AS
SELECT d.*
FROM (SELECT unnest(games) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom_games.json'));

CREATE OR REPLACE TABLE fandom_manufacturers AS
SELECT d.*
FROM (SELECT unnest(manufacturers) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom_manufacturers.json'));

CREATE OR REPLACE TABLE fandom_persons AS
SELECT d.*
FROM (SELECT unnest(persons) AS d FROM read_json_auto(getvariable('ingest_base') || '/fandom_persons.json'));

-- Pinball Map API exports
CREATE OR REPLACE TABLE pinballmap_machines AS
SELECT d.*
FROM (SELECT unnest(machines) AS d FROM read_json_auto(getvariable('ingest_base') || '/pinballmap_machines.json'));

CREATE OR REPLACE TABLE pinballmap_machine_groups AS
SELECT d.*
FROM (SELECT unnest(machine_groups) AS d FROM read_json_auto(getvariable('ingest_base') || '/pinballmap_machine_groups.json'));

-- OPDB (Open Pinball Database) exports
CREATE OR REPLACE TABLE opdb_groups AS
SELECT * FROM read_json_auto(getvariable('ingest_base') || '/opdb_export_groups.json');

CREATE OR REPLACE TABLE opdb_machines AS
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
-- The scrape ships as periodic full snapshots, kept side by side rather than
-- overwritten: each one is a dated observation of IPDB, and holding two lets
-- the build see what moved between them.
--
-- Snapshots are listed explicitly rather than globbed. A glob cannot be
-- expanded over HTTP, which --remote needs (ingest_base is an R2 URL there),
-- and an explicit list also stops a stray file from silently joining the union.
-- Adding a snapshot is a deliberate edit here, and both files must be in R2
-- (`make push`) or a fresh checkout won't reproduce the build.
--
-- `union_by_name` matters because these records are sparse: 36 distinct keys
-- across the file, only 5 of them on every record, so two snapshots need not
-- agree on the column set and a future one may add or drop keys.
CREATE OR REPLACE TABLE ipdb_machines_snapshots AS
SELECT
  -- The snapshot's own timestamp, from the file's header rather than its name,
  -- so the date is the scrape's claim about itself.
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

-- One row per machine: the newest snapshot that observed it wins, whole.
--
-- The merge is record-level, never field-level. A snapshot is a single atomic
-- observation, so a row that mixed fields across snapshots would describe a
-- record that never existed upstream and could not be cited as evidence. Where
-- both snapshots have a record, the newer one replaces it entirely.
--
-- An older snapshot therefore contributes exactly one thing: whole records the
-- newest scrape missed. Those are flagged rather than blended in, because a
-- carried-forward row is a stale observation -- it predates the encoding fix
-- that landed between the 2025-02 and 2026-04 snapshots, so it can still hold
-- the U+FFFD mojibake the fresh rows no longer have, and gap analysis should
-- not read it as what IPDB says today.
CREATE OR REPLACE TABLE ipdb_machines AS
SELECT
  s.*,
  s.snapshot_utc < (SELECT max(snapshot_utc) FROM ipdb_machines_snapshots) AS carried_forward
FROM ipdb_machines_snapshots AS s
WHERE NOT EXISTS (SELECT 1 FROM ref_ipdb_retracted AS r WHERE r.ipdb_id = s.IpdbId)
QUALIFY row_number() OVER (PARTITION BY s.IpdbId ORDER BY s.snapshot_utc DESC) = 1;

-- Pinball glossaries
CREATE OR REPLACE TABLE ipdb_glossary AS
SELECT
  slug,
  "name",
  definition,
  see_also,
  aliases,
  games,
FROM read_json_auto(getvariable('ingest_base') || '/glossary_ipdb/ipdb_glossary.json', (union_by_name = true));

CREATE OR REPLACE TABLE kineticist_glossary AS
SELECT *
FROM read_json_auto(getvariable('ingest_base') || '/glossary_kineticist/kineticist_glossary.json');

CREATE OR REPLACE TABLE pinball_primer_glossary AS
SELECT *
FROM read_json_auto(getvariable('ingest_base') || '/glossary_pinball_primer/pinball_primer_glossary.json');
