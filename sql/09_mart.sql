-- The published marts: each external source, in our vocabulary.
--
-- THIS IS THE CONTRACT. Flippatch reads these relations and nothing beneath
-- them, so changing a column here is a cross-repo change and changing one in
-- staging is not.
--
-- The job is translation: our spelling, NULL where the source says "nothing" in
-- its own dialect, and a fact on the row where a consumer would otherwise have
-- to know to go and join for it. Correcting the dump against its own source is
-- staging's job -- `ipdb_stg.models` is where that line falls.

------------------------------------------------------------
-- OPDB
------------------------------------------------------------

-- OPDB's export with its two coded fields resolved to catalog slugs.
--
-- Published straight from raw: this is the source's only transform. Column names
-- are OPDB's own and already snake_case.
CREATE OR REPLACE VIEW opdb.machines AS
SELECT
  om.*,
  (om.manufacturer ->> 'name') AS manufacturer_name,
  tg.slug AS technology_generation_slug,
  dt.slug AS display_type_slug
FROM opdb_raw.machines AS om
LEFT JOIN opdb_ref.technology_generation AS tg ON om."type" = tg.opdb_type
LEFT JOIN opdb_ref.display_type AS dt ON om.display = dt.opdb_display;

CREATE OR REPLACE VIEW opdb.manufacturers AS
SELECT DISTINCT
  om.manufacturer.manufacturer_id AS opdb_manufacturer_id,
  (om.manufacturer ->> 'name') AS "name",
  (om.manufacturer ->> 'full_name') AS full_name
FROM opdb_raw.machines AS om
WHERE om.manufacturer IS NOT NULL
ORDER BY "name";

CREATE OR REPLACE VIEW opdb.keywords AS
SELECT opdb_id, "name", unnest(keywords) AS keyword
FROM opdb_raw.machines
WHERE len(keywords) > 0;

-- OPDB's image array flattened, one row per image: the struct is awkward to
-- reach into from a join.
CREATE OR REPLACE VIEW opdb.machine_images AS
SELECT
  om.opdb_id,
  om.name AS machine_name,
  img.title AS image_title,
  img."primary" AS is_primary,
  img."type" AS image_type,
  img.urls.small AS url_small,
  img.urls.medium AS url_medium,
  img.urls."large" AS url_large,
  img.sizes.small.width AS small_width,
  img.sizes.small.height AS small_height,
  img.sizes.medium.width AS medium_width,
  img.sizes.medium.height AS medium_height,
  img.sizes."large".width AS large_width,
  img.sizes."large".height AS large_height
FROM opdb.machines AS om, unnest(om.images) AS t(img)
WHERE len(om.images) > 0;

------------------------------------------------------------
-- IPDB
------------------------------------------------------------

-- One row per IPDB model, as IPDB states it, in our vocabulary.
--
-- TWO SOURCES. The bulk is the xantari export; a handful of fields come from
-- IPDB's own pages via `ipdb_stg.archive_models`. Xantari always wins where it
-- speaks -- its newest snapshot post-dates every capture -- so the pages fill
-- gaps and never override. `rating_src` and `production_src` name the source of
-- a filled value; every other archive-supplied column exists only because
-- xantari has no field for it.
--
-- Read `additional_details_date_kind` before using the dates. The pages supply
-- no date VALUE, only what KIND the header-line date is, and `project_inferred`
-- marks where even that is inference rather than a page's own statement.
--
-- The star keeps a field the dump gains upstream instead of dropping it; the
-- RENAME spells the known ones our way. An unrecognised field arrives in
-- PascalCase and trips `ipdb_models_column_not_snake_case`, so naming it is a
-- one-line addition below.
--
-- `RENAME` is silent about a column that is ABSENT, where `EXCLUDE` and
-- `REPLACE` raise. So a field the dump stops carrying vanishes from here without
-- a sound, unless staging happens to read it by its original name.
--
-- The manufacturer fields are excluded rather than renamed because a column
-- cannot appear in both `REPLACE` and `RENAME`, and they need both.
CREATE OR REPLACE VIEW ipdb.models AS
SELECT
  s.* EXCLUDE (Manufacturer, ManufacturerId)
      RENAME (
        IpdbId              AS ipdb_id,
        Title               AS name,
        Players             AS players,
        AdditionalDetails   AS additional_details,
        "Type"              AS type_text,
        TypeShortName       AS type_code,
        DateOfManufacture   AS date_of_manufacture,
        Theme               AS theme_text,
        production_status   AS production_status_name,
        NotableFeatures     AS notable_features,
        DesignBy            AS design_by,
        ArtBy               AS art_by,
        DotsAnimationBy     AS dots_animation_by,
        MechanicsBy         AS mechanics_by,
        MusicBy             AS music_by,
        SoundBy             AS sound_by,
        SoftwareBy          AS software_by,
        Notes               AS notes,
        PhotosIn            AS photos_in,
        "Source"            AS source_note,
        ImageFiles          AS image_files,
        AverageFunRating    AS average_fun_rating,
        ModelNumber         AS model_number,
        ProductionNumber    AS production_number,
        Documentation       AS documentation,
        "Files"             AS files,
        CommonAbbreviations AS common_abbreviations,
        MPU                 AS mpu,
        Toys                AS toys,
        MarketingSlogans    AS marketing_slogans,
        RuleSheetUrls       AS rule_sheet_urls,
        ROMs                AS roms,
        ServiceBulletins    AS service_bulletins,
        MultimediaFiles     AS multimedia_files
      ),

  -- `ipdb_ref.corporate_entity_not_a_company`'s sentinel ids, written our one
  -- way. Translating here rather than leaving each consumer to filter is what
  -- makes the failure mode safe: a forgotten `NOT IN (…)` compares against a real
  -- corporate entity and returns a WRONG match, where a forgotten `IS NOT NULL`
  -- returns NO match. The id list stays private in `ipdb_ref`, since publishing
  -- it would make "every consumer must remember to filter" part of the contract.
  --
  -- `ipdb_` prefixes the id because a bare `corporate_entity_id` reads as a
  -- catalog key, and joining it to one returns rows rather than failing. The text
  -- beside it needs no prefix; nothing joins on it.
  CASE WHEN nac.ipdb_manufacturer_id IS NULL THEN s.ManufacturerId END AS ipdb_corporate_entity_id,
  CASE WHEN nac.ipdb_manufacturer_id IS NULL THEN s.Manufacturer   END AS corporate_entity_text,

  -- Which listing this one duplicates, on the row rather than behind a join.
  -- Every consumer of the duplicate list wants this one fact, it is 1:1 with the
  -- model, and a column cannot be forgotten the way a join can. Why a pairing is
  -- believed -- the reasoning and the two URLs it rests on -- stays in
  -- `ipdb_ref.duplicate_listings`, which is where a reader goes to judge it.
  dup.duplicate_of_ipdb_id
FROM ipdb_stg.models AS s
LEFT JOIN ipdb_ref.corporate_entity_not_a_company AS nac
  ON nac.ipdb_manufacturer_id = s.ManufacturerId
LEFT JOIN ipdb_ref.duplicate_listings AS dup
  ON dup.ipdb_id = s.IpdbId;

-- IPDB's credits, one row per credited person per model.
--
-- `role` is IPDB's own wording for the field the credit came from; `role_slug`
-- is that wording in catalog vocabulary. Both are kept: the slug is what joins,
-- the original is what a human checks the mapping against.
CREATE OR REPLACE VIEW ipdb.credits AS
SELECT
  IpdbId AS ipdb_id,
  role,
  role_slug,
  person_name
FROM ipdb_stg.credits;

-- Published whole so unused rules and unresolved catalog vocabulary remain
-- visible even when no cached page exercises them.
CREATE OR REPLACE VIEW ipdb.specialties AS
SELECT
  ipdb_specialty AS specialty,
  target_entity_type,
  target_public_id,
  target_is_public_id
FROM ipdb_ref.specialty;

-- Keeps IPDB's wording beside its decode, plus the capture provenance because
-- classification may have changed since the archived page.
-- `ipdb_specialty_unmapped` prevents the INNER join from silently dropping a
-- page value.
CREATE OR REPLACE VIEW ipdb.model_specialties AS
SELECT
  ams.ipdb_id,
  ams.specialty,
  sp.target_entity_type,
  sp.target_public_id,
  sp.target_is_public_id,
  ams.archive_source_url,
  ams.archive_capture_date
FROM ipdb_stg.archive_model_specialties AS ams
INNER JOIN ipdb_ref.specialty AS sp
  ON sp.ipdb_specialty = ams.specialty;

-- IPDB's corporate entities, in our vocabulary.
--
-- The translation is the entity itself. IPDB calls this a "manufacturer" and
-- keys it `ManufacturerId`, but it issues one id per corporate INCARNATION --
-- Bally is 47, 48 and 214 -- which is a corporate entity in our terms, with the
-- manufacturer being the brand it rolls up to. Consumers were doing this rename
-- themselves; doing it here means the mart and `ipdb.models` agree on what
-- `corporate_entity_id` means.
CREATE OR REPLACE VIEW ipdb.corporate_entities AS
SELECT
  ipdb_manufacturer_id AS ipdb_corporate_entity_id,
  raw_name             AS corporate_entity_text,
  company_name         AS corporate_entity_name,
  trade_name,
  manufacturer_name,
  year_start,
  year_end,
  single_year,
  location             AS location_text,
  year_of_first_model,
  year_of_last_model
FROM ipdb_stg.corporate_entities;

-- Listings IPDB deleted, which `ipdb_stg.models_merged` drops.
--
-- Republished because it is the one fact about the dump that no column can
-- carry: the row it describes is absent from `ipdb.models` by construction. A
-- consumer holding a dead IPDB id needs to know whether the absence is a
-- confirmed deletion or a crawl that missed a page, and only this says which.
CREATE OR REPLACE VIEW ipdb.retracted_listings AS
SELECT ipdb_id, first_absent_on, reason, evidence_url
FROM ipdb_ref.retracted;
