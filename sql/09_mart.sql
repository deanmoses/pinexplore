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
--
-- WHAT MAY BE NAMED HERE. A mart publishes Flipcommons fields, plus columns that
-- exist to explain how one was derived or why it is empty. Every column of the
-- second kind must name, in its comment, the Flipcommons field it serves --
-- `variant_parent_id`, `variant_parent_is_model`, `variant_parent_relation` and
-- `sibling_set_primary_id` all exist to explain `variant_of`.
--
-- That is a justification requirement and not a spelling one. A rule demanding
-- the served field as a name PREFIX would reject `sibling_set_primary_id`, whose
-- whole point is that it is keyed to the set rather than to the edge; and it
-- would admit an OPDB-shaped column that happened to start with the right word.
-- What must never appear is a column named in the SOURCE's vocabulary for a
-- concept the catalog has -- that is the leak the rule is for.

------------------------------------------------------------
-- OPDB
------------------------------------------------------------

-- OPDB's machines as Flipcommons MODELS.
--
-- One row per thing Flipcommons would call a Model: a distinct buyable machine.
-- That is OPDB's real machines plus its aliases, because an alias is a full
-- Model in Flipcommons -- same fields as any other, distinguished only by
-- pointing at the model it dresses differently.
--
-- OPDB'S CONTAINERS ARE NOT HERE. A row OPDB marks non-physical is a virtual
-- holder for a set of gameplay-identical machines, which matters enormously to
-- OPDB -- they serve tournaments, where gameplay equivalence is the whole
-- question -- and corresponds to nothing in Flipcommons. Published here they
-- would read as several dozen models the catalog is missing. `opdb_stg.machines`
-- holds them for anyone who wants the export entire.
--
-- WHAT OPDB SAYS ABOUT A VARIANT, AND WHAT IT DOES NOT. OPDB states that two
-- machines are related and which is the parent; it never states what the
-- relation IS. `variant_parent_relation` is the verdict on that, and only one of
-- its values fills `variant_of`:
--
--   variant             Same manufacturer -- an edition of a company's own machine.
--   cross_manufacturer  The two disagree on manufacturer. `variant_of` IS NULL on
--                       every one of these, deliberately. Not typed further,
--                       because the catalog holds these as a `copy` edge on fewer
--                       than half the rows and as a retheme, an export edition, a
--                       remake or nothing at all on the rest. Worth opening: a
--                       shared Title asserts no gameplay identity, so catalog
--                       silence is the catalog declining to say rather than
--                       saying no.
--   conversion          A `ModelRelationship` edge; see `opdb.model_relationships`.
--   NULL                A manufacturer is missing, so no verdict was reached.
--
-- TWO KINDS OF CLAIM, IN TWO COLUMNS, AND THE DIFFERENCE IS THE POINT.
--
-- `variant_of` is a DECODE: OPDB named a real machine and the manufacturers
-- matched. Nothing here was elected or inferred.
--
-- `sibling_set_primary_id` is a PROPOSAL. OPDB hangs most aliases off one of its
-- non-physical containers, which is not a machine and corresponds to nothing in
-- Flipcommons, so Pinexplore elects the broadest member of the set and names it
-- here -- as a proposal to resolve against the live records, never as
-- `variant_of`. `= opdb_id` means this model IS the elected primary, a different
-- id is the proposed parent, and NULL means no election.
--
-- Read NULL together with BOTH `variant_parent_is_model` and
-- `variant_parent_relation`: on its own it covers three things -- an alias of a
-- real machine, which never had a set to be elected within; a container nobody
-- could call, which is the worklist; and a conversion inside a container, which
-- was dropped before ranking and so never competed. `opdb_stg.alias_lineage`
-- tabulates them.
--
-- `variant_parent_id` is OPDB's own answer, always set on an alias: the id it
-- hangs off, container or machine. NOT A FOREIGN KEY -- it resolves here only
-- when `variant_parent_is_model`. Read it as the sibling-set key, which is what
-- it always is, and it stays usable where no verdict and no election survive.
--
-- NOTHING HERE IS CERTAIN. Flipcommons `variant_of` needs gameplay-identical AND
-- cosmetic-only; OPDB's alias tree means gameplay-identical alone. Even a
-- same-manufacturer verdict is OPDB's claim decoded, not a fact -- a retheme
-- OPDB does not tag reads as a variant and nothing here can tell.
--
-- `is_remake` is a flag rather than a `remake_of` pointer because OPDB says a
-- model is a remake and never says what of. `opdb_stg.alias_lineage` has the
-- reasoning behind all of it and the Godzilla and Cactus Canyon worked cases.
CREATE OR REPLACE VIEW opdb.models AS
SELECT
  m.* EXCLUDE (
        physical_machine, group_id, machine_id, alias_id, is_model,
        features, keywords, images, manufacture_date, is_alias, is_machine,
        "type", display, shortname
      )
      RENAME (
        technology_generation_slug AS technology_generation,
        display_type_slug          AS display_type
      ),
  m.group_id AS title_opdb_id,
  l.variant_parent_id,
  l.variant_parent_is_model,
  l.variant_parent_relation,
  l.variant_of,
  l.sibling_set_primary_id,
  l.export_edition_of,
  cab.target_value AS cabinet
FROM opdb_stg.machines AS m
LEFT JOIN opdb_stg.alias_lineage AS l ON l.opdb_id = m.opdb_id
-- Scalar Flipcommons FKs sourced from a coded value become columns, not rows.
-- `cabinet` is the only one OPDB can speak to: it says `Cocktail table` and
-- nothing else, so every other model is NULL -- undefined, not `floor`.
LEFT JOIN opdb_stg.model_features AS cab
  ON cab.opdb_id = m.opdb_id AND cab.target_entity_type = 'cabinet'
WHERE m.is_model;
COMMENT ON VIEW opdb.models IS
  'OPDB machines and aliases as Flipcommons Models, with the non-physical containers excluded.';

-- OPDB's groups as Flipcommons TITLES.
--
-- A `Title` is the canonical identity of a game design across editions and
-- manufacturers -- one Medieval Madness spanning the 1997 Williams original and
-- every Chicago Gaming remake. An OPDB group is the same idea, and Flipcommons
-- already stores the link: `Title.opdb_id` holds the group id from `opdb_id`
-- here, despite the field name not saying "group".
--
-- The two do NOT always agree on membership, and Pinexplore cannot fix that.
-- OPDB files Cactus Canyon's remakes inside the original's group and Metallica
-- Remastered in a group of its own; which is right is a Flipcommons editorial
-- call, so the disagreement is flippatch's to surface.
--
-- Titles and variants are the reason to hold OPDB at all. Nothing else we ingest
-- groups machines this way -- IPDB has no concept of a title -- so a Flipcommons
-- title that disagrees with this view has no third opinion to appeal to.
--
-- The four URL columns are OPDB's links out to rule and card sources, which is
-- where a fetch campaign for one of these machines starts.
CREATE OR REPLACE VIEW opdb.titles AS
SELECT
  g.* EXCLUDE (shortname),
  (SELECT count(*) FROM opdb.models AS m WHERE m.title_opdb_id = g.opdb_id) AS n_models
FROM opdb_stg.machine_groups AS g;
COMMENT ON VIEW opdb.titles IS
  'OPDB machine groups as Flipcommons Titles.';

-- Any OPDB id we might be handed, and where it is now.
--
-- Replaces reading the changelog directly, which states only the retirements and
-- leaves a caller to walk a chain of them. An id missing from the export is
-- otherwise indistinguishable from an id nobody has seen; here OPDB says which,
-- and `current_opdb_id` is the END of the chain rather than one hop along it.
--
-- THE UNIVERSE IS EVERY ID OPDB HAS EVER ISSUED, which is the export plus the
-- changelog, and the export means `opdb_stg.machines` rather than `opdb.models`.
-- The containers are deliberately included: a container id is a real id OPDB has
-- issued and can hand back, and answering "never heard of it" for one is the
-- exact confusion this view exists to remove. `is_model` says whether the id
-- names something `opdb.models` publishes.
--
-- `UNION` and not `UNION ALL` builds it, so an id in both the export and the
-- changelog appears once. That is not tidiness -- it is the grain, and the
-- overlap is NORMAL: the two artifacts are downloaded separately and the
-- changelog runs ahead of the export, which is what
-- `opdb_changelog_retired_id_still_in_export` exists to say.
--
-- The changelog is the authority on status, and the export only on presence:
--
--   current  Live in the export, with no retirement recorded.
--   moved    Retired, and `current_opdb_id` is where it went.
--   deleted  Retired with no replacement; `current_opdb_id` is NULL.
--
-- `retired_at` is when OPDB recorded THIS id's retirement, not when the chain it
-- starts ended -- each hop carries its own row and its own date. NULL on
-- `current`. Cast because the changelog's timestamp infers as VARCHAR where the
-- export's infers as DATE; UTC on every row.
--
-- An id the changelog retired that the export still lists reads as retired. The
-- changelog is the record of intent and the export is a snapshot that may
-- predate it.
--
-- `UNION` and not `UNION ALL` in the recursion too, so a cycle in OPDB's moves
-- converges instead of spinning until the layer times out.
CREATE OR REPLACE VIEW opdb.model_ids AS
WITH RECURSIVE chain(opdb_id, id) AS (
  SELECT opdbIdDeleted, opdbIdReplacement FROM opdb_raw.changelog WHERE action = 'move'
  UNION
  SELECT ch.opdb_id, c.opdbIdReplacement
  FROM chain AS ch
  JOIN opdb_raw.changelog AS c ON c.opdbIdDeleted = ch.id AND c.action = 'move'
),
-- The end of each chain: a replacement nothing has since retired. Scoped to
-- `action = 'move'` on both sides -- an id that was moved and later DELETED has
-- terminated, and reading any retirement as "keep walking" would leave the chain
-- unresolved and the id looking like a broken `moved` rather than a `deleted`.
terminus AS (
  SELECT opdb_id, id AS current_opdb_id FROM chain
  WHERE NOT EXISTS (
    SELECT 1 FROM opdb_raw.changelog AS c
    WHERE c.opdbIdDeleted = chain.id AND c.action = 'move'
  )
),
universe AS (
  SELECT opdb_id FROM opdb_stg.machines
  UNION
  SELECT opdbIdDeleted FROM opdb_raw.changelog
)
SELECT
  u.opdb_id,
  CASE WHEN c.opdbIdDeleted IS NULL THEN u.opdb_id ELSE t.current_opdb_id END AS current_opdb_id,
  CASE WHEN c.opdbIdDeleted IS NULL THEN 'current'
       WHEN c.action = 'move'      THEN 'moved'
       ELSE 'deleted' END AS status,
  CAST(c.createdAt AS TIMESTAMP) AS retired_at,
  EXISTS (SELECT 1 FROM opdb.models AS m WHERE m.opdb_id = u.opdb_id) AS is_model
FROM universe AS u
LEFT JOIN opdb_raw.changelog AS c ON c.opdbIdDeleted = u.opdb_id
LEFT JOIN terminus AS t ON t.opdb_id = u.opdb_id;
COMMENT ON VIEW opdb.model_ids IS
  'Every id OPDB has ever issued and where it is now: current, moved or deleted.';

-- OPDB's manufacturers, one row per id -- as long as OPDB spells each one the
-- same way on every machine. It has no manufacturer list to read: the names ride
-- on the machines, so this is a DISTINCT over them and a second spelling of one
-- id publishes that id twice. `opdb_manufacturer_ambiguous` watches for it, the
-- counterpart of `ipdb_corporate_entity_ambiguous`, and warns rather than
-- aborting for the same reason: the cause is upstream.
--
-- No ORDER BY. DuckDB does not hold a view's ordering through a join or an
-- aggregate, so it is a sort paid for on every read in exchange for a guarantee
-- the first consumer to wrap this view loses. Order at the point of reading.
CREATE OR REPLACE VIEW opdb.manufacturers AS
SELECT DISTINCT
  om.opdb_manufacturer_id,
  om.manufacturer_name AS "name",
  om.manufacturer_full_name AS full_name
FROM opdb.models AS om
WHERE om.opdb_manufacturer_id IS NOT NULL;
COMMENT ON VIEW opdb.manufacturers IS
  'Distinct manufacturer id and name combinations observed on OPDB models; an id may repeat when upstream names conflict.';

------------------------------------------------------------
-- OPDB models, one view per many-to-many Flipcommons entity
--
-- Each is fed by whichever OPDB vocabularies speak to it -- `widebody` arrives
-- as both a feature and a keyword -- so a consumer joins one view per catalog
-- entity and never learns that OPDB had two lists.
--
-- THESE VIEWS DO NOT SAY WHETHER THE CATALOG HAS THE VALUE. Pinexplore reads no
-- catalog, so a column claiming to know would be a guess that rots the moment
-- the catalog changes and nothing here would notice. Resolving these values, and
-- deciding which ought not to resolve, is flippatch's, beside the live records.
--
-- DO NOT ASSUME A VALUE IS A SLUG. Themes and gameplay features carry OPDB's own
-- wording, which is a display phrase where OPDB writes one -- `Head-to-head
-- play` -- because the catalog's aliases for those vocabularies are display
-- phrases too. Tags, reward types, cabinets and series are translated to catalog
-- slugs here. See `opdb_ref.keyword`.
------------------------------------------------------------

-- OPDB's `shortname` as Flipcommons `ModelAbbreviation` / `TitleAbbreviation`.
--
-- The field is named for a short NAME and holds an abbreviation: `MM`, `AFM`,
-- `WOZ`, `B66`. Flipcommons has an entity for exactly that, at both grains, so
-- this is a view per grain rather than a column on `opdb.models` -- a model may
-- carry several abbreviations in the catalog even though OPDB states at most
-- one, and publishing it as a scalar would bake OPDB's limit into the shape.
--
-- `shortname` is gone from `opdb.models` and `opdb.titles` for the same reason:
-- the mart names Flipcommons entities, and a column spelled OPDB's way invites a
-- comparison against a catalog field that does not exist.
CREATE OR REPLACE VIEW opdb.model_abbreviations AS
SELECT opdb_id, "name" AS model_name, shortname AS abbreviation
FROM opdb_stg.machines
WHERE is_model AND shortname IS NOT NULL;
COMMENT ON VIEW opdb.model_abbreviations IS
  'OPDB shortnames as Flipcommons ModelAbbreviations.';

CREATE OR REPLACE VIEW opdb.title_abbreviations AS
SELECT opdb_id, "name" AS title_name, shortname AS abbreviation
FROM opdb_stg.machine_groups
WHERE shortname IS NOT NULL;
COMMENT ON VIEW opdb.title_abbreviations IS
  'OPDB shortnames as Flipcommons TitleAbbreviations.';

CREATE OR REPLACE VIEW opdb.model_tags AS
SELECT DISTINCT opdb_id, "name" AS model_name,
  target_value AS tag
FROM (
  SELECT * FROM opdb_stg.model_features WHERE target_entity_type = 'tag'
  UNION ALL BY NAME
  SELECT * FROM opdb_stg.model_keywords WHERE target_entity_type = 'tag'
);
COMMENT ON VIEW opdb.model_tags IS
  'One row per model and OPDB value bucketed for later matching to Flipcommons Tags, fed by features and keywords.';

CREATE OR REPLACE VIEW opdb.model_themes AS
SELECT DISTINCT opdb_id, "name" AS model_name,
  target_value AS theme
FROM opdb_stg.model_keywords WHERE target_entity_type = 'theme';
COMMENT ON VIEW opdb.model_themes IS
  'One row per model and OPDB keyword bucketed for later matching to Flipcommons Themes.';

CREATE OR REPLACE VIEW opdb.model_gameplay_features AS
SELECT DISTINCT opdb_id, "name" AS model_name,
  target_value AS gameplay_feature
FROM (
  SELECT * FROM opdb_stg.model_features WHERE target_entity_type = 'gameplay-feature'
  UNION ALL BY NAME
  SELECT * FROM opdb_stg.model_keywords WHERE target_entity_type = 'gameplay-feature'
);
COMMENT ON VIEW opdb.model_gameplay_features IS
  'One row per model and OPDB value bucketed for later matching to Flipcommons GameplayFeatures, fed by features and keywords.';

-- One row per Flipcommons `Series` OPDB implies, at OPDB's grain.
--
-- Flipcommons hangs a Series off the TITLE -- one Black Knight series spanning
-- its titles -- while OPDB tags the machines. Rolling these up would mean
-- deciding that every title with a tagged model belongs to the series, which is
-- a catalog judgement made better beside the catalog. Published as stated.
CREATE OR REPLACE VIEW opdb.model_series AS
SELECT DISTINCT opdb_id, "name" AS model_name,
  target_value AS series
FROM opdb_stg.model_keywords WHERE target_entity_type = 'series';
COMMENT ON VIEW opdb.model_series IS
  'One row per model per Flipcommons Series OPDB implies, left at OPDB grain because Flipcommons hangs a Series off the Title.';

CREATE OR REPLACE VIEW opdb.model_reward_types AS
SELECT DISTINCT opdb_id, "name" AS model_name,
  target_value AS reward_type
FROM opdb_stg.model_features WHERE target_entity_type = 'reward-type';
COMMENT ON VIEW opdb.model_reward_types IS
  'One row per model and OPDB feature bucketed for later matching to Flipcommons RewardTypes.';

-- One row per Flipcommons `ModelRelationship` edge OPDB implies.
--
-- The TYPE is stated and the DONOR usually is not: OPDB names one only where it
-- files the conversion as an alias of it. A null donor is not an incomplete row
-- -- Flipcommons expresses an unseeded donor as a `target_label` -- so these are
-- publishable as they stand, and the few with a donor are simply better.
--
-- OPDB's one `Conversion kit` tag does not separate Flipcommons' `conversion`
-- from its `conversion_kit`; it files Challenger V, a complete machine, under
-- the kit wording. Treat `relationship_type` as OPDB's claim, not a verdict.
CREATE OR REPLACE VIEW opdb.model_relationships AS
SELECT
  mf.opdb_id,
  mf."name" AS model_name,
  mf.target_value AS relationship_type,
  l.conversion_donor_id AS target_opdb_id
FROM opdb_stg.model_features AS mf
LEFT JOIN opdb_stg.alias_lineage AS l ON l.opdb_id = mf.opdb_id
WHERE mf.target_entity_type = 'model-relationship';
COMMENT ON VIEW opdb.model_relationships IS
  'One row per Flipcommons ModelRelationship edge OPDB implies; the type is stated and the donor usually is not.';

-- One row per Flipcommons `ModelExportMarket`. OPDB says a model was built for
-- export and never says for WHERE, so the market is always unknown -- which the
-- domain model allows as a row recording the export fact alone. Where OPDB also
-- files the model as an alias of its domestic original, `opdb.models` carries
-- `export_edition_of` as well, and the two agree.
CREATE OR REPLACE VIEW opdb.model_export_markets AS
SELECT m.opdb_id, m."name" AS model_name, CAST(NULL AS VARCHAR) AS target_market
FROM opdb_stg.machines AS m
WHERE m.is_model
  AND EXISTS (SELECT 1 FROM opdb_ref.feature AS f
              WHERE f.target_value = 'export_edition_of'
                AND list_contains(m.features, f.opdb_feature));
COMMENT ON VIEW opdb.model_export_markets IS
  'One row per Flipcommons ModelExportMarket; OPDB states that a model was built for export and never for where.';

-- OPDB's image array flattened, one row per image.
--
-- `is_primary` is primary WITHIN ITS `image_type`, not for the machine -- a
-- machine with a backglass and a playfield has two. Picking one image means
-- naming the type as well.
--
-- `image_asset_id` is OPDB's `group`, renamed because it groups nothing: it is
-- unique per image, and is the uuid the size URLs are built from.
CREATE OR REPLACE VIEW opdb.model_images AS
SELECT
  om.opdb_id,
  om."name" AS model_name,
  img."group" AS image_asset_id,
  nullif(img.title, '') AS image_title,
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
FROM opdb_stg.machines AS om, unnest(om.images) AS t(img)
WHERE om.is_model;
COMMENT ON VIEW opdb.model_images IS
  'One row per OPDB image per model; is_primary is scoped to image_type, not the model.';

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
-- one-line addition below. Do not replace the star with a column list: a new
-- field would then vanish here instead of stopping the build.
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
COMMENT ON VIEW ipdb.models IS
  'Newest available xantari observation per IPDB model, supplemented with selected archived-page fields; carried_forward marks older observations.';

-- IPDB's credits, one row per credited person per model.
--
-- `role` is IPDB's own wording for the field the credit came from; `role_slug`
-- is that wording in catalog vocabulary. Both are kept: the slug is what joins,
-- the original is what a human checks the mapping against.
CREATE OR REPLACE VIEW ipdb.model_credits AS
SELECT
  IpdbId AS ipdb_id,
  role,
  role_slug,
  person_name
FROM ipdb_stg.credits;
COMMENT ON VIEW ipdb.model_credits IS
  'One row per IPDB model, credited person and role, with the role in both IPDB and catalog vocabulary.';

-- Published whole so unused rules remain visible even when no cached page
-- exercises them.
CREATE OR REPLACE VIEW ipdb.specialties AS
SELECT
  ipdb_specialty AS specialty,
  target_entity_type,
  target_value
FROM ipdb_ref.specialty;
COMMENT ON VIEW ipdb.specialties IS
  'IPDB''s full Specialty vocabulary with the Flipcommons target type and value each rule proposes, including values unused by cached pages.';

-- Keeps IPDB's wording beside its decode, plus the capture provenance because
-- classification may have changed since the archived page.
-- `ipdb_specialty_unmapped` prevents the INNER join from silently dropping a
-- page value.
CREATE OR REPLACE VIEW ipdb.model_specialties AS
SELECT
  ams.ipdb_id,
  ams.specialty,
  sp.target_entity_type,
  sp.target_value,
  ams.archive_source_url,
  ams.archive_capture_date
FROM ipdb_stg.archive_model_specialties AS ams
INNER JOIN ipdb_ref.specialty AS sp
  ON sp.ipdb_specialty = ams.specialty;
COMMENT ON VIEW ipdb.model_specialties IS
  'One row per model per IPDB specialty, keeping IPDB''s wording beside its decode and the archived page it was read from.';

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
COMMENT ON VIEW ipdb.corporate_entities IS
  'IPDB manufacturer records parsed as corporate incarnations rather than brands, with company, trade name, dates and location separated.';

-- Listings IPDB deleted, which `ipdb_stg.models_merged` drops.
--
-- Republished because it is the one fact about the dump that no column can
-- carry: the row it describes is absent from `ipdb.models` by construction. A
-- consumer holding a dead IPDB id needs to know whether the absence is a
-- confirmed deletion or a crawl that missed a page, and only this says which.
CREATE OR REPLACE VIEW ipdb.retracted_listings AS
SELECT ipdb_id, first_absent_on, reason, evidence_url
FROM ipdb_ref.retracted;
COMMENT ON VIEW ipdb.retracted_listings IS
  'Listings IPDB has deleted -- the only place an id absent from ipdb.models is distinguishable from one never crawled.';
