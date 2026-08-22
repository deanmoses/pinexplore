-- Per-source staging: merging, parsing and correcting each dump.
--
-- No cross-source joins.

------------------------------------------------------------
-- OPDB staged
------------------------------------------------------------

-- Machines and aliases back in one relation, the shape the rest of the pipeline
-- wants and the shape OPDB's older export shipped.
--
-- An alias IS a machine as far as every consumer is concerned -- a specific
-- edition of one, keyed by a third id segment -- and splitting them across two
-- arrays is a fact about OPDB's export format, not about the machines. Rejoining
-- them here means the mart publishes one grain and a caller that wants only the
-- base machines says `WHERE is_machine`.
--
-- `UNION ALL BY NAME` because the two arrays disagree on their column sets: it
-- matches on name and fills the alias-side absences with NULL, which is exactly
-- what the old export carried on those rows. A column list would have to
-- enumerate both sides and would silently drop whichever field OPDB adds next.
--
-- `is_machine` and `is_alias` are forced to real booleans. Upstream states only
-- the true one on each row, so a straight union leaves the other NULL and
-- `WHERE NOT is_alias` -- the obvious way to write the obvious filter -- returns
-- nothing at all.
--
-- The id is split because OPDB encodes the hierarchy INTO it: `G50L9-MDxXD` is
-- machine `MDxXD` of group `G50L9`, and a third segment makes it an alias. Every
-- join to a group or a parent machine needs the parts, and deriving them at each
-- call site is where they get derived inconsistently.
CREATE OR REPLACE TABLE opdb_stg.machines AS
SELECT
  *,
  split_part(opdbId, '-', 1) AS group_id,
  split_part(opdbId, '-', 2) AS machine_id,
  nullif(split_part(opdbId, '-', 3), '') AS alias_id
FROM (
  SELECT m.* REPLACE (true AS isMachine), false AS isAlias
  FROM opdb_raw.machines AS m
  UNION ALL BY NAME
  SELECT a.* REPLACE (true AS isAlias), false AS isMachine
  FROM opdb_raw.aliases AS a
);

------------------------------------------------------------
-- IPDB staged
------------------------------------------------------------

-- One row per model: the newest snapshot that observed it wins, whole.
--
-- Record-level, never field-level. A snapshot is one atomic observation, so a
-- row mixing fields across snapshots would describe a record that never existed
-- upstream and could not be cited as evidence.
--
-- An older snapshot contributes only whole records the newest scrape missed, and
-- those are flagged rather than blended in: a carried-forward row predates the
-- encoding fix that landed between the 2025-02 and 2026-04 snapshots, so it can
-- still hold U+FFFD mojibake the fresh rows no longer have.
CREATE OR REPLACE TABLE ipdb_stg.models_merged AS
SELECT
  s.*,
  s.snapshot_utc < (SELECT max(snapshot_utc) FROM ipdb_raw.xantari_model_snapshots) AS carried_forward
FROM ipdb_raw.xantari_model_snapshots AS s
WHERE NOT EXISTS (SELECT 1 FROM ipdb_ref.retracted AS r WHERE r.ipdb_id = s.IpdbId)
QUALIFY row_number() OVER (PARTITION BY s.IpdbId ORDER BY s.snapshot_utc DESC) = 1;

-- The archive.org pages, flattened and cut down to the models xantari knows.
--
-- Drops every model xantari has never listed: xantari is authoritative on which
-- models EXIST, and holding a 2018 capture of a page is not evidence IPDB still
-- lists it. Restricting against `ipdb_stg.models_merged` rather than the raw
-- snapshots also inherits the retraction filter, so a deleted listing stays
-- deleted. `ipdb_archive_model_not_in_dump` reports what this drops.
--
-- The array columns stay unflattened. The views below unnest them, and reading
-- from here rather than raw is what gives them the same id restriction.
CREATE OR REPLACE VIEW ipdb_stg.archive_models AS
SELECT
  am.ipdb_id,

  -- IPDB's two LABELLED date rows. These are the whole point of this source:
  -- not the values, which xantari's header line already carries, but the fact
  -- that IPDB says which kind each one is. See `additional_details_date_kind`.
  am.manufacture_date.year  AS manufacture_date_year,
  am.project_date.year      AS project_date_year,

  am.production.units          AS production_units,
  am.production.status         AS production_status,
  am.production.never_produced AS production_never_produced,

  am.rating.score       AS rating_score,
  am.rating.ratings     AS rating_ratings,
  am.rating."comments"  AS rating_comments,
  am.rating.provisional AS rating_provisional,

  am.serial_number_database_url,
  am.owners_list_url,
  am.easter_eggs.text AS easter_eggs_text,
  am.easter_eggs.url  AS easter_eggs_url,

  am.themes,
  am.specialties,
  am.documents,
  am.credits,

  am.source_url AS archive_source_url,
  am.archive_capture_date
FROM ipdb_raw.archive_models AS am
WHERE EXISTS (
  SELECT 1 FROM ipdb_stg.models_merged AS mm WHERE mm.IpdbId = am.ipdb_id
);

-- Parse the IPDB page header line, which xantari captured verbatim into
-- `AdditionalDetails` without modelling its parts:
--
--     IPD No. 5755 / 2011 / 4 Players
--
-- The date segment is the reason to parse this. IPDB pages carry either a "Date
-- Of Manufacture" row or a "Project Date" row and the header renders whichever
-- exists; xantari scraped only the former. Where the header is the sole
-- surviving date, it is NOT a manufacture date -- spot-checking found both
-- project dates (Bally bingos, mostly) and dates the dump simply missed -- so
-- confirm against the IPDB page before using one.
--
-- Year/month/day are split rather than emitted as a DATE because the header
-- states three precisions ("March 21, 1961", "August, 1941", "1932"). A DATE
-- would pad the missing parts to 1 and read as a genuine January 1st.
--
-- Built over `ipdb_stg.models_merged`, not `ipdb_stg.models`: the latter reads
-- this view, and sourcing it there would close the cycle.
--
-- The grammar is strict, admitting four shapes (id alone; id + date;
-- id + players; all three). Strictness makes upstream drift visible rather than
-- mis-binding -- a permissive date group swallows "1 Player" as a date. Month
-- names go through try_strptime, so an unfamiliar one lands as a NULL year and
-- trips `ipdb_additional_details_date_unrecognised`.
CREATE OR REPLACE VIEW ipdb_stg.model_additional_details AS
WITH parsed AS (
  SELECT
    im.IpdbId,
    regexp_extract(
      im.AdditionalDetails,
      '^IPD No\. (\d+)(?: / ([A-Za-z]+ \d{1,2}, \d{4}|[A-Za-z]+, \d{4}|\d{4}))?(?: / (\d+) Players?)?$',
      ['ipd_no', 'date_text', 'players']
    ) AS g
  FROM ipdb_stg.models_merged AS im
),
typed AS (
  SELECT
    IpdbId,
    TRY_CAST(nullif(g.ipd_no, '') AS INTEGER)   AS additional_details_ipd_no,
    TRY_CAST(nullif(g.players, '') AS UTINYINT) AS additional_details_players,
    nullif(g.date_text, '')                     AS additional_details_date_string,
    try_strptime(nullif(g.date_text, ''), '%B %d, %Y') AS d_day,
    try_strptime(nullif(g.date_text, ''), '%B, %Y')    AS d_month,
    try_strptime(nullif(g.date_text, ''), '%Y')        AS d_year
  FROM parsed
)
SELECT
  IpdbId,
  -- Redundant with IpdbId and Players on every row today. Kept because that
  -- redundancy is the tripwire `ipdb_additional_details_parse_misaligned` reads:
  -- if the capture groups ever slip,
  -- these disagree and the date is wrong too.
  additional_details_ipd_no,
  additional_details_players,
  additional_details_date_string,
  CAST(year(COALESCE(d_day, d_month, d_year)) AS SMALLINT) AS additional_details_date_year,
  CAST(month(COALESCE(d_day, d_month)) AS UTINYINT)        AS additional_details_date_month,
  CAST(day(d_day) AS UTINYINT)                             AS additional_details_date_day
FROM typed;


-- Add technology generation slug and system/subgeneration via MPU match,
-- plus the parsed header date.
--
-- One row per IPDB model, no exclusions -- `ipdb_stg.models_merged` enriched,
-- with the counts asserted equal by `ipdb_stg_models_drops_rows`. A consumer
-- that cannot use an unknown manufacturer excludes it in its own WHERE.
-- Filtering here is silent: every downstream count inherits it, and gap analysis
-- loses the obscure records likeliest to be missing from the catalog.
CREATE OR REPLACE VIEW ipdb_stg.models AS
SELECT
  -- `ManufacturerShortName` is dropped because is xantari's own derivation, not a
  -- field on the IPDB page: on some records it is the trade name ("Gottlieb"), on
  -- more than half it is the entire `Manufacturer` string, years and trade-name
  -- bracket included. Which one you get tracks whether IPDB recorded a LOCATION.
  -- Counting it distinctly does not rescue it either: it
  -- varies per corporate entity, so one brand across two incarnations counts as two
  -- manufacturers. The brand is `ipdb_stg.corporate_entities.manufacturer_name`.
  im.* EXCLUDE (ManufacturerShortName)
  -- REPLACE nulls both manufacturer fields for the models listed in
  -- `ipdb_ref.model_corporate_entity_misparsed`, so a consumer sees the absence
  -- IPDB's page shows rather than the company the dump invented.
  --
  -- This is staging's job rather than the mart's, and the distinction is worth
  -- keeping straight: staging corrects the DUMP against the SOURCE -- here, a
  -- name the scrape lifted out of prose that IPDB's own page denies -- while the
  -- mart translates the SOURCE's vocabulary into ours. Ids 0 and 328, IPDB's two
  -- ways of writing "no manufacturer", are faithfully scraped and are therefore
  -- the mart's to translate, not staging's to correct.
  --
  -- That list is written by hand, not detected. Recognising that a manufacturer
  -- name is a fragment of prose lifted out of the Notes means reading the page,
  -- so every row carries the URL it was read from and nothing lands there
  -- automatically.
       REPLACE (
         CASE WHEN misparsed.ipdb_id IS NULL THEN im.Manufacturer   END AS Manufacturer,
         CASE WHEN misparsed.ipdb_id IS NULL THEN im.ManufacturerId END AS ManufacturerId,
         -- The only two values taken from the archive pages, and both only
         -- where xantari is silent. A NAMED LIST, not a blanket coalesce: a
         -- xantari NULL does not mean IPDB is empty, it can equally mean IPDB
         -- withdrew the value after the capture, so every fill is a decision
         -- someone made rather than a rule that fires on absence.
         COALESCE(im.AverageFunRating, arc.rating_score)    AS AverageFunRating,
         COALESCE(im.ProductionNumber, arc.production_units) AS ProductionNumber
       ),

  COALESCE(tg1.slug, tg2.slug) AS technology_generation_slug,
  ad.additional_details_date_string,
  ad.additional_details_date_year,
  ad.additional_details_date_month,
  ad.additional_details_date_day,

  -- WHAT KIND OF DATE the header line is stating. The archive pages' entire
  -- contribution to dates, and it is a label, never a value: on every model
  -- they resolve, their date equals the header parse to the day.
  --
  -- IPDB changed how the header picks a date on 2018-04-26, announced at
  -- https://pinside.com/pinball/forum/topic/display-of-ipdb-project-dates-and-manufacture-dates
  -- Since then a listing holding both dates shows the MANUFACTURE date in the
  -- header (before, it showed the project date), and a listing holding one shows
  -- it in the body too, labelled. Both xantari snapshots post-date that change,
  -- so `DateOfManufacture` being present means the header is showing it.
  --
  -- Which is why the last branch is `project_inferred` and not `project`. By
  -- IPDB's rule a header date with no `DateOfManufacture` IS a project date --
  -- but that reasoning trusts xantari to have captured a manufacture date
  -- wherever IPDB states one, and archive pages show plain `Date Of Manufacture`
  -- rows that the dump simply dropped.
  -- The inference is sound about IPDB and unreliable about xantari, so the rows
  -- resting on it are marked and reported by
  -- `ipdb_archive_header_date_inferred` rather than mixed in with the observed
  -- ones.
  CASE
    WHEN ad.additional_details_date_year IS NULL       THEN NULL
    WHEN im.DateOfManufacture IS NOT NULL              THEN 'manufacture'
    WHEN arc.manufacture_date_year IS NOT NULL         THEN 'manufacture'
    WHEN arc.project_date_year IS NOT NULL             THEN 'project'
    ELSE 'project_inferred'
  END AS additional_details_date_kind,

  -- Which source the filled value above came from. Only these two fills exist,
  -- so only these two markers do.
  CASE WHEN im.AverageFunRating IS NOT NULL THEN 'xantari'
       WHEN arc.rating_score    IS NOT NULL THEN 'archive' END AS rating_src,
  CASE WHEN im.ProductionNumber IS NOT NULL THEN 'xantari'
       WHEN arc.production_units IS NOT NULL THEN 'archive' END AS production_src,

  -- Archive-only fields: xantari has no column for any of these, so there is
  -- nothing to coalesce against and no marker to carry -- their source is
  -- implied by their existing at all.
  --
  -- `production_never_produced` is the substantial one. IPDB records a
  -- never-produced design by writing a STATUS where a unit count would go, and
  -- the dump models only the count, so the status was invisible to us.
  arc.rating_ratings,
  arc.rating_comments,
  arc.rating_provisional,
  arc.production_status,
  arc.production_never_produced,
  arc.serial_number_database_url,
  arc.owners_list_url,
  arc.easter_eggs_text,
  arc.easter_eggs_url,

  -- What a patch cites when the claim rests on the archive page: the address,
  -- and the date the words on it were true.
  arc.archive_source_url,
  arc.archive_capture_date
FROM ipdb_stg.models_merged AS im
LEFT JOIN ipdb_ref.model_corporate_entity_misparsed AS misparsed
  ON misparsed.ipdb_id = im.IpdbId
LEFT JOIN ipdb_stg.model_additional_details AS ad
  ON ad.IpdbId = im.IpdbId
LEFT JOIN ipdb_stg.archive_models AS arc
  ON arc.ipdb_id = im.IpdbId
LEFT JOIN ipdb_ref.technology_generation AS tg1
  ON im.TypeShortName = tg1.type_short_name AND tg1.type_short_name IS NOT NULL
LEFT JOIN ipdb_ref.technology_generation AS tg2
  ON im."Type" = tg2.type_full AND tg2.type_full IS NOT NULL;

-- Distinct corporate entities parsed from IPDB manufacturer strings.
-- Splits the structured string into company name, trade name, years, location,
-- and HQ city/state/country (with US state detection and override handling).
--
-- Staging because `ipdb_stg.files` reads it to name a file's maker.
-- `ipdb.corporate_entities` is the published translation.
CREATE OR REPLACE VIEW ipdb_stg.corporate_entities AS
WITH raw_extractions AS (
  -- Run each regex on Manufacturer exactly once, named for what it produces.
  SELECT DISTINCT
    ManufacturerId        AS ipdb_manufacturer_id,
    Manufacturer          AS raw_name,
    regexp_replace(Manufacturer, '\s*\[Trade Name:.*?\]', '')                     AS _sans_trade,
    regexp_extract(Manufacturer, '\[Trade Name:\s*(.+?)\]', 1)                    AS trade_name,
    regexp_extract(Manufacturer, '\((\d{4})-', 1)                                 AS _year_start_raw,
    regexp_extract(Manufacturer, '\(\d{4}-(\d{4})\)', 1)                          AS _year_end_raw,
    regexp_extract(Manufacturer, '\((\d{4})\)', 1)                                AS _single_year_raw,
    regexp_extract(Manufacturer, ',\s*of\s+(.+?)(?:\s*\(\d|\s*\[Trade|\s*$)', 1) AS _location_raw
  FROM ipdb_stg.models
  WHERE Manufacturer IS NOT NULL
    AND ManufacturerId NOT IN (SELECT ipdb_manufacturer_id FROM ipdb_ref.corporate_entity_not_a_company)
),
parsed AS (
  SELECT
    ipdb_manufacturer_id,
    raw_name,
    trade_name,

    -- Company name: strip years and ", of ..." from the already-trade-stripped string
    trim(trailing ',' FROM trim(
      regexp_replace(
        regexp_replace(_sans_trade, '\s*\(\d+.*?\)', ''),
        ',\s*of\s+.*$', '')
    )) AS company_name,

    -- Year range: each pattern extracted once above, cast here
    CASE WHEN _year_start_raw  != '' THEN CAST(_year_start_raw  AS INTEGER) END AS year_start,
    CASE WHEN _year_end_raw    != '' THEN CAST(_year_end_raw    AS INTEGER) END AS year_end,
    CASE WHEN _single_year_raw != '' AND raw_name NOT LIKE '%-%(%'
         THEN CAST(_single_year_raw AS INTEGER) END AS single_year,

    -- The ", of ..." segment, as IPDB writes it. Left whole rather than split into
    -- city/state/country: telling "St. Paul, Minnesota" from "Paris, France" needs a
    -- vocabulary of US states, and the catalog holds one. Splitting it here without
    -- that vocabulary reads Minnesota as a country.
    COALESCE(trim(trailing ',' FROM _location_raw), '') AS location

  FROM raw_extractions
)
SELECT
  p.ipdb_manufacturer_id, p.raw_name,
  p.company_name, p.trade_name,
  -- IPDB states a trade name only where it differs from the company name, so the
  -- brand is the trade name when there is one and the company name otherwise.
  CASE WHEN p.trade_name != '' THEN p.trade_name ELSE p.company_name END AS manufacturer_name,
  p.year_start, p.year_end, p.single_year, p.location,
  model_years.year_of_first_model,
  model_years.year_of_last_model
FROM parsed p
LEFT JOIN (
  SELECT
    ManufacturerId,
    MIN(EXTRACT(YEAR FROM CAST(DateOfManufacture AS DATE)))::INTEGER AS year_of_first_model,
    MAX(EXTRACT(YEAR FROM CAST(DateOfManufacture AS DATE)))::INTEGER AS year_of_last_model
  FROM ipdb_stg.models
  WHERE DateOfManufacture IS NOT NULL
  GROUP BY ManufacturerId
) model_years ON model_years.ManufacturerId = p.ipdb_manufacturer_id;

------------------------------------------------------------
-- IPDB credits
------------------------------------------------------------

-- IPDB's seven credit fields, flattened to one row per credited person.
--
-- The sentinel filter is what makes a row mean "IPDB credits this person". IPDB
-- spells an unknown credit a dozen ways -- '(undisclosed)', 'unknown', '?', an
-- empty string -- each of which would otherwise become a person of that name,
-- credited on hundreds of models.
--
-- Names are not resolved to catalog records: which person a name means is a
-- question about the catalog, answered better by its own alias pool.
--
-- Materialized because the seven-branch UNION runs once per query otherwise.
CREATE OR REPLACE TABLE ipdb_stg.credits AS
WITH raw AS (
  SELECT IpdbId, 'Design' AS role, TRIM(UNNEST(string_split(DesignBy, ','))) AS person_name FROM ipdb_stg.models_merged WHERE DesignBy <> ''
  UNION ALL
  SELECT IpdbId, 'Art', TRIM(UNNEST(string_split(ArtBy, ','))) FROM ipdb_stg.models_merged WHERE ArtBy <> ''
  UNION ALL
  SELECT IpdbId, 'Dots/Animation', TRIM(UNNEST(string_split(DotsAnimationBy, ','))) FROM ipdb_stg.models_merged WHERE DotsAnimationBy <> ''
  UNION ALL
  SELECT IpdbId, 'Mechanics', TRIM(UNNEST(string_split(MechanicsBy, ','))) FROM ipdb_stg.models_merged WHERE MechanicsBy <> ''
  UNION ALL
  SELECT IpdbId, 'Music', TRIM(UNNEST(string_split(MusicBy, ','))) FROM ipdb_stg.models_merged WHERE MusicBy <> ''
  UNION ALL
  SELECT IpdbId, 'Sound', TRIM(UNNEST(string_split(SoundBy, ','))) FROM ipdb_stg.models_merged WHERE SoundBy <> ''
  UNION ALL
  SELECT IpdbId, 'Software', TRIM(UNNEST(string_split(SoftwareBy, ','))) FROM ipdb_stg.models_merged WHERE SoftwareBy <> ''
  UNION ALL
  -- Archive-only roles. `xantari_field IS NULL` prevents this branch from
  -- overriding dump data; an unmatched page label is reported by
  -- `ipdb_archive_credit_role_unrecognised`.
  SELECT am.ipdb_id, cr.ipdb_role, TRIM(c."name")
  FROM ipdb_stg.archive_models AS am,
       unnest(am.credits) AS t(c)
  JOIN ipdb_ref.credit_role AS cr
    ON cr.xantari_field IS NULL
   AND list_contains(cr.archive_label, c.role)
)
SELECT r.IpdbId, r.role, cr.role_slug, r.person_name
FROM raw r
LEFT JOIN ipdb_ref.credit_role cr ON cr.ipdb_role = r.role
WHERE LOWER(r.person_name) NOT IN (
  '(undisclosed)', 'undisclosed', 'unknown', 'missing', 'null', 'undefined',
  'n/a', 'none', 'tbd', 'tba', '?', ''
)
  AND r.person_name NOT ILIKE '%(undisclosed)%'
  AND r.person_name NOT ILIKE '%unknown%';

------------------------------------------------------------
-- IPDB file trove
------------------------------------------------------------

-- IPDB's per-model file arrays, flattened to one row per file. Its seven
-- array names collapse two different axes: `rom` and `service_bulletin` say
-- what a file *is*, `image` and `file` say how it is delivered.
CREATE OR REPLACE VIEW ipdb_stg.model_files AS
SELECT
  IpdbId AS ipdb_id,
  Title AS model_name,
  f.Url AS file_url,
  f."Name" AS file_name,
  category
FROM ipdb_stg.models_merged, (
  SELECT unnest(ImageFiles) AS f, 'image' AS category
  UNION ALL SELECT unnest(Documentation), 'documentation'
  UNION ALL SELECT unnest(Files), 'file'
  UNION ALL SELECT unnest(RuleSheetUrls), 'rule_sheet'
  UNION ALL SELECT unnest(ROMs), 'rom'
  UNION ALL SELECT unnest(ServiceBulletins), 'service_bulletin'
  UNION ALL SELECT unnest(MultimediaFiles), 'multimedia'
);

-- One row per file with the delivery axis resolved and its model's context
-- carried alongside. Everything here is mechanical: the container is the URL's
-- extension, and the rest is the model's own attributes.
--
-- `model_mpu` is IPDB's free-text platform string, carried so a consumer can
-- group by platform without a vocabulary. Every join is on a unique key, so none
-- multiplies the file count.
--
-- Staging because `ipdb_stg.file_class_matches` below reads it. `ipdb.documents`
-- is the published answer to the same question.
CREATE OR REPLACE VIEW ipdb_stg.files AS
SELECT
  f.ipdb_id,
  f.model_name,
  f.category AS ipdb_category,
  f.file_name,
  f.file_url,
  regexp_extract(f.file_url, '([^/]+)$', 1) AS file_basename,
  nullif(lower(regexp_extract(f.file_url, '\.([A-Za-z0-9]{1,5})$', 1)), '') AS container,
  corporate_entity.manufacturer_name AS model_manufacturer,
  s.MPU AS model_mpu
FROM ipdb_stg.model_files AS f
LEFT JOIN ipdb_stg.models AS s ON s.IpdbId = f.ipdb_id
LEFT JOIN ipdb_stg.corporate_entities AS corporate_entity
  ON corporate_entity.ipdb_manufacturer_id = s.ManufacturerId;

-- Applying the declared patterns to the staged files: one row per file per
-- class it matches. The judgement is all in `ipdb_ref.document_class_pattern`;
-- this only runs it.
--
-- In two stages for the cost reason `ipdb_ref.document_class_pattern` describes:
-- stage one pairs a file with a pattern only when one of that pattern's declared
-- literals appears in the name, a substring test; stage two runs the real regex
-- on the few survivors. `required_any` is necessary only, so the verdict stays
-- entirely the regex's.
--
-- Materialized so the pairing runs once per build rather than once per query.
CREATE OR REPLACE TABLE ipdb_stg.file_class_matches AS
WITH needles AS (
  SELECT
    document_class,
    pattern,
    allow_containers,
    deny_pattern,
    unnest(required_any) AS needle
  FROM ipdb_ref.document_class_pattern
),
candidates AS (
  SELECT DISTINCT
    f.ipdb_id,
    f.file_url,
    f.ipdb_category,
    f.file_name,
    f.container,
    n.document_class,
    n.pattern,
    n.allow_containers,
    n.deny_pattern
  FROM ipdb_stg.files AS f
  INNER JOIN needles AS n ON contains(lower(f.file_name), n.needle)
)
-- DISTINCT because a class may be detected by more than one pattern row, and
-- which row fired is not projected: two matches on the same class would be
-- indistinguishable rows, and would double any count taken off this table.
SELECT DISTINCT
  cd.ipdb_id,
  cd.file_url,
  cd.ipdb_category,
  cd.file_name,
  cd.container,
  cd.document_class,
  c.source_kind
FROM candidates AS cd
INNER JOIN ipdb_ref.document_class AS c USING (document_class)
WHERE regexp_matches(lower(cd.file_name), cd.pattern)
  AND (
    cd.allow_containers IS NULL
    OR list_contains(cd.allow_containers, coalesce(cd.container, ''))
  )
  AND (
    cd.deny_pattern IS NULL
    OR NOT regexp_matches(lower(cd.file_name), cd.deny_pattern)
  );

------------------------------------------------------------
-- IPDB archive pages, unnested
------------------------------------------------------------

-- The three list-valued page fields, one row per element.
--
-- STAGING, not mart, and deliberately so. Themes and documents are here to be
-- COMPARED against what xantari already gives us -- IPDB's theme string is one
-- field in the dump and a list on the page, and the page's document rows carry a
-- section, kind and size the dump's flat url/name pairs do not. Whether any of
-- that is worth taking is a question to answer by querying these, and a mart
-- view would promise flippatch an answer nobody has reached yet.

-- IPDB's themes as the page lists them, against `ipdb.models.theme`, which is
-- xantari's single delimited string for the same thing.
CREATE OR REPLACE VIEW ipdb_stg.archive_model_themes AS
SELECT ipdb_id, unnest(themes) AS theme
FROM ipdb_stg.archive_models
WHERE len(themes) > 0;

-- The page's document listings, richer than `ipdb_stg.model_files`: that view
-- has a url and a name, these rows also say which section of the page the file
-- was listed under and what IPDB calls it.
CREATE OR REPLACE VIEW ipdb_stg.archive_model_documents AS
SELECT
  am.ipdb_id,
  d.section,
  d."name" AS file_name,
  d.url    AS file_url,
  d.kind,
  d.size,
  d.credit
FROM ipdb_stg.archive_models AS am, unnest(am.documents) AS t(d);

-- Page specialties in IPDB's wording. Capture provenance stays on each unnested
-- row rather than depending on a later join whose uniqueness is not asserted.
CREATE OR REPLACE VIEW ipdb_stg.archive_model_specialties AS
SELECT
  ipdb_id,
  unnest(specialties) AS specialty,
  archive_source_url,
  archive_capture_date
FROM ipdb_stg.archive_models
WHERE len(specialties) > 0;

------------------------------------------------------------
-- Fandom staged
------------------------------------------------------------

-- Fandom is parsed but never compared. It duplicates information we already
-- hold from better sources and we have not acquired its ids, so there is no
-- `fandom` mart -- comparing would only surface the ids we chose not to get.

CREATE OR REPLACE VIEW fandom_stg.games AS
SELECT
  page_id,
  title AS fandom_name,
  regexp_extract(wikitext, '\|manufacturer\s*=\s*\[\[([^\]]+)', 1) AS manufacturer,
  regexp_extract(wikitext, '\|system\s*=\s*\[\[([^\]]+)', 1) AS system,
  TRY_CAST(regexp_extract(wikitext, '\|release\s*=\s*.*?\[\[(\d{4})', 1) AS INTEGER) AS year,
  TRY_CAST(replace(regexp_extract(wikitext, '\|production\s*=\s*([\d,]+)', 1), ',', '') AS INTEGER) AS production,
  wikitext
FROM fandom_raw.games;

CREATE OR REPLACE VIEW fandom_stg.manufacturers AS
SELECT
  page_id,
  title AS fandom_name,
  wikitext
FROM fandom_raw.manufacturers;

CREATE OR REPLACE VIEW fandom_stg.people AS
SELECT
  page_id,
  title AS fandom_name,
  wikitext
FROM fandom_raw.people;
