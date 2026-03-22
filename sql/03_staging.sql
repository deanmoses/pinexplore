-- Per-source transforms.
-- Makes each source independently queryable with normalized slugs.
-- No cross-source joins.

------------------------------------------------------------
-- OPDB staged
------------------------------------------------------------

-- Add technology/display slugs and extract manufacturer name
CREATE OR REPLACE VIEW opdb_machines_staged AS
SELECT
  om.*,
  (om.manufacturer ->> 'name') AS manufacturer_name,
  tg.slug AS technology_generation_slug,
  dt.slug AS display_type_slug
FROM opdb_machines AS om
LEFT JOIN ref_opdb_technology_generation AS tg ON om."type" = tg.opdb_type
LEFT JOIN ref_opdb_display_type AS dt ON om.display = dt.opdb_display;

-- Distinct manufacturers from OPDB
CREATE OR REPLACE VIEW opdb_manufacturers AS
SELECT DISTINCT
  om.manufacturer.manufacturer_id AS opdb_manufacturer_id,
  (om.manufacturer ->> 'name') AS "name",
  (om.manufacturer ->> 'full_name') AS full_name
FROM opdb_machines AS om
WHERE om.manufacturer IS NOT NULL
ORDER BY "name";

-- OPDB manufacturers mapped to pinbase manufacturer slugs.
-- Uses exact name match, normalized match, or alias table.
CREATE OR REPLACE VIEW opdb_manufacturers_mapped AS
SELECT
  om.opdb_manufacturer_id,
  om.name AS opdb_name,
  om.full_name AS opdb_full_name,
  COALESCE(
    m_exact.slug,
    m_norm.slug,
    alias.manufacturer_slug
  ) AS manufacturer_slug
FROM opdb_manufacturers om
LEFT JOIN manufacturers m_exact
  ON lower(m_exact.name) = lower(om.name)
LEFT JOIN (
  SELECT slug, normalize_mfr_name(name) AS norm_name
  FROM manufacturers
  WHERE normalize_mfr_name(name) != ''
  QUALIFY count(*) OVER (PARTITION BY normalize_mfr_name(name)) = 1
) m_norm
  ON m_norm.norm_name = normalize_mfr_name(om.name)
  AND m_exact.slug IS NULL
LEFT JOIN ref_opdb_manufacturer_aliases alias
  ON alias.opdb_manufacturer_id = om.opdb_manufacturer_id
  AND m_exact.slug IS NULL AND m_norm.slug IS NULL;

-- OPDB feature string → pinbase slug mappings.
-- Matches on name (case-insensitive) or explicit aliases.

CREATE OR REPLACE VIEW ref_feature_tag AS
SELECT feature, slug AS tag_slug FROM (
  SELECT lower(name) AS feature, slug FROM tags
  UNION
  SELECT lower(unnest(aliases)), slug FROM tags WHERE aliases IS NOT NULL
);

CREATE OR REPLACE VIEW ref_feature_gameplay AS
SELECT feature, slug AS gameplay_feature_slug FROM (
  SELECT lower(name) AS feature, slug FROM gameplay_features
  UNION
  SELECT lower(unnest(aliases)), slug FROM gameplay_features WHERE aliases IS NOT NULL
);

CREATE OR REPLACE VIEW ref_feature_reward_type AS
SELECT feature, slug AS reward_type_slug FROM (
  SELECT lower(name) AS feature, slug FROM reward_types
  UNION
  SELECT lower(unnest(aliases)), slug FROM reward_types WHERE aliases IS NOT NULL
);

CREATE OR REPLACE VIEW ref_feature_cabinet AS
SELECT feature, slug AS cabinet_slug FROM (
  SELECT lower(name) AS feature, slug FROM cabinets
  UNION
  SELECT lower(unnest(aliases)), slug FROM cabinets WHERE aliases IS NOT NULL
);

-- Unnested keywords per machine
CREATE OR REPLACE VIEW opdb_keywords AS
SELECT opdb_id, "name", unnest(keywords) AS keyword
FROM opdb_machines
WHERE len(keywords) > 0;

------------------------------------------------------------
-- IPDB staged
------------------------------------------------------------

-- Add technology generation slug and system/subgeneration via MPU match.
-- Filters out unknown/null manufacturers (ManufacturerId 0 or 328).
CREATE OR REPLACE VIEW ipdb_machines_staged AS
SELECT
  im.*,
  COALESCE(tg1.slug, tg2.slug) AS technology_generation_slug,
  ps.slug AS system_slug,
  ps.technology_subgeneration_slug
FROM ipdb_machines AS im
LEFT JOIN ref_ipdb_technology_generation AS tg1
  ON im.TypeShortName = tg1.type_short_name AND tg1.type_short_name IS NOT NULL
LEFT JOIN ref_ipdb_technology_generation AS tg2
  ON im."Type" = tg2.type_full AND tg2.type_full IS NOT NULL
LEFT JOIN systems AS ps
  ON list_contains(ps.mpu_strings, im.MPU)
WHERE im.ManufacturerId NOT IN (0, 328);

------------------------------------------------------------
-- IPDB gameplay features (parsed from NotableFeatures)
------------------------------------------------------------

-- Extract "Feature (N)" patterns from IPDB NotableFeatures free text
-- and resolve them against the pinbase gameplay_features vocabulary.
--
-- Strategy: split on delimiters first, then look for "(N)" in each
-- segment. This avoids mid-word false positives (e.g. "LED" → "Ed")
-- and captures multi-word names (e.g. "Vertical Up-kickers").
--
-- Unmatched features are retained with a NULL slug for gap analysis.
CREATE OR REPLACE VIEW ipdb_gameplay_features AS
WITH

-- Step 1: Clean the raw text before splitting.
cleaned AS (
    SELECT
        i.IpdbId,
        replace(
            regexp_replace(i.NotableFeatures,
                '^Notable Features:\s*',  -- some entries have this prefix
                ''),
            '�',  -- mojibake U+FFFD replaces hyphens, apostrophes, bullets
            ' '
        ) AS features_text
    FROM ipdb_machines_staged i
    WHERE i.NotableFeatures IS NOT NULL
),

-- Step 2: Split into segments on comma or period-followed-by-space.
-- Each segment is one potential "Feature name (count)" entry.
-- Example: "Flippers (2), Pop bumpers (3). Left kickback."
--       → ["Flippers (2)", "Pop bumpers (3)", "Left kickback"]
segments AS (
    SELECT
        c.IpdbId,
        trim(unnest(regexp_split_to_array(
            c.features_text,
            ',|\.\s'  -- comma OR period+whitespace
        ))) AS segment
    FROM cleaned c
),

-- Step 3: From segments containing "(N)", extract the feature name
-- and count. The name is everything before the parenthesized number.
-- Example: "5-bank drop targets (2)" → name="5-bank drop targets", qty=2
parsed AS (
    SELECT
        IpdbId,
        lower(trim(
            regexp_extract(segment,
                '^(.+?)\s*\((\d+).*\)',  -- name (count...)
                1)                        -- capture group 1 = name
        )) AS feature_name,
        TRY_CAST(
            regexp_extract(segment,
                '\((\d+)',  -- first number inside parens
                1)
        AS INTEGER) AS quantity
    FROM segments
    WHERE regexp_matches(segment, '\(\d+\)')  -- has a parenthesized number
)

-- Step 4: Resolve feature names against pinbase vocabulary.
SELECT
    p.IpdbId,
    p.feature_name AS ipdb_feature,
    p.quantity,
    rfg.gameplay_feature_slug
FROM parsed p
LEFT JOIN ref_feature_gameplay rfg ON p.feature_name = rfg.feature
WHERE p.feature_name != '';

------------------------------------------------------------
-- IPDB reward types (parsed from NotableFeatures)
------------------------------------------------------------

-- Extract reward type mentions from IPDB NotableFeatures free text.
-- Unlike gameplay features, reward types appear as keywords rather than
-- "Feature (N)" patterns, so we match whole words case-insensitively.
CREATE OR REPLACE VIEW ipdb_reward_types AS
SELECT DISTINCT
    i.IpdbId,
    rt.feature AS ipdb_feature,
    rt.reward_type_slug
FROM ipdb_machines_staged i
INNER JOIN ref_feature_reward_type rt
    ON i.NotableFeatures IS NOT NULL
    AND regexp_matches(i.NotableFeatures, '\b' || rt.feature || '\b', 'i');

------------------------------------------------------------
-- Theme views (derived from pinbase themes table)
------------------------------------------------------------

-- Alias → canonical name mapping (one row per alias→theme).
CREATE OR REPLACE VIEW theme_aliases AS
SELECT unnest(t.aliases) AS raw_theme, t.name AS canonical_theme
FROM themes t
WHERE t.aliases IS NOT NULL;

-- Parent relationships (one row per child→parent edge).
CREATE OR REPLACE VIEW theme_parents AS
SELECT t.name AS theme, unnest(t.parents) AS parent
FROM themes t
WHERE t.parents IS NOT NULL;

------------------------------------------------------------
-- Gameplay feature views (derived from pinbase gameplay_features table)
------------------------------------------------------------

-- Parent relationships (one row per child→parent edge).
CREATE OR REPLACE VIEW gameplay_feature_parents AS
SELECT gf.slug AS feature, unnest(gf.is_type_of) AS parent
FROM gameplay_features gf
WHERE gf.is_type_of IS NOT NULL;

------------------------------------------------------------
-- IPDB theme views
------------------------------------------------------------

-- Distinct IPDB themes: split compound Theme strings into individual terms.
-- IPDB stores themes as "Adventure - Fantasy - Outer Space", sometimes with
-- slash pairs ("Cards/Gambling"), commas, and mojibake (U+FFFD for dashes).
-- This view normalises delimiters, splits into atomic terms, title-cases,
-- and deduplicates so downstream comparison can work term-by-term.
CREATE OR REPLACE VIEW ipdb_themes AS
WITH
  -- 1. Fix encoding damage and normalise delimiters to " - "
  cleaned AS (
    SELECT
      im.IpdbId,
      -- Replace mojibake (U+FFFD) separator with dash, then comma with dash
      replace(
        replace(im.Theme, ' ' || chr(65533) || ' ', ' - '),
        ', ', ' - '
      ) AS theme_clean
    FROM ipdb_machines im
    WHERE im.Theme IS NOT NULL AND im.Theme <> ''
  ),
  -- 2. Split on " - " into individual tokens
  dash_split AS (
    SELECT c.IpdbId, trim(t.token) AS token
    FROM cleaned c,
    LATERAL unnest(string_split(c.theme_clean, ' - ')) AS t(token)
    WHERE trim(t.token) <> ''
  ),
  -- 3. Expand slash-delimited pairs into separate terms.
  --    E.g. "Cards/Gambling" → "Cards", "Gambling"
  --    Also handles "Circus / Carnival" (with spaces around slash).
  slash_split AS (
    SELECT d.IpdbId, trim(s.part) AS token
    FROM dash_split d,
    LATERAL unnest(string_split(d.token, '/')) AS s(part)
    WHERE trim(s.part) <> ''
  ),
  -- 4. Strip surrounding quotes (e.g. '"21 or Bust"' → '21 or Bust')
  unquoted AS (
    SELECT IpdbId, trim(token, '"') AS token
    FROM slash_split
    WHERE trim(token, '"') <> ''
  ),
  -- 5. Strip "Theme: " prefix (IPDB split artifact)
  prefix_cleaned AS (
    SELECT IpdbId,
      CASE WHEN token LIKE 'Theme: %' THEN trim(token[8:]) ELSE token END AS token
    FROM unquoted
  ),
  -- 6. Title-case each token
  title_cased AS (
    SELECT DISTINCT
      IpdbId,
      list_aggregate(
        list_transform(
          string_split(lower(token), ' '),
          w -> upper(w[1]) || w[2:]
        ),
        'string_agg', ' '
      ) AS theme
    FROM prefix_cleaned
  )
-- 7. Apply alias table to merge duplicates into canonical forms
SELECT DISTINCT
  tc.IpdbId,
  COALESCE(a.canonical_theme, tc.theme) AS theme
FROM title_cased tc
LEFT JOIN theme_aliases a ON a.raw_theme = tc.theme;

-- Distinct corporate entities parsed from IPDB manufacturer strings.
-- Splits the structured string into company name, trade name, years, location,
-- and HQ city/state/country (with US state detection and override handling).
CREATE OR REPLACE VIEW ipdb_corporate_entities AS
WITH raw_extractions AS (
  -- Run each regex on Manufacturer exactly once, named for what it produces.
  SELECT DISTINCT
    ManufacturerId        AS ipdb_manufacturer_id,
    Manufacturer          AS raw_name,
    ManufacturerShortName AS short_name,
    regexp_replace(Manufacturer, '\s*\[Trade Name:.*?\]', '')                     AS _sans_trade,
    regexp_extract(Manufacturer, '\[Trade Name:\s*(.+?)\]', 1)                    AS trade_name,
    regexp_extract(Manufacturer, '\((\d{4})-', 1)                                 AS _year_start_raw,
    regexp_extract(Manufacturer, '\(\d{4}-(\d{4})\)', 1)                          AS _year_end_raw,
    regexp_extract(Manufacturer, '\((\d{4})\)', 1)                                AS _single_year_raw,
    regexp_extract(Manufacturer, ',\s*of\s+(.+?)(?:\s*\(\d|\s*\[Trade|\s*$)', 1) AS _location_raw
  FROM ipdb_machines
  WHERE Manufacturer IS NOT NULL
    AND Manufacturer != 'Unknown Manufacturer'
),
parsed AS (
  SELECT
    ipdb_manufacturer_id,
    raw_name,
    short_name,
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

    -- Full location string from ", of ..."
    COALESCE(trim(trailing ',' FROM _location_raw), '') AS location

  FROM raw_extractions
),
-- Split location into raw city/state/country with US state detection
with_location AS (
  SELECT
    p.*,
    string_split(p.location, ', ') AS parts,
    len(string_split(p.location, ', ')) AS nparts
  FROM parsed p
),
with_raw_hq AS (
  SELECT
    p.*,
    CASE
      WHEN p.location = '' THEN NULL
      WHEN p.nparts >= 2 THEN p.parts[1]
      ELSE NULL
    END AS _raw_city,
    CASE
      WHEN p.location = '' THEN NULL
      WHEN p.nparts >= 3 THEN p.parts[2]
      WHEN p.nparts = 2 THEN st2.canonical_name
      WHEN p.nparts = 1 THEN st1.canonical_name
      ELSE NULL
    END AS _raw_state,
    CASE
      WHEN p.location = '' THEN NULL
      WHEN p.nparts >= 3 THEN p.parts[p.nparts]
      WHEN p.nparts = 2 AND st2.canonical_name IS NOT NULL THEN 'USA'
      WHEN p.nparts = 2 THEN p.parts[2]
      WHEN p.nparts = 1 AND st1.canonical_name IS NOT NULL THEN 'USA'
      ELSE p.location
    END AS _raw_country
  FROM with_location p
  LEFT JOIN ref_us_states st2
    ON p.nparts = 2 AND lower(st2.state_name) = lower(p.parts[2])
  LEFT JOIN ref_us_states st1
    ON p.nparts = 1 AND lower(st1.state_name) = lower(p.location)
)
SELECT
  h.ipdb_manufacturer_id, h.raw_name, h.short_name,
  h.company_name, h.trade_name,
  CASE WHEN h.trade_name != '' THEN h.trade_name ELSE h.company_name END AS manufacturer_name,
  h.year_start, h.year_end, h.single_year, h.location,

  CASE WHEN ovr.ipdb_manufacturer_id IS NOT NULL THEN ovr.headquarters_city ELSE h._raw_city END AS headquarters_city,
  CASE WHEN ovr.ipdb_manufacturer_id IS NOT NULL THEN ovr.headquarters_state ELSE h._raw_state END AS headquarters_state,
  COALESCE(cn.normalized_country,
    CASE WHEN ovr.ipdb_manufacturer_id IS NOT NULL THEN ovr.headquarters_country ELSE h._raw_country END
  ) AS headquarters_country,

  -- Manufacturer resolution — derived purely from IPDB data + pinbase manufacturers.
  -- No dependency on existing corporate_entities or models tables.
  -- 1. Exact match: manufacturer_name → manufacturer.name
  -- 2. Normalized match (unambiguous): strip business suffixes, match if unique
  -- NULL if neither matches — that's a gap to investigate, not paper over.
  COALESCE(mfr_exact.slug, mfr_normalized.slug) AS manufacturer_slug,

  model_years.year_of_first_model,
  model_years.year_of_last_model
FROM with_raw_hq h
LEFT JOIN ref_ipdb_location_overrides ovr
  ON ovr.ipdb_manufacturer_id = h.ipdb_manufacturer_id
LEFT JOIN ref_country_normalization cn
  ON cn.raw_country = COALESCE(ovr.headquarters_country, h._raw_country)
LEFT JOIN manufacturers mfr_exact
  ON lower(mfr_exact.name) = lower(
    CASE WHEN h.trade_name != '' THEN h.trade_name ELSE h.company_name END
  )
LEFT JOIN (
  SELECT slug, normalize_mfr_name(name) AS norm_name
  FROM manufacturers
  WHERE normalize_mfr_name(name) != ''
  QUALIFY count(*) OVER (PARTITION BY normalize_mfr_name(name)) = 1
) mfr_normalized
  ON mfr_normalized.norm_name = normalize_mfr_name(
    CASE WHEN h.trade_name != '' THEN h.trade_name ELSE h.company_name END
  )
  AND mfr_exact.slug IS NULL
LEFT JOIN (
  SELECT
    ManufacturerId,
    MIN(EXTRACT(YEAR FROM CAST(DateOfManufacture AS DATE)))::INTEGER AS year_of_first_model,
    MAX(EXTRACT(YEAR FROM CAST(DateOfManufacture AS DATE)))::INTEGER AS year_of_last_model
  FROM ipdb_machines
  WHERE DateOfManufacture IS NOT NULL
  GROUP BY ManufacturerId
) model_years ON model_years.ManufacturerId = h.ipdb_manufacturer_id;

------------------------------------------------------------
-- Fandom staged
------------------------------------------------------------

-- Fandom games: extract structured fields from wikitext infobox
CREATE OR REPLACE VIEW fandom_games_staged AS
SELECT
  page_id,
  title AS fandom_name,
  regexp_extract(wikitext, '\|manufacturer\s*=\s*\[\[([^\]]+)', 1) AS manufacturer,
  regexp_extract(wikitext, '\|system\s*=\s*\[\[([^\]]+)', 1) AS system,
  TRY_CAST(regexp_extract(wikitext, '\|release\s*=\s*.*?\[\[(\d{4})', 1) AS INTEGER) AS year,
  TRY_CAST(replace(regexp_extract(wikitext, '\|production\s*=\s*([\d,]+)', 1), ',', '') AS INTEGER) AS production,
  wikitext
FROM fandom_games;

-- Fandom manufacturers: extract fields from wikitext infobox
CREATE OR REPLACE VIEW fandom_manufacturers_staged AS
SELECT
  page_id,
  title AS fandom_name,
  wikitext
FROM fandom_manufacturers;

-- Fandom persons: extract fields from wikitext
CREATE OR REPLACE VIEW fandom_persons_staged AS
SELECT
  page_id,
  title AS fandom_name,
  wikitext
FROM fandom_persons;

------------------------------------------------------------
-- Pinbase staged
------------------------------------------------------------

-- Flat credits: one row per model + person + role
CREATE OR REPLACE VIEW pinbase_credits AS
SELECT
  m.slug AS model_slug,
  m.title_slug,
  unnest(m.credit_refs).person_slug AS person_slug,
  unnest(m.credit_refs)."role" AS "role"
FROM models AS m
WHERE m.credit_refs IS NOT NULL AND len(m.credit_refs) > 0;

------------------------------------------------------------
-- Shared materialized tables (used by checks + compare)
------------------------------------------------------------

-- Person name/alias lookup: maps lowercase name → slug.
-- Materialized once; referenced by checks and compare layers.
CREATE OR REPLACE TABLE _person_lookup AS
SELECT slug, LOWER(name) AS lookup_name FROM people
UNION ALL
SELECT slug, LOWER(UNNEST(aliases)) FROM people WHERE aliases IS NOT NULL;

-- IPDB credits flattened + sentinel-filtered, with person resolution.
-- Materializes the 7-branch UNION ALL + sentinel filter that was previously
-- duplicated across checks and compare files.
CREATE OR REPLACE TABLE _ipdb_credits AS
WITH raw AS (
  SELECT IpdbId, 'Design' AS role, TRIM(UNNEST(string_split(DesignBy, ','))) AS person_name FROM ipdb_machines WHERE DesignBy <> ''
  UNION ALL
  SELECT IpdbId, 'Art', TRIM(UNNEST(string_split(ArtBy, ','))) FROM ipdb_machines WHERE ArtBy <> ''
  UNION ALL
  SELECT IpdbId, 'Dots/Animation', TRIM(UNNEST(string_split(DotsAnimationBy, ','))) FROM ipdb_machines WHERE DotsAnimationBy <> ''
  UNION ALL
  SELECT IpdbId, 'Mechanics', TRIM(UNNEST(string_split(MechanicsBy, ','))) FROM ipdb_machines WHERE MechanicsBy <> ''
  UNION ALL
  SELECT IpdbId, 'Music', TRIM(UNNEST(string_split(MusicBy, ','))) FROM ipdb_machines WHERE MusicBy <> ''
  UNION ALL
  SELECT IpdbId, 'Sound', TRIM(UNNEST(string_split(SoundBy, ','))) FROM ipdb_machines WHERE SoundBy <> ''
  UNION ALL
  SELECT IpdbId, 'Software', TRIM(UNNEST(string_split(SoftwareBy, ','))) FROM ipdb_machines WHERE SoftwareBy <> ''
)
SELECT r.IpdbId, r.role, r.person_name, pl.slug AS person_slug
FROM raw r
LEFT JOIN _person_lookup pl ON LOWER(r.person_name) = pl.lookup_name
WHERE LOWER(r.person_name) NOT IN (
  '(undisclosed)', 'undisclosed', 'unknown', 'missing', 'null', 'undefined',
  'n/a', 'none', 'tbd', 'tba', '?', ''
)
  AND r.person_name NOT ILIKE '%(undisclosed)%'
  AND r.person_name NOT ILIKE '%unknown%';
