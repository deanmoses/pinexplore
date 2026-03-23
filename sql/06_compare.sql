-- Cross-source comparison: do sources agree?

------------------------------------------------------------
-- Models vs OPDB (by opdb_id)
------------------------------------------------------------

CREATE OR REPLACE VIEW compare_models_opdb AS
SELECT
  m.slug,
  m.name AS pinbase_name,
  o.name AS opdb_name,
  m.name IS DISTINCT FROM o.name AS name_differs,
  m.corporate_entity_slug AS pinbase_corporate_entity,
  ce.manufacturer_slug AS pinbase_manufacturer,
  o.manufacturer_name AS opdb_manufacturer,
  m.year AS pinbase_year,
  year(o.manufacture_date) AS opdb_year,
  m.year IS DISTINCT FROM year(o.manufacture_date) AS year_differs,
  m.technology_generation_slug AS pinbase_tech_gen,
  o.technology_generation_slug AS opdb_tech_gen,
  m.technology_generation_slug IS DISTINCT FROM o.technology_generation_slug AS tech_gen_differs,
  m.display_type_slug AS pinbase_display,
  o.display_type_slug AS opdb_display,
  m.display_type_slug IS DISTINCT FROM o.display_type_slug AS display_differs,
  m.player_count AS pinbase_players,
  o.player_count AS opdb_players,
  m.opdb_id
FROM models AS m
INNER JOIN opdb_machines_staged AS o ON m.opdb_id = o.opdb_id
LEFT JOIN corporate_entities AS ce ON ce.slug = m.corporate_entity_slug;

------------------------------------------------------------
-- Models vs IPDB (by ipdb_id)
------------------------------------------------------------

CREATE OR REPLACE VIEW compare_models_ipdb AS
SELECT
  m.slug,
  m.name AS pinbase_name,
  i.Title AS ipdb_name,
  m.name IS DISTINCT FROM i.Title AS name_differs,
  m.corporate_entity_slug AS pinbase_corporate_entity,
  ce.manufacturer_slug AS pinbase_manufacturer,
  i.ManufacturerShortName AS ipdb_manufacturer,
  m.year AS pinbase_year,
  EXTRACT(YEAR FROM TRY_CAST(i.DateOfManufacture AS DATE))::INTEGER AS ipdb_year,
  m.year IS DISTINCT FROM EXTRACT(YEAR FROM TRY_CAST(i.DateOfManufacture AS DATE))::INTEGER AS year_differs,
  m.technology_generation_slug AS pinbase_tech_gen,
  i.technology_generation_slug AS ipdb_tech_gen,
  m.player_count AS pinbase_players,
  i.Players AS ipdb_players,
  i.AverageFunRating AS ipdb_rating,
  i.ProductionNumber AS ipdb_production,
  m.ipdb_id
FROM models AS m
INNER JOIN ipdb_machines_staged AS i ON m.ipdb_id = i.IpdbId
LEFT JOIN corporate_entities AS ce ON ce.slug = m.corporate_entity_slug;

------------------------------------------------------------
-- Titles vs OPDB groups (by opdb_group_id)
------------------------------------------------------------

CREATE OR REPLACE VIEW compare_titles_opdb AS
SELECT
  t.slug,
  t.name AS pinbase_name,
  g.name AS opdb_name,
  t.name <> g.name AS name_differs,
  t.opdb_group_id
FROM titles AS t
INNER JOIN opdb_groups AS g ON t.opdb_group_id = g.opdb_id;

------------------------------------------------------------
-- IPDB credits missing from Pinbase
------------------------------------------------------------

CREATE OR REPLACE VIEW compare_credits_ipdb AS
SELECT
  m.slug AS model_slug,
  ic.role,
  ic.person_slug,
  ic.person_name AS ipdb_person_name,
  ic.IpdbId
FROM _ipdb_credits ic
JOIN models m ON m.ipdb_id = ic.IpdbId
WHERE ic.person_slug IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM pinbase_credits pc
    WHERE pc.model_slug = m.slug
      AND pc.person_slug = ic.person_slug
      AND pc.role = ic.role
  );

------------------------------------------------------------
-- IPDB themes resolved against pinbase vocabulary
------------------------------------------------------------

-- Direct IPDB themes plus all implied parents from the pinbase hierarchy.
CREATE OR REPLACE VIEW ipdb_themes_resolved AS
WITH RECURSIVE
  -- Start with direct themes that exist in pinbase vocabulary
  direct AS (
    SELECT DISTINCT it.IpdbId, it.theme
    FROM ipdb_themes it
    WHERE it.theme IN (SELECT name FROM themes)
  ),
  -- Walk up the parent graph
  rollup AS (
    SELECT IpdbId, theme, theme AS source, 0 AS depth
    FROM direct
    UNION
    SELECT r.IpdbId, p.parent AS theme, r.theme AS source, r.depth + 1
    FROM rollup r
    JOIN theme_parents p ON p.theme = r.theme
    WHERE r.depth < 20  -- guard against cycles
  )
SELECT DISTINCT IpdbId, theme
FROM rollup;

------------------------------------------------------------
-- Model theme assignments: pinbase vs IPDB
------------------------------------------------------------

-- Per-model comparison of direct theme assignments. Shows themes that are
-- in pinbase only, IPDB only, or both. Compares direct assignments only,
-- not rolled-up parents (ipdb_themes_resolved handles rollup separately).
CREATE OR REPLACE VIEW compare_model_themes_ipdb AS
WITH
  pinbase_themes AS (
    SELECT DISTINCT m.slug AS model_slug, unnest(m.theme_slugs) AS theme_slug
    FROM models m
    WHERE m.theme_slugs IS NOT NULL AND len(m.theme_slugs) > 0
  ),
  ipdb_model_themes AS (
    SELECT DISTINCT m.slug AS model_slug, th.slug AS theme_slug
    FROM models m
    JOIN ipdb_themes it ON m.ipdb_id = it.IpdbId
    JOIN themes th ON it.theme = th.name
    WHERE it.theme IN (SELECT name FROM themes)
  )
SELECT
  COALESCE(p.model_slug, i.model_slug) AS model_slug,
  COALESCE(p.theme_slug, i.theme_slug) AS theme_slug,
  (p.theme_slug IS NOT NULL) AS in_pinbase,
  (i.theme_slug IS NOT NULL) AS in_ipdb
FROM pinbase_themes p
FULL OUTER JOIN ipdb_model_themes i
  ON p.model_slug = i.model_slug AND p.theme_slug = i.theme_slug;

------------------------------------------------------------
-- Cabinet type: pinbase vs OPDB
------------------------------------------------------------

-- Models where OPDB assigns a cabinet type via its features array but
-- pinbase disagrees, or vice versa. Uses aliases from the cabinets
-- entity to match OPDB feature strings to pinbase cabinet slugs.
CREATE OR REPLACE VIEW compare_cabinets_opdb AS
SELECT
  m.slug AS model_slug,
  m.cabinet_slug AS pinbase_cabinet,
  m.opdb_id,
  rfc.cabinet_slug AS opdb_cabinet
FROM models AS m
INNER JOIN opdb_machines AS o ON m.opdb_id = o.opdb_id
INNER JOIN (
  SELECT om2.opdb_id, rfc2.cabinet_slug
  FROM opdb_machines AS om2, unnest(om2.features) AS t(f)
  INNER JOIN ref_feature_cabinet AS rfc2 ON lower(f) = rfc2.feature
) AS rfc ON o.opdb_id = rfc.opdb_id
WHERE COALESCE(m.cabinet_slug, '') != rfc.cabinet_slug;

------------------------------------------------------------
-- Conversion status: pinbase vs OPDB
------------------------------------------------------------

-- Models where OPDB marks as 'Conversion kit' or 'Converted game' but
-- pinbase does not have is_conversion=true, or vice versa.
CREATE OR REPLACE VIEW compare_conversions_opdb AS
SELECT
  m.slug AS model_slug,
  m.is_conversion AS pinbase_is_conversion,
  m.converted_from AS pinbase_converted_from,
  m.opdb_id,
  list_contains(o.features, 'Conversion kit') AS opdb_conversion_kit,
  list_contains(o.features, 'Converted game') AS opdb_converted_game,
  (list_contains(o.features, 'Conversion kit')
    OR list_contains(o.features, 'Converted game')) AS opdb_is_conversion
FROM models AS m
INNER JOIN opdb_machines AS o ON m.opdb_id = o.opdb_id
WHERE COALESCE(m.is_conversion, false)
   IS DISTINCT FROM (list_contains(o.features, 'Conversion kit')
                      OR list_contains(o.features, 'Converted game'));

------------------------------------------------------------
-- Gameplay features: OPDB vs pinbase
------------------------------------------------------------

-- Models where OPDB assigns a gameplay feature but pinbase does not,
-- or vice versa. Currently one-directional because models do not yet
-- have a gameplay_feature_slugs field; rows here represent OPDB claims
-- that pinbase should eventually match.
CREATE OR REPLACE VIEW compare_gameplay_features_opdb AS
SELECT
  m.slug AS model_slug,
  m.opdb_id,
  rfg.gameplay_feature_slug,
  gf.name AS gameplay_feature_name,
  rfg.feature AS opdb_feature
FROM opdb_machines AS o, unnest(o.features) AS t(f)
INNER JOIN ref_feature_gameplay AS rfg ON lower(f) = rfg.feature
INNER JOIN models AS m ON o.opdb_id = m.opdb_id
LEFT JOIN gameplay_features AS gf ON rfg.gameplay_feature_slug = gf.slug;

------------------------------------------------------------
-- Reward types: OPDB vs pinbase
------------------------------------------------------------

-- Models where OPDB assigns a reward type but pinbase does not, or
-- vice versa. Currently one-directional because models do not yet have
-- a reward_type_slugs field; rows here represent OPDB claims that
-- pinbase should eventually match.
CREATE OR REPLACE VIEW compare_reward_types_opdb AS
SELECT
  m.slug AS model_slug,
  m.opdb_id,
  rfrt.reward_type_slug,
  rt.name AS reward_type_name,
  rfrt.feature AS opdb_feature
FROM opdb_machines AS o, unnest(o.features) AS t(f)
INNER JOIN ref_feature_reward_type AS rfrt ON lower(f) = rfrt.feature
INNER JOIN models AS m ON o.opdb_id = m.opdb_id
LEFT JOIN reward_types AS rt ON rfrt.reward_type_slug = rt.slug;

------------------------------------------------------------
-- Gameplay features: IPDB vs pinbase
------------------------------------------------------------

-- Distinct IPDB gameplay feature names that do not map to any pinbase
-- gameplay_feature (via name or alias). Each row is a feature term
-- extracted from NotableFeatures that pinbase has no vocabulary entry
-- for.
CREATE OR REPLACE VIEW compare_gameplay_features_ipdb AS
SELECT
  ipdb_feature,
  count(DISTINCT IpdbId) AS machine_count
FROM ipdb_gameplay_features
WHERE gameplay_feature_slug IS NULL
  AND ipdb_feature NOT IN (SELECT feature FROM ref_gameplay_features_dropped)
GROUP BY ipdb_feature
ORDER BY machine_count DESC;

------------------------------------------------------------
-- Reward types: IPDB vs pinbase
------------------------------------------------------------

-- Distinct IPDB reward type terms that do not map to any pinbase
-- reward_type (via name or alias). Currently expected to be empty
-- since all six reward types are defined.
CREATE OR REPLACE VIEW compare_reward_types_ipdb AS
SELECT
  ipdb_feature,
  count(DISTINCT IpdbId) AS machine_count
FROM ipdb_reward_types
WHERE reward_type_slug IS NULL
GROUP BY ipdb_feature
ORDER BY machine_count DESC;

------------------------------------------------------------
-- Glossary comparison across sources
------------------------------------------------------------

CREATE OR REPLACE VIEW compare_glossaries AS
WITH
  -- Deduplicate primer entries per slug (e.g. Add-a-ball has award + game type)
  primer_deduped AS (
    SELECT slug, name, definition
    FROM pinball_primer_glossary
    QUALIFY row_number() OVER (PARTITION BY slug ORDER BY slug) = 1
  ),
  all_terms AS (
    SELECT slug FROM ipdb_glossary
    UNION
    SELECT slug FROM kineticist_glossary
    UNION
    SELECT slug FROM primer_deduped
  )
SELECT
  a.slug,
  coalesce(i.name, k.name, p.name)  AS name,
  i.name  IS NOT NULL               AS in_ipdb,
  k.name  IS NOT NULL               AS in_kineticist,
  p.name  IS NOT NULL               AS in_primer,
  (i.name IS NOT NULL)::int
    + (k.name IS NOT NULL)::int
    + (p.name IS NOT NULL)::int      AS source_count,
  i.definition                       AS ipdb_definition,
  k.definition                       AS kineticist_definition,
  p.definition                       AS primer_definition,
FROM all_terms AS a
LEFT JOIN ipdb_glossary     AS i ON i.slug = a.slug
LEFT JOIN kineticist_glossary AS k ON k.slug = a.slug
LEFT JOIN primer_deduped    AS p ON p.slug = a.slug
ORDER BY a.slug;

------------------------------------------------------------
-- Warnings from compare views
------------------------------------------------------------

INSERT INTO _warnings SELECT 'compare_cabinets_opdb',           count(*) FROM compare_cabinets_opdb;
INSERT INTO _warnings SELECT 'compare_conversions_opdb',        count(*) FROM compare_conversions_opdb;
INSERT INTO _warnings SELECT 'compare_gameplay_features_opdb',  count(*) FROM compare_gameplay_features_opdb;
INSERT INTO _warnings SELECT 'compare_reward_types_opdb',       count(*) FROM compare_reward_types_opdb;
INSERT INTO _warnings SELECT 'compare_gameplay_features_ipdb',  count(*) FROM compare_gameplay_features_ipdb;
INSERT INTO _warnings SELECT 'compare_reward_types_ipdb',       count(*) FROM compare_reward_types_ipdb;
