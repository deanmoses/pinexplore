-- Soft warnings (data quality, not hard build-stopping violations).
-- Creates the _warnings table. Later layers also insert into _warnings. 
-- All warnings are printed at the end of the build process.

DROP TABLE IF EXISTS _warnings;
CREATE TEMP TABLE _warnings (check_name VARCHAR, cnt BIGINT);

------------------------------------------------------------
-- Data quality warnings
------------------------------------------------------------

INSERT INTO _warnings
SELECT 'pindata_opdb_id_not_in_dump', count(*)
FROM models AS m
WHERE m.opdb_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM opdb_machines AS o WHERE o.opdb_id = m.opdb_id);

INSERT INTO _warnings
SELECT 'pindata_ipdb_id_not_in_dump', count(*)
FROM models AS m
WHERE m.ipdb_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM ipdb_machines AS i WHERE i.IpdbId = m.ipdb_id);

INSERT INTO _warnings
SELECT 'models_missing_corporate_entity', count(*)
FROM models m
WHERE m.corporate_entity_slug IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM ipdb_machines i
    WHERE m.ipdb_id = i.IpdbId
      AND i.ManufacturerId IS NOT NULL AND i.ManufacturerId != 0 AND i.ManufacturerId != 328
  )
  AND NOT EXISTS (
    SELECT 1 FROM opdb_machines om
    WHERE m.opdb_id = om.opdb_id
      AND om.manufacturer.name IS NOT NULL
  );

INSERT INTO _warnings
SELECT 'titles_missing_opdb_group', count(*)
FROM titles WHERE opdb_group_id IS NULL;

INSERT INTO _warnings
SELECT 'conversion_without_source', count(*)
FROM models WHERE is_conversion AND converted_from IS NULL;

INSERT INTO _warnings
SELECT 'ambiguous_theme_alias', count(*)
FROM (
  SELECT raw_theme
  FROM theme_aliases
  GROUP BY raw_theme HAVING count(DISTINCT canonical_theme) > 1
);

INSERT INTO _warnings
SELECT 'themes_without_machines', count(*)
FROM themes th
WHERE th.slug NOT IN (
    SELECT unnest(m.theme_slugs) FROM models m WHERE m.theme_slugs IS NOT NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM ipdb_themes it WHERE it.theme = th.name
  );

-- Theme hierarchy depth: only warn if deeper than 5
INSERT INTO _warnings
SELECT 'theme_max_parent_depth', md FROM (
  SELECT max(depth) AS md FROM (
    WITH RECURSIVE walk AS (
      SELECT theme, parent, 1 AS depth FROM theme_parents
      UNION ALL
      SELECT w.theme, p.parent, w.depth + 1
      FROM walk w JOIN theme_parents p ON p.theme = w.parent
      WHERE w.depth < 20
    )
    SELECT max(depth) AS depth FROM walk GROUP BY theme
  )
) WHERE md > 5;

-- IPDB-parsed city not found in pindata location files
-- Details: SELECT * FROM ipdb_corporate_entities WHERE headquarters_city IS NOT NULL
--   AND NOT EXISTS (SELECT 1 FROM ref_location_city_aliases WHERE alias = headquarters_city)
INSERT INTO _warnings
SELECT 'ipdb_ce_unresolved_city', count(*)
FROM ipdb_corporate_entities
WHERE headquarters_city IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ref_location_city_aliases WHERE alias = headquarters_city
  );

-- IPDB-parsed country not found in pindata location files
-- Details: SELECT * FROM ipdb_corporate_entities WHERE headquarters_country IS NOT NULL
--   AND NOT EXISTS (SELECT 1 FROM ref_location_country_aliases WHERE alias = headquarters_country)
INSERT INTO _warnings
SELECT 'ipdb_ce_unresolved_country', count(*)
FROM ipdb_corporate_entities
WHERE headquarters_country IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ref_location_country_aliases WHERE alias = headquarters_country
  );

-- Gameplay feature hierarchy depth: only warn if deeper than 5
INSERT INTO _warnings
SELECT 'gameplay_feature_max_parent_depth', md FROM (
  SELECT max(depth) AS md FROM (
    WITH RECURSIVE walk AS (
      SELECT feature, parent, 1 AS depth FROM gameplay_feature_parents
      UNION ALL
      SELECT w.feature, p.parent, w.depth + 1
      FROM walk w JOIN gameplay_feature_parents p ON p.feature = w.parent
      WHERE w.depth < 20
    )
    SELECT max(depth) AS depth FROM walk GROUP BY feature
  )
) WHERE md > 5;


-- The IPDB header line no longer matches the grammar parsed in
-- `ipdb_machine_additional_details`. Warns rather than aborting: the shape of
-- this string is xantari's to change, and a reformat upstream should not stop a
-- build. It does mean the parsed date columns are silently empty for those rows.
-- Details: SELECT IpdbId, AdditionalDetails FROM ipdb_machines i
--   WHERE NOT EXISTS (SELECT 1 FROM ipdb_machine_additional_details ad
--     WHERE ad.IpdbId = i.IpdbId AND ad.additional_details_ipd_no IS NOT NULL)
INSERT INTO _warnings
SELECT 'ipdb_additional_details_unparsed', count(*)
FROM ipdb_machines AS i
JOIN ipdb_machine_additional_details AS ad ON ad.IpdbId = i.IpdbId
WHERE i.AdditionalDetails IS NOT NULL
  AND ad.additional_details_ipd_no IS NULL;

-- A date segment matched the grammar but no month name recognised it, so the
-- year/month/day columns are NULL while the string is present. Signals a date
-- format IPDB has started using that try_strptime does not cover.
INSERT INTO _warnings
SELECT 'ipdb_additional_details_date_unrecognised', count(*)
FROM ipdb_machine_additional_details
WHERE additional_details_date_string IS NOT NULL
  AND additional_details_date_year IS NULL;
