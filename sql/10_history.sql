-- Industry history: decade-level trends.
--
-- Views:
--   decade_summary — title count, manufacturer count (parent brands, not
--                    corporate entities), and top 3 manufacturers per decade

CREATE OR REPLACE VIEW decade_summary AS
WITH decade_models AS (
  SELECT
    (m.year // 10) * 10 AS decade,
    ce.manufacturer_slug,
    m.title_slug
  FROM models m
  JOIN corporate_entities ce ON m.corporate_entity_slug = ce.slug
  WHERE m.year IS NOT NULL
),
totals AS (
  SELECT
    decade,
    count(DISTINCT manufacturer_slug) AS manufacturer_count,
    count(DISTINCT title_slug) AS title_count
  FROM decade_models
  GROUP BY decade
),
per_manufacturer AS (
  SELECT
    decade,
    manufacturer_slug,
    count(DISTINCT title_slug) AS title_count
  FROM decade_models
  GROUP BY decade, manufacturer_slug
),
ranked AS (
  SELECT
    pm.decade,
    man.name AS manufacturer_name,
    pm.title_count,
    row_number() OVER (PARTITION BY pm.decade ORDER BY pm.title_count DESC, man.name) AS rank
  FROM per_manufacturer pm
  JOIN manufacturers man ON pm.manufacturer_slug = man.slug
),
top3 AS (
  SELECT
    decade,
    string_agg(manufacturer_name || ' (' || title_count || ')', ', ' ORDER BY rank) AS top_manufacturers
  FROM ranked
  WHERE rank <= 3
  GROUP BY decade
)
SELECT
  t.decade,
  t.manufacturer_count,
  t.title_count,
  top3.top_manufacturers
FROM totals t
JOIN top3 ON t.decade = top3.decade
ORDER BY t.decade;
