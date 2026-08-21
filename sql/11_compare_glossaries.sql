-- Glossary comparison: which terms each pinball glossary defines.
--
-- The one place this build compares two external sources to each other, and the
-- exception is deliberate: no single glossary can answer which glossaries define
-- a term. Comparing a source against the CATALOG still belongs in flippatch.

CREATE OR REPLACE VIEW glossary.compared AS
WITH
  -- Deduplicate primer entries per slug (e.g. Add-a-ball has award + game type)
  primer_deduped AS (
    SELECT slug, name, definition
    FROM glossary.pinball_primer
    QUALIFY row_number() OVER (PARTITION BY slug ORDER BY slug) = 1
  ),
  all_terms AS (
    SELECT slug FROM glossary.ipdb
    UNION
    SELECT slug FROM glossary.kineticist
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
LEFT JOIN glossary.ipdb     AS i ON i.slug = a.slug
LEFT JOIN glossary.kineticist AS k ON k.slug = a.slug
LEFT JOIN primer_deduped    AS p ON p.slug = a.slug
ORDER BY a.slug;

