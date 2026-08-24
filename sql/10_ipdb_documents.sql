-- IPDB's files, classified.
--
-- Read by nothing, which is the expected state: these views are how the web
-- cache's `source = 'ipdb_pattern'` classifications are produced. Change a
-- detection pattern, rebuild, and the diff is what the change did.
--
-- Carrying a rebuild into the cache must layer UNDER what is there. The cache
-- records who judged a class, and a pattern's guess must not displace a
-- person's.
--
-- The mechanical half is upstream in `06_staging.sql` and the judgement is
-- declared in `05_reference.sql`. What is left here is analysis: folding matches
-- back to one row per file, resolving how wide each source's subject is, and
-- projecting the two non-document kinds.
--
-- A source's identity is a surrogate, since nothing here has the bytes. The
-- default is the filename's basename; where a class carries a real identifier —
-- a patent number, a publication and issue — the projection recomputes on that,
-- because IPDB writes the same patent under different filenames on different
-- models and basenames therefore undercount.

-- Every ancestor of every class, so a match can be rolled up to its parents.
-- Recursive so it stays correct if the vocabulary deepens past its current two
-- levels.
CREATE OR REPLACE VIEW ipdb_ref.document_class_ancestors AS
WITH RECURSIVE up(document_class, ancestor_class) AS (
  SELECT document_class, parent_class
  FROM ipdb_ref.document_class_parent
  UNION
  SELECT u.document_class, p.parent_class
  FROM up AS u
  INNER JOIN ipdb_ref.document_class_parent AS p ON p.document_class = u.ancestor_class
)
SELECT * FROM up;

CREATE OR REPLACE VIEW ipdb.model_documents AS
WITH
-- The matches plus their inherited ancestors, tagged so one aggregation can
-- emit both the literal and the rolled-up list.
expanded AS (
  SELECT ipdb_id, file_url, ipdb_category, document_class, source_kind, false AS inherited
  FROM ipdb_stg.file_class_matches
  UNION ALL
  SELECT m.ipdb_id, m.file_url, m.ipdb_category, a.ancestor_class, m.source_kind, true
  FROM ipdb_stg.file_class_matches AS m
  INNER JOIN ipdb_ref.document_class_ancestors AS a USING (document_class)
),
cls AS (
  SELECT
    ipdb_id,
    file_url,
    ipdb_category,
    list_sort(list_distinct(list(document_class) FILTER (WHERE NOT inherited))) AS class_matches,
    list_sort(list_distinct(list(document_class))) AS class_matches_rolled_up,
    list_sort(list_distinct(list(source_kind) FILTER (WHERE NOT inherited))) AS source_kinds
  FROM expanded
  GROUP BY 1, 2, 3
),
shared AS (
  -- IPDB stores a platform document once per model that uses it, under that
  -- model's own /files/<id>/ directory, so the copies share only a basename.
  --
  -- Restricted to files IPDB names after their content. Its gallery numbers
  -- images positionally per model — `image-1.jpg`, `image-A15.jpg` — so a
  -- basename there says which slot the photograph occupies, not which document
  -- it is, and counting models per basename would find thousands of
  -- "sightings" of a file that was never one file. The same goes for a URL that
  -- ends in a path segment rather than a filename.
  --
  -- The manufacturer count is NULL unless every referencing model resolved,
  -- because `count(DISTINCT)` skips NULLs and would report a partial count as a
  -- whole one. That inverts the answer rather than blurring it: a model IPDB
  -- names no maker for resolves to nothing, and dropping it silently narrows the
  -- scope to the makers that happened to resolve.
  SELECT
    file_basename,
    count(DISTINCT ipdb_id) AS models_referencing,
    CASE WHEN count(DISTINCT ipdb_id) FILTER (WHERE model_manufacturer IS NULL) = 0
      THEN count(DISTINCT model_manufacturer) END AS manufacturers_referencing,
    count(DISTINCT model_mpu) AS mpus_referencing
  FROM ipdb_stg.files
  WHERE ipdb_category <> 'image'
    AND container IS NOT NULL
  GROUP BY 1
)
SELECT
  f.ipdb_id,
  f.model_name,
  f.ipdb_category,
  f.file_name,
  f.file_url,
  f.file_basename,
  f.container,
  f.model_manufacturer,
  f.model_mpu,

  -- What it is. A list, never a scalar: a manual containing schematics is both,
  -- and a good share of the docs matches more than one class.
  coalesce(c.class_matches, []::VARCHAR[]) AS class_matches,
  -- The same list widened to declared parents, so "every manual" needs no
  -- hardcoded child list at the call site.
  coalesce(c.class_matches_rolled_up, []::VARCHAR[]) AS class_matches_rolled_up,
  coalesce(c.source_kinds, []::VARCHAR[]) AS source_kinds,

  -- Who published it, read off IPDB's `Maker_Year_Title_...` filename habit;
  -- NULL where the name opens with the year and carries no maker. The year is
  -- the machine's, not the document's — an anchor, never projected.
  --
  -- The maker is capped at three letter-initial words: in names with no
  -- machine year, the first `_YYYY_` is the document's own date near the end,
  -- and an unbounded prefix would read the whole title as the publisher.
  -- Names whose maker can't be read positionally answer NULL; recovering them
  -- needs a manufacturer vocabulary, not a wider pattern.
  nullif(
    regexp_extract(
      f.file_basename,
      '^([A-Za-z][A-Za-z0-9]*(?:_[A-Za-z][A-Za-z0-9]*){0,2})_(?:18|19|20)[0-9]{2}_',
      1
    ),
    ''
  ) AS publisher_prefix,

  -- NULL where the basename is not an identity, rather than a scope of one:
  -- the gallery images excluded above have no answer here, they are not lone
  -- copies.
  s.models_referencing,
  s.manufacturers_referencing,
  s.mpus_referencing,
  ipdb_ref.document_subject_scope(s.models_referencing, s.manufacturers_referencing) AS subject_scope
FROM ipdb_stg.files AS f
LEFT JOIN cls AS c USING (ipdb_id, file_url, ipdb_category)
LEFT JOIN shared AS s USING (file_basename);
COMMENT ON VIEW ipdb.model_documents IS
  'IPDB file listings with filename-pattern classifications and inferred subject scope; grain is model, URL and IPDB category.';

-- A patent is addressed by jurisdiction and number, so that pair is a stronger
-- identity than the filename and the subject scope is recomputed on it.
CREATE OR REPLACE VIEW ipdb.patents AS
WITH parsed AS (
  SELECT
    f.ipdb_id,
    f.model_name,
    f.file_name,
    f.file_url,
    f.container,
    f.model_manufacturer,
    f.model_mpu,
    -- US is the fallback, not a guess: every remaining form 
    -- ("Zipper Flippers Patent 3,404,888", "Patent Listing #1,925,018") carries
    -- a US number, and the two non-US issuers name themselves.
    CASE
      WHEN regexp_matches(lower(f.file_name), 'u\.?\s?k\.?\s+patent|\bgb[0-9]') THEN 'GB'
      WHEN regexp_matches(lower(f.file_name), 'spanish patent|patente de invencion') THEN 'ES'
      ELSE 'US'
    END AS jurisdiction,
    -- The "No." is optional and sits between the two words it would otherwise
    -- be safe to require adjacent: "Patent D96,384" and "Patent No. D89,228"
    -- are the same form.
    regexp_matches(lower(f.file_name), 'design patent|patent (no\.?\s*)?d[0-9]|\bd[0-9]{5,6}\b') AS is_design_patent,
    -- The D prefix is part of the number, not a note about it: design D087759
    -- and utility 87759 are different grants.
    nullif(
      CASE WHEN is_design_patent THEN 'D' ELSE '' END
        || replace(regexp_extract(f.file_name, '([0-9][0-9,\.]{4,})', 1), ',', ''),
      ''
    ) AS patent_number,
    -- IPDB writes the office's own title in brackets: "[VERTICALLY ADJUSTABLE
    -- BUMPER FOR BALL ROLLING GAMES]".
    nullif(regexp_extract(f.file_name, '\[([^\]]+)\]', 1), '') AS patent_title
  FROM ipdb_stg.files AS f
  -- A semi-join: a class may carry several pattern rows, and joining would emit
  -- the file once per rule that fired.
  WHERE EXISTS (
    SELECT 1 FROM ipdb_stg.file_class_matches AS m
    WHERE m.ipdb_id = f.ipdb_id
      AND m.file_url = f.file_url
      AND m.ipdb_category = f.ipdb_category
      AND m.source_kind = 'patent'
  )
),
scope AS (
  -- Keyed on the pair, matching the identity above: numbering restarts in each
  -- office, so US 1,234,567 and GB 1,234,567 are unrelated grants.
  SELECT
    jurisdiction,
    patent_number,
    count(DISTINCT ipdb_id) AS models_referencing,
    count(DISTINCT model_manufacturer) AS manufacturers_referencing,
    count(DISTINCT model_mpu) AS mpus_referencing
  FROM parsed
  WHERE patent_number IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  p.*,
  s.models_referencing,
  s.manufacturers_referencing,
  s.mpus_referencing,
  ipdb_ref.document_subject_scope(s.models_referencing, s.manufacturers_referencing) AS subject_scope
FROM parsed AS p
LEFT JOIN scope AS s USING (jurisdiction, patent_number);
COMMENT ON VIEW ipdb.patents IS
  'IPDB patent file listings with jurisdiction and number parsed from filenames and subject scope aggregated by that pair.';

-- Trade-press coverage, addressed by publication, issue and page. One article
-- routinely covers several models, which inverts the pattern documents follow.
CREATE OR REPLACE VIEW ipdb.trade_articles AS
WITH parsed AS (
  SELECT
    f.ipdb_id,
    f.model_name,
    f.file_name,
    f.file_url,
    f.container,
    f.model_manufacturer,
    f.model_mpu,
    -- Best effort: the publication sits before "Article"/"Magazine Article" in
    -- most names, inside the parentheses in a few ("Expo 1991 Factory Tour
    -- (Coin Slot, Spring 1992, pages 21-22)"), and is absent from the rest.
    nullif(trim(regexp_extract(f.file_name, '^(.*?)\s+(?:Magazine\s+)?(?:Article|Ad\b)', 1)), '') AS publication,
    nullif(trim(regexp_extract(f.file_name, '(?:pages?\s+)([0-9][0-9,\s\-]*)', 1)), '') AS pages,
    nullif(regexp_extract(
      f.file_name,
      '((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[.,\-]?\s*''?[0-9]{2,4})', 1), '') AS issue_date
  FROM ipdb_stg.files AS f
  -- A semi-join: a class may carry several pattern rows, and joining would emit
  -- the file once per rule that fired.
  WHERE EXISTS (
    SELECT 1 FROM ipdb_stg.file_class_matches AS m
    WHERE m.ipdb_id = f.ipdb_id
      AND m.file_url = f.file_url
      AND m.ipdb_category = f.ipdb_category
      AND m.source_kind = 'trade_article'
  )
),
scope AS (
  -- The display name is the identity here: IPDB files a copy under each machine
  -- the piece covers, carrying the same name each time.
  SELECT
    file_name,
    count(DISTINCT ipdb_id) AS models_referencing,
    count(DISTINCT model_manufacturer) AS manufacturers_referencing,
    count(DISTINCT model_mpu) AS mpus_referencing
  FROM parsed
  GROUP BY 1
)
SELECT
  a.*,
  s.models_referencing,
  s.manufacturers_referencing,
  s.mpus_referencing,
  ipdb_ref.document_subject_scope(s.models_referencing, s.manufacturers_referencing) AS subject_scope
FROM parsed AS a
LEFT JOIN scope AS s USING (file_name);
COMMENT ON VIEW ipdb.trade_articles IS
  'IPDB trade-press file listings with publication, issue and pages parsed best-effort from filenames; subject scope is grouped by file name.';
