-- The IPDB file trove, classified.
--
-- The mechanical half is upstream in `04_staging.sql` (`ipdb_files`,
-- `ipdb_file_class_matches`) and the judgement is declared in `01_reference.sql`
-- (`ref_document_class` and friends). What is left here is analysis: folding the
-- matches back to one row per file, resolving how wide each source's subject is,
-- projecting the two non-document kinds, and checking the declared hierarchy
-- against what the patterns actually did.
--
-- A source's identity is a surrogate, since nothing here has the bytes. The
-- default is the filename's basename; where a class carries a real identifier —
-- a patent number, a publication and issue — the projection recomputes on that
-- instead, because IPDB writes the same patent under different filenames on
-- different machines and basenames therefore undercount.
--
-- Views:
--   ref_document_class_ancestors     — transitive closure of the class hierarchy
--   ipdb_documents                   — one row per file, classified and scoped
--   ipdb_patents                     — patents, keyed by jurisdiction and number
--   ipdb_trade_articles              — trade-press coverage, keyed by publication
--   ipdb_document_class_subsumption  — declared hierarchy vs. what the patterns did

-- Every ancestor of every class, so a match can be rolled up to its parents.
-- Recursive so it stays correct if the vocabulary deepens past its current two
-- levels.
CREATE OR REPLACE VIEW ref_document_class_ancestors AS
WITH RECURSIVE up(document_class, ancestor_class) AS (
  SELECT document_class, parent_class
  FROM ref_document_class_parent
  UNION
  SELECT u.document_class, p.parent_class
  FROM up AS u
  INNER JOIN ref_document_class_parent AS p ON p.document_class = u.ancestor_class
)
SELECT * FROM up;

CREATE OR REPLACE VIEW ipdb_documents AS
WITH
-- The matches plus their inherited ancestors, tagged so one aggregation can
-- emit both the literal and the rolled-up list.
expanded AS (
  SELECT ipdb_id, file_url, ipdb_category, document_class, source_kind, false AS inherited
  FROM ipdb_file_class_matches
  UNION ALL
  SELECT m.ipdb_id, m.file_url, m.ipdb_category, a.ancestor_class, m.source_kind, true
  FROM ipdb_file_class_matches AS m
  INNER JOIN ref_document_class_ancestors AS a USING (document_class)
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
  -- IPDB stores a platform document once per machine that uses it, under that
  -- machine's own /files/<id>/ directory, so the copies share only a basename.
  --
  -- Restricted to files IPDB names after their content. Its gallery numbers
  -- images positionally per machine — `image-1.jpg`, `image-A15.jpg` — so a
  -- basename there says which slot the photograph occupies, not which document
  -- it is, and counting machines per basename would find thousands of
  -- "sightings" of a file that was never one file. The same goes for a URL that
  -- ends in a path segment rather than a filename.
  --
  -- Titles are what separates the two shapes the plan doc names. A document
  -- under many titles is about a platform; one under many machines of a single
  -- title is about that title's editions. Systems cannot tell them apart —
  -- editions of one game share a system by definition — so the count is carried
  -- for corroboration and the conclusion is left to the reader.
  --
  -- Both catalog counts are NULL unless every referencing machine resolved,
  -- because `count(DISTINCT)` skips NULLs and would report a partial count as a
  -- whole one. That inverts the answer rather than blurring it: one resolved
  -- system out of fourteen reads as a single-platform document.
  SELECT
    file_basename,
    count(DISTINCT ipdb_id) AS machines_referencing,
    count(DISTINCT machine_manufacturer) AS manufacturers_referencing,
    count(DISTINCT machine_mpu) AS mpus_referencing,
    CASE WHEN count(DISTINCT ipdb_id) FILTER (WHERE machine_title_slug IS NULL) = 0
      THEN count(DISTINCT machine_title_slug) END AS titles_referencing,
    CASE WHEN count(DISTINCT ipdb_id) FILTER (WHERE machine_system_slug IS NULL) = 0
      THEN count(DISTINCT machine_system_slug) END AS systems_referencing
  FROM ipdb_files
  WHERE ipdb_category <> 'image'
    AND container IS NOT NULL
  GROUP BY 1
)
SELECT
  f.ipdb_id,
  f.machine_name,
  f.ipdb_category,
  f.file_name,
  f.file_url,
  f.file_basename,
  f.container,
  f.machine_manufacturer,
  f.machine_mpu,

  -- What it is. A list, never a scalar: a manual containing schematics is both,
  -- and a good share of the documentation trove matches more than one class.
  coalesce(c.class_matches, []::VARCHAR[]) AS class_matches,
  -- The same list widened to declared parents, so "every manual" needs no
  -- hardcoded child list at the call site.
  coalesce(c.class_matches_rolled_up, []::VARCHAR[]) AS class_matches_rolled_up,
  coalesce(c.source_kinds, []::VARCHAR[]) AS source_kinds,

  -- Who published it, read off IPDB's `Maker_Year_Title_...` filename habit.
  -- NULL where the name opens with the year instead
  -- ("2020_Guns_N_Roses_Not_In_This_Lifetime_Standard_..."), which carries no
  -- maker at all.
  --
  -- The year in that convention is the machine's, not the document's, so it is
  -- consumed as an anchor here and not projected. A document's own date, where
  -- IPDB recorded one, is in `file_name`.
  nullif(regexp_extract(f.file_basename, '^(.*?)_(?:18|19|20)[0-9]{2}_', 1), '') AS publisher_prefix,

  -- NULL where the basename is not an identity, rather than a scope of one:
  -- the gallery images excluded above have no answer here, they are not lone
  -- copies.
  s.machines_referencing,
  s.manufacturers_referencing,
  s.mpus_referencing,
  s.titles_referencing,
  s.systems_referencing,
  document_subject_scope(s.machines_referencing, s.manufacturers_referencing) AS subject_scope
FROM ipdb_files AS f
LEFT JOIN cls AS c USING (ipdb_id, file_url, ipdb_category)
LEFT JOIN shared AS s USING (file_basename);

-- A patent is addressed by jurisdiction and number, so that pair is a stronger
-- identity than the filename and the subject scope is recomputed on it.
CREATE OR REPLACE VIEW ipdb_patents AS
WITH parsed AS (
  SELECT
    f.ipdb_id,
    f.machine_name,
    f.file_name,
    f.file_url,
    f.container,
    f.machine_manufacturer,
    f.machine_mpu,
    -- US is the fallback, not a guess: every remaining form in the trove
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
  FROM ipdb_files AS f
  -- A semi-join: a class may carry several pattern rows, and joining would emit
  -- the file once per rule that fired.
  WHERE EXISTS (
    SELECT 1 FROM ipdb_file_class_matches AS m
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
    count(DISTINCT ipdb_id) AS machines_referencing,
    count(DISTINCT machine_manufacturer) AS manufacturers_referencing,
    count(DISTINCT machine_mpu) AS mpus_referencing
  FROM parsed
  WHERE patent_number IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  p.*,
  s.machines_referencing,
  s.manufacturers_referencing,
  s.mpus_referencing,
  document_subject_scope(s.machines_referencing, s.manufacturers_referencing) AS subject_scope
FROM parsed AS p
LEFT JOIN scope AS s USING (jurisdiction, patent_number);

-- Trade-press coverage, addressed by publication, issue and page. One article
-- routinely covers several machines, which inverts the pattern documents follow.
CREATE OR REPLACE VIEW ipdb_trade_articles AS
WITH parsed AS (
  SELECT
    f.ipdb_id,
    f.machine_name,
    f.file_name,
    f.file_url,
    f.container,
    f.machine_manufacturer,
    f.machine_mpu,
    -- Best effort: the publication sits before "Article"/"Magazine Article" in
    -- most names, inside the parentheses in a few ("Expo 1991 Factory Tour
    -- (Coin Slot, Spring 1992, pages 21-22)"), and is absent from the rest.
    nullif(trim(regexp_extract(f.file_name, '^(.*?)\s+(?:Magazine\s+)?(?:Article|Ad\b)', 1)), '') AS publication,
    nullif(trim(regexp_extract(f.file_name, '(?:pages?\s+)([0-9][0-9,\s\-]*)', 1)), '') AS pages,
    nullif(regexp_extract(
      f.file_name,
      '((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[.,\-]?\s*''?[0-9]{2,4})', 1), '') AS issue_date
  FROM ipdb_files AS f
  -- A semi-join: a class may carry several pattern rows, and joining would emit
  -- the file once per rule that fired.
  WHERE EXISTS (
    SELECT 1 FROM ipdb_file_class_matches AS m
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
    count(DISTINCT ipdb_id) AS machines_referencing,
    count(DISTINCT machine_manufacturer) AS manufacturers_referencing,
    count(DISTINCT machine_mpu) AS mpus_referencing
  FROM parsed
  GROUP BY 1
)
SELECT
  a.*,
  s.machines_referencing,
  s.manufacturers_referencing,
  s.mpus_referencing,
  document_subject_scope(s.machines_referencing, s.manufacturers_referencing) AS subject_scope
FROM parsed AS a
LEFT JOIN scope AS s USING (file_name);

-- Declared parent edges against what the patterns independently did. The two
-- derivations are separate: the hierarchy is authored, the patterns know
-- nothing of it, so agreement is evidence the edge is real.
--
-- Only checkable where the child's name lexically contains the parent's —
-- "Operations Manual" contains "Manual", so both fire and the edge is
-- corroborated. A wiring diagram is a schematic without saying so, so a low
-- share there is silence rather than contradiction, and a grouping node that
-- has no patterns at all gets a NULL rather than a misleading zero.
CREATE OR REPLACE VIEW ipdb_document_class_subsumption AS
WITH per_file AS (
  SELECT
    ipdb_id,
    file_url,
    ipdb_category,
    list_distinct(list(document_class)) AS classes
  FROM ipdb_file_class_matches
  GROUP BY 1, 2, 3
),
edges AS (
  SELECT
    e.document_class,
    e.parent_class,
    EXISTS (
      SELECT 1 FROM ref_document_class_pattern AS pp
      WHERE pp.document_class = e.parent_class
    ) AS checkable
  FROM ref_document_class_parent AS e
)
SELECT
  e.document_class,
  e.parent_class,
  e.checkable,
  count(*) AS child_matches,
  count(*) FILTER (WHERE list_contains(p.classes, e.parent_class)) AS also_matched_parent,
  CASE WHEN e.checkable THEN round(
    100.0 * count(*) FILTER (WHERE list_contains(p.classes, e.parent_class)) / count(*),
    1
  ) END AS pct
FROM edges AS e
INNER JOIN per_file AS p ON list_contains(p.classes, e.document_class)
GROUP BY 1, 2, 3
ORDER BY checkable DESC, pct DESC, child_matches DESC;
