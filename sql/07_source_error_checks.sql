-- Hard integrity checks over the source dumps. Aborts the build on any violation.
--
-- Structural checks need every layer built, so they run in
-- `80_structure_error_checks.sql`.
--
-- Scope is the source dumps and our own reference vocabularies -- nobody
-- upstream checks a dump for us, and the vocabularies reference each other by
-- name, so a rename dangles with nothing raising. Nothing here reads the
-- catalog; comparing against it is flippatch's job.

-- A real table, so rows written here survive the `error()` below that aborts the
-- build: reopen `explore.duckdb` and query it to see what failed.
CREATE OR REPLACE TABLE checks.violations (category VARCHAR, check_name VARCHAR, detail VARCHAR);

------------------------------------------------------------
-- Source dump integrity
------------------------------------------------------------

INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_record_missing_id', name
FROM opdb_raw.machines WHERE opdb_id IS NULL;

INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_record_missing_id', Title
FROM ipdb_stg.models_merged WHERE IpdbId IS NULL;

-- The parsed header line restates the IPD number and player count, which we hold
-- as typed columns anyway. That redundancy is the only way to tell a correct
-- parse from a misaligned one: if the capture groups slip a segment these
-- disagree, and the date beside them is wrong the same way with nothing else to
-- check it against. Guarded on a non-NULL ipd_no, so a row that never matched
-- the grammar warns instead of stopping the build.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_additional_details_parse_misaligned',
  'IpdbId ' || i.IpdbId || ': ' || i.AdditionalDetails
FROM ipdb_stg.models_merged AS i
JOIN ipdb_stg.model_additional_details AS ad ON ad.IpdbId = i.IpdbId
WHERE ad.additional_details_ipd_no IS NOT NULL
  AND (ad.additional_details_ipd_no != i.IpdbId
       OR ad.additional_details_players IS DISTINCT FROM i.Players);

------------------------------------------------------------
-- IPDB staging grain
------------------------------------------------------------

-- Every consumer joins `ipdb_stg.models` as a lookup, so a second row silently
-- multiplies whatever joined it. None of the lookups the view resolves is unique
-- by construction, so the invariant is asserted once here rather than at each.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_stg_models_not_unique',
  'IpdbId ' || IpdbId || ' -> ' || count(*) || ' rows (MPU: ' || coalesce(any_value(MPU), 'none') || ')'
FROM ipdb_stg.models
GROUP BY IpdbId
HAVING count(*) > 1;

-- The other half of that invariant: staging enriches every model and drops
-- none, so the two row counts are equal. Asserted because a WHERE added here is
-- invisible from every consumer -- gap analysis would simply report fewer
-- missing models and look healthier for it. Uniqueness above catches a row
-- multiplying; this catches one disappearing.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_stg_models_drops_rows',
  (SELECT count(*) FROM ipdb_stg.models_merged)::VARCHAR || ' models -> '
    || (SELECT count(*) FROM ipdb_stg.models)::VARCHAR || ' staged'
WHERE (SELECT count(*) FROM ipdb_stg.models) <> (SELECT count(*) FROM ipdb_stg.models_merged);

------------------------------------------------------------
-- Credit role vocabulary
------------------------------------------------------------

-- Expiry guard for archive-supplied roles. A new dump `%By` column fails the
-- build until `ipdb_ref.credit_role` claims it: add the row, or fill in the
-- `xantari_field` of the row that already exists. That same edit is what
-- withdraws the archive's permission to supply the role.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_credit_role_xantari_field_unclaimed',
  'ipdb_raw.xantari_model_snapshots.' || column_name
    || ' is a credit field no ipdb_ref.credit_role row claims'
FROM duckdb_columns()
WHERE database_name = current_database()
  AND schema_name = 'ipdb_raw' AND table_name = 'xantari_model_snapshots'
  AND column_name LIKE '%By'
  AND column_name NOT IN (
    SELECT xantari_field FROM ipdb_ref.credit_role WHERE xantari_field IS NOT NULL
  );

------------------------------------------------------------
-- Specialty vocabulary
------------------------------------------------------------

-- An INNER join publishes only ruled specialties, so an unruled page value would
-- otherwise vanish. This first surfaces after a fetch campaign, when the new
-- value enters the archive corpus.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_specialty_unmapped',
  '"' || ams.specialty || '" is stated by ' || count(*)
    || ' archive page(s) and no ipdb_ref.specialty row decodes it'
FROM ipdb_stg.archive_model_specialties AS ams
WHERE NOT EXISTS (
  SELECT 1 FROM ipdb_ref.specialty AS sp WHERE sp.ipdb_specialty = ams.specialty
)
GROUP BY ams.specialty;

-- A mistyped target type would look like an honestly unresolved target in
-- flippatch. The allowed subset is not derivable here; `model-relationship` is
-- the one value that is not a Flipcommons entity type.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_specialty_target_entity_type_unknown',
  ipdb_specialty || ' -> ' || target_entity_type
FROM ipdb_ref.specialty
WHERE target_entity_type NOT IN (
  'game-format', 'reward-type', 'gameplay-feature', 'tag', 'cabinet', 'model-relationship'
);

-- A duplicate rule would fan one specialty into two catalog facts.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_specialty_duplicated',
  ipdb_specialty || ' has ' || count(*) || ' rules'
FROM ipdb_ref.specialty
GROUP BY ipdb_specialty
HAVING count(*) > 1;

-- Expiry guard for archive-supplied specialties, the counterpart of the credit
-- role one above. Assignments come only from archive pages, some captured in
-- 2018; a dump that gains the field is both newer and authoritative, and
-- `ipdb_stg.archive_model_specialties` must then be rebuilt to prefer it.
-- Nothing else asks that question: the new column stars through to `ipdb.models`
-- and fails the snake_case check, but naming it there makes the build green
-- again with the archive still supplying every specialty.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_specialty_xantari_column_appeared',
  'ipdb_raw.xantari_model_snapshots.' || column_name
    || ' -- xantari now carries Specialty and outranks the archive pages'
FROM duckdb_columns()
WHERE database_name = current_database()
  AND schema_name = 'ipdb_raw' AND table_name = 'xantari_model_snapshots'
  AND column_name ILIKE 'special%';

------------------------------------------------------------
-- Document classification vocabulary
------------------------------------------------------------

-- The vocabulary is split across three hand-maintained tables that reference
-- each other by name, so a rename applied to one of them dangles in the others.
-- Nothing downstream raises when that happens: `ipdb_stg.file_class_matches` joins
-- patterns to classes with an INNER JOIN, so a pattern naming a class that no
-- longer exists contributes nothing and the build stays green, so renaming a
-- well-populated class would quietly delete every match it had.

-- A pattern detects a class that isn't in the vocabulary
INSERT INTO checks.violations
SELECT 'documents', 'document_class_pattern_orphan',
  document_class || ' (pattern: ' || pattern || ')'
FROM ipdb_ref.document_class_pattern
WHERE document_class NOT IN (SELECT document_class FROM ipdb_ref.document_class);

-- A hierarchy edge names a class that isn't in the vocabulary, at either end
INSERT INTO checks.violations
SELECT 'documents', 'document_class_parent_orphan', detail
FROM (
  SELECT document_class || ' -> ' || parent_class || ' (unknown child)' AS detail
  FROM ipdb_ref.document_class_parent
  WHERE document_class NOT IN (SELECT document_class FROM ipdb_ref.document_class)
  UNION ALL
  SELECT document_class || ' -> ' || parent_class || ' (unknown parent)'
  FROM ipdb_ref.document_class_parent
  WHERE parent_class NOT IN (SELECT document_class FROM ipdb_ref.document_class)
);

-- A class declares a kind that isn't in the vocabulary. This is the only thing
-- that reads `ipdb_ref.source_kind`, but the kinds are load-bearing anyway:
-- `ipdb.patents` and `ipdb.trade_articles` select on their literal values, so a
-- misspelt kind empties those views rather than failing.
INSERT INTO checks.violations
SELECT 'documents', 'document_class_unknown_kind',
  document_class || ' -> ' || source_kind
FROM ipdb_ref.document_class
WHERE source_kind NOT IN (SELECT source_kind FROM ipdb_ref.source_kind);

------------------------------------------------------------
-- Results
------------------------------------------------------------

-- Per-category violation summary
SELECT category, count(*) AS violations
FROM checks.violations GROUP BY category ORDER BY category;

SELECT CASE
  WHEN count(*) > 0
  THEN error(count(*) || ' contract violation(s) found')
  ELSE 'All checks passed'
END FROM checks.violations;
