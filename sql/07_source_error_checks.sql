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

-- The detail is built with `concat` and coalesced, not `||`. A record broken
-- enough to have lost its id is exactly the one likely to have lost its name
-- too, and `||` propagates NULL -- so the build would abort naming nothing at
-- all. Two fields, because either may be the one that survived.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_record_missing_id',
  concat('name=', coalesce("name", '(none)'),
         ', manufacturer=', coalesce(manufacturer_name, '(none)'))
FROM opdb_stg.machines WHERE opdb_id IS NULL;

-- Machines and aliases are separate lists upstream with nothing keeping them
-- disjoint, and staging unions them -- an id in both silently doubles a machine
-- and every count taken over it.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_duplicate_id', opdb_id || ' x' || count(*)
FROM opdb_stg.machines GROUP BY opdb_id HAVING count(*) > 1;

-- A row whose id shape disagrees with the array it arrived in. A machine id has
-- two segments and an alias id three, so a three-segment row among the machines
-- is an alias filed as one -- and it then skips every check below that is
-- guarded on `isAlias`, which is what makes the misfiling silent.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_id_shape_disagrees_with_kind', opdb_id
FROM opdb_stg.machines WHERE is_alias <> (alias_id IS NOT NULL);

-- OPDB states the hierarchy twice, in the id and in the arrays. These two check
-- the statements agree; where they don't, a join through the hierarchy loses
-- rows with nothing raising.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_machine_group_missing', m.opdb_id
FROM opdb_stg.machines AS m
WHERE NOT EXISTS (
  SELECT 1 FROM opdb_raw.machine_groups AS g WHERE g.opdbId = m.group_id
);

INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_alias_parent_machine_missing', a.opdb_id
FROM opdb_stg.machines AS a
WHERE a.is_alias
  AND NOT EXISTS (
    SELECT 1 FROM opdb_stg.machines AS m
    WHERE m.is_machine AND m.opdb_id = a.group_id || '-' || a.machine_id
  );

-- A group year `opdb_stg.machine_groups` could not read as a number. It uses
-- TRY_CAST so a malformed one does not crash the layer, which leaves this as the
-- only thing standing between a bad year and a silent NULL.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_group_year_not_a_number', opdbId || ': ' || "year"
FROM opdb_raw.machine_groups
WHERE "year" IS NOT NULL AND TRY_CAST("year" AS SMALLINT) IS NULL;

-- The image struct OPDB ships, changed in any way. This is the one place in the
-- OPDB path where the star discipline cannot reach: a new top-level COLUMN trips
-- `opdb_column_not_snake_case`, but `opdb.model_images` reaches INSIDE this
-- struct by name -- all fifteen fields of it -- so anything OPDB adds, drops or
-- renames in here would simply not be selected and would vanish without a word.
--
-- Asserted against the column's declared TYPE, not by unnesting every image.
-- `images` is one DuckDB type, so every element carries identical keys by
-- construction; reading them per row learns nothing the type does not say, and
-- an export with no images leaves nothing to unnest and reports clean.
--
-- Whole-type equality rather than a pattern. Every field is read by the mart, so
-- there is no part of this type a change could touch harmlessly, and a pattern
-- pinning one sub-struct silently leaves the rest open. The detail prints the
-- new type, so the diff is readable when it fires.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_image_struct_changed',
  concat('opdb_stg.machines.images is ', data_type)
FROM duckdb_columns()
WHERE database_name = current_database()
  AND schema_name = 'opdb_stg' AND table_name = 'machines' AND column_name = 'images'
  AND data_type <> 'STRUCT("group" UUID, title VARCHAR, "primary" BOOLEAN, "type" VARCHAR, urls STRUCT(medium VARCHAR, "large" VARCHAR, small VARCHAR), sizes STRUCT(medium STRUCT(width BIGINT, height BIGINT), "large" STRUCT(width BIGINT, height BIGINT), small STRUCT(width BIGINT, height BIGINT)))[]';

-- The types `read_json_auto` inferred for OPDB's timestamps, asserted because
-- the layer above reads them as dates and nothing else would notice a change.
-- OPDB writes date-only strings today and the inference lands on DATE; the day
-- it appends a time component the column silently becomes TIMESTAMP and every
-- comparison through `updated_at` changes meaning without failing. The changelog
-- already infers as VARCHAR from the same source, which is why this is worth
-- pinning rather than trusting.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_raw_timestamp_type_changed',
  concat(table_name, '.', column_name, ' is ', data_type, ', expected DATE')
FROM duckdb_columns()
WHERE database_name = current_database()
  AND schema_name = 'opdb_raw'
  AND table_name IN ('machines', 'aliases', 'machine_groups')
  AND column_name IN ('createdAt', 'updatedAt')
  AND data_type <> 'DATE';

-- Same shape, same reason as `opdb_record_missing_id` above.
INSERT INTO checks.violations
SELECT 'source_dumps', 'ipdb_record_missing_id',
  concat('title=', coalesce(Title, '(none)'),
         ', manufacturer=', coalesce(Manufacturer, '(none)'))
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
-- OPDB feature vocabulary
------------------------------------------------------------

-- A feature OPDB has started using that no rule decodes. `opdb_stg.model_features`
-- joins INNER, so without this the new value would simply not be published --
-- and OPDB adds one whenever it needs to describe a machine it has not seen
-- before, which is exactly when we want to know.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_feature_unmapped',
  concat('"', t.feature, '" is stated by ', count(*),
         ' model(s) and no opdb_ref.feature row decodes it')
FROM opdb_stg.machines AS m, unnest(m.features) AS t(feature)
WHERE NOT EXISTS (
  SELECT 1 FROM opdb_ref.feature AS f WHERE f.opdb_feature = t.feature
)
GROUP BY t.feature;

-- A mistyped target type would read in flippatch as an honestly unresolved
-- target. `model-relationship` and `model-lineage` are the two values that name
-- a Flipcommons STRUCTURE rather than one of its entity types.
--
-- NULL is tested separately because `NULL NOT IN (...)` is NULL, not true, so a
-- missing value passes a bare `NOT IN` and reaches a mart row with no type at
-- all. Neither column may be NULL here -- unlike `opdb_ref.keyword`, where a
-- NULL type is the deliberate `no-target` verdict.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_feature_target_entity_type_unknown',
  concat_ws(' -> ', opdb_feature, coalesce(target_entity_type, '(null)'))
FROM opdb_ref.feature
WHERE target_entity_type IS NULL
   OR target_entity_type NOT IN (
     'reward-type', 'gameplay-feature', 'tag', 'cabinet', 'model-relationship', 'model-lineage'
   );

INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_feature_target_value_missing', opdb_feature
FROM opdb_ref.feature WHERE target_value IS NULL;

-- Every value these two tables emit is slug-shaped. THE ONLY THING PINEXPLORE
-- CAN ASSERT ABOUT A TARGET: whether the catalog holds it is unknowable from
-- here, so nothing claims to know, and what is left is that the value is
-- well-formed. `pro-edition` and `limited-edition` are indistinguishable to this
-- check, which is the point -- the mart speaks one vocabulary and flippatch
-- decides which halves of it resolve.
--
-- Underscores are allowed because `model-lineage` and `model-relationship`
-- targets name Flipcommons columns and relationship types rather than public
-- ids: `export_edition_of`, `conversion_kit`.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_target_value_not_slug_shaped', concat_ws(' -> ', src, value)
FROM (
  SELECT 'opdb_ref.feature' AS src, opdb_feature AS src_value, target_value AS value
  FROM opdb_ref.feature
  UNION ALL
  SELECT 'opdb_ref.keyword', opdb_keyword, target_value FROM opdb_ref.keyword
) AS v
WHERE value IS NOT NULL
  AND NOT regexp_full_match(value, '[a-z0-9][a-z0-9_-]*');

------------------------------------------------------------
-- OPDB keyword vocabulary
------------------------------------------------------------

-- OPDB's second coded vocabulary, guarded the same way as its first.
--
-- A keyword OPDB has started using that no rule decodes. `opdb_stg.model_keywords`
-- joins INNER, so without this the new value would simply not be published.
--
-- This is also what catches a CASING change upstream. The keyword vocabulary is
-- mixed-case -- `Bathurst` beside `board-game` -- and the join is exact, so
-- normalising the case upstream would drop every affected row. Blocking on that
-- is the intent, not a shortcoming of the join: `OpdbMappings.md` asks for a
-- value nobody has ruled on to stop the build, and a case-insensitive join would
-- hide precisely the event we want to be told about.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_keyword_unmapped',
  concat('"', t.keyword, '" is stated by ', count(*),
         ' model(s) and no opdb_ref.keyword row decodes it')
FROM opdb_stg.machines AS m, unnest(m.keywords) AS t(keyword)
WHERE NOT EXISTS (
  SELECT 1 FROM opdb_ref.keyword AS k WHERE k.opdb_keyword = t.keyword
)
GROUP BY t.keyword;

-- A mistyped target type. `opdb_ref.keyword` folds the `no-target` sentinel to
-- NULL, so NULL here is the deliberate "carries no catalog fact" verdict and is
-- exempt; anything else has to name a Flipcommons entity type. A typo in either
-- the type or the sentinel lands in this check rather than silently dropping the
-- keyword out of `opdb.model_themes`.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_keyword_target_entity_type_unknown',
  concat_ws(' -> ', opdb_keyword, target_entity_type)
FROM opdb_ref.keyword
WHERE target_entity_type IS NOT NULL
  AND target_entity_type NOT IN ('theme', 'tag', 'gameplay-feature', 'series');

-- A duplicate rule fans one OPDB value into two catalog facts. Only these two
-- vocabularies need it: `technology_generation` and `display_type` are joined in
-- `opdb_stg.machines`, where a duplicate shows up as a row count that no longer
-- matches the export and `opdb_staged_machines_grain_not_one_row_per_export_row`
-- catches it; `edition_rank` is read through `min()`, where a duplicate changes
-- nothing.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_feature_duplicated', concat(opdb_feature, ' has ', count(*), ' rules')
FROM opdb_ref.feature GROUP BY opdb_feature HAVING count(*) > 1;

INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_keyword_duplicated', concat(opdb_keyword, ' has ', count(*), ' rules')
FROM opdb_ref.keyword GROUP BY opdb_keyword HAVING count(*) > 1;

-- The lineage flags -- `is_remake`, `is_export`, `is_conversion` -- are read out
-- of this table by OUR slug rather than by OPDB's wording, so a rename upstream
-- reaches them through the decode and cannot desync it. What CAN still break
-- them is an edit on this side: drop or re-slug one of these targets and the
-- flag goes quietly empty, turning every export edition into an ordinary variant
-- with nothing raising. Three keys, asserted to still resolve.
--
-- `model-relationship` is checked as an entity type, not a slug, because BOTH
-- its rows feed `is_conversion` -- naming either one alone would miss the
-- other, and OPDB spells a conversion two ways.
INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_lineage_decode_key_unresolved', k.key
FROM (VALUES ('is_remake'), ('export_edition_of')) AS k(key)
WHERE NOT EXISTS (
  SELECT 1 FROM opdb_ref.feature AS f WHERE f.target_value = k.key
);

INSERT INTO checks.violations
SELECT 'source_dumps', 'opdb_lineage_decode_key_unresolved', 'model-relationship'
WHERE NOT EXISTS (
  SELECT 1 FROM opdb_ref.feature AS f WHERE f.target_entity_type = 'model-relationship'
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
