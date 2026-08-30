-- Structural checks over the finished database. Aborts the build on any row.

------------------------------------------------------------
-- Nothing lands in `main`
------------------------------------------------------------

-- Every relation belongs to a schema that says which layer it is in. A stray
-- `main` object is one nobody classified, and it is invisible to the boundary
-- rule flippatch enforces from the other side -- an unlayered name matches none
-- of its patterns, so it reads as published whether or not it is.
--
-- Filtered to this database because the build's own session also holds `temp`,
-- whose objects genuinely do live in `temp.main`; without the filter this would
-- either fire on them or, written the other way, pass vacuously.
--
-- Macros are included. A macro in `main` is as unlayered as a view is, and it is
-- the easier one to leave behind, since nothing about its definition mentions a
-- schema.
INSERT INTO checks.violations
SELECT 'layering', 'object_in_main_schema', kind || ' ' || nm
FROM (
  SELECT 'table' AS kind, table_name AS nm
  FROM duckdb_tables() WHERE database_name = current_database() AND schema_name = 'main'
  UNION ALL
  SELECT 'view', view_name
  FROM duckdb_views() WHERE database_name = current_database() AND schema_name = 'main' AND NOT internal
  UNION ALL
  SELECT 'macro', function_name
  FROM duckdb_functions() WHERE database_name = current_database() AND schema_name = 'main' AND NOT internal
);

------------------------------------------------------------
-- The mart is spelled the way it promises
------------------------------------------------------------

-- A dump field that reached `ipdb.models` under IPDB's spelling because nobody
-- named it. The fix is one line in the RENAME list in `09_mart.sql`.
--
-- A rule over whatever the view currently projects, rather than a list of
-- expected names: a list would only ever cover what someone remembered to type
-- while reading as though it covered everything.
INSERT INTO checks.violations
SELECT 'mart', 'ipdb_models_column_not_snake_case', column_name
FROM duckdb_columns()
WHERE database_name = current_database()
  AND schema_name = 'ipdb' AND table_name = 'models'
  AND NOT regexp_full_match(column_name, '[a-z][a-z0-9_]*');

-- Every view in the OPDB mart, not one relation: OPDB exports camelCase and all
-- of them rename it. The fix is one line in the relevant RENAME in `09_mart.sql`.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_column_not_snake_case', table_name || '.' || column_name
FROM duckdb_columns()
WHERE database_name = current_database()
  AND schema_name = 'opdb'
  AND NOT regexp_full_match(column_name, '[a-z][a-z0-9_]*');

-- Staging's two code lookups may not multiply: a duplicate `opdb_ref` row for
-- one code fans every machine using it into two, and the count carries through
-- to `opdb.models` with both copies looking like real machines. Asserted against
-- the raw arrays, which are the only place the true machine count survives.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_staged_machines_grain_not_one_row_per_export_row',
  (SELECT count(*) FROM opdb_stg.machines)::VARCHAR || ' staged from '
    || ((SELECT count(*) FROM opdb_raw.machines) + (SELECT count(*) FROM opdb_raw.aliases))::VARCHAR
    || ' exported'
WHERE (SELECT count(*) FROM opdb_stg.machines)
   <> (SELECT count(*) FROM opdb_raw.machines) + (SELECT count(*) FROM opdb_raw.aliases);

-- `opdb.models` publishes ids the view itself does not contain. The pointers are
-- derived, so nothing upstream guarantees they land: an election bug aiming one
-- at a container would produce a variant of a machine that does not exist, and
-- the join would return nothing rather than complain.
--
-- `variant_parent_id` is exempt by design -- it is a sibling-set key, not an FK,
-- and it names one of OPDB's containers on every variant that hangs off one.
-- `sibling_set_primary_id` is NOT exempt: it is elected from the members of a
-- set, all of which `opdb.models` publishes, so it always names a real model --
-- including on the primary itself, where it names that row.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_model_lineage_pointer_dangling', concat_ws(' -> ', m.opdb_id, m.target)
FROM (
  SELECT opdb_id, unnest([variant_of, export_edition_of, sibling_set_primary_id]) AS target
  FROM opdb.models
) AS m
WHERE m.target IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM opdb.models AS t WHERE t.opdb_id = m.target);

-- `variant_of` never crosses a manufacturer. THE CLAIM THE MART RESTS ON.
--
-- Since the election moved out of `variant_of`, this is a TAUTOLOGY: the column
-- is filled only where `variant_parent_relation = 'variant'`, which is the same
-- test written the other way, twenty lines up in `opdb_stg.alias_lineage`. Kept
-- anyway, as cheap insurance and labelled as such -- publishing a licensed copy
-- as a Flipcommons variant is the worst outcome this design has, and one `CASE`
-- is all that now stands between here and there.
--
-- It was NOT a tautology before: the verdict was taken against a container while
-- `variant_of` pointed at an elected sibling, so the two could disagree. Nothing
-- elects into `variant_of` any more, which is what removed the hazard.
--
-- `IS DISTINCT FROM` rather than `<>`, so a NULL manufacturer on either side is
-- caught rather than passing quietly.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_variant_of_crosses_manufacturer', concat_ws(' -> ', m.opdb_id, p.opdb_id)
FROM opdb.models AS m
JOIN opdb.models AS p ON p.opdb_id = m.variant_of
WHERE m.opdb_manufacturer_id IS DISTINCT FROM p.opdb_manufacturer_id;

-- A variant pointing at a variant. Flipcommons expects `variant_of` to reach a
-- canonical model in one hop, so a chain here would be published as a lineage
-- the catalog cannot represent. True by construction -- a stated parent is never
-- an alias, and an elected primary is a variant of nothing -- which is exactly
-- why a break in it would otherwise go unseen.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_variant_of_is_itself_a_variant', concat_ws(' -> ', m.opdb_id, t.opdb_id)
FROM opdb.models AS m
JOIN opdb.models AS t ON t.opdb_id = m.variant_of
WHERE t.variant_of IS NOT NULL;

-- Every model belongs to a title. The id carries the group, so this asserts the
-- two views agree on what exists, not merely that the string parses.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_model_title_missing', m.opdb_id
FROM opdb.models AS m
WHERE NOT EXISTS (SELECT 1 FROM opdb.titles AS t WHERE t.opdb_id = m.title_opdb_id);

-- `opdb.models` is OPDB's machines and aliases, minus the containers, and
-- nothing else. Asserted on the count because the WHERE that drops containers is
-- one clause away from dropping the aliases too, and because the `cabinet` join
-- is a lookup that must not multiply.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_models_grain_not_machines_plus_aliases',
  concat((SELECT count(*) FROM opdb.models), ' published, expected ',
         (SELECT count(*) FROM opdb_stg.machines WHERE is_model))
WHERE (SELECT count(*) FROM opdb.models)
   <> (SELECT count(*) FROM opdb_stg.machines WHERE is_model);

-- Every `opdb.model_*` row is keyed to a model `opdb.models` actually publishes.
--
-- These views are built from staging, which holds OPDB's containers too, so each
-- carries its own restriction back to the model grain -- and a view added later
-- will not. The failure is invisible from the outside: the extra rows key to ids
-- that simply do not join, so a consumer sees fewer tags than it expected and
-- nothing says why.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_model_row_without_a_model', v || ': ' || opdb_id
FROM (
  SELECT 'model_tags' AS v, opdb_id FROM opdb.model_tags
  UNION ALL SELECT 'model_themes', opdb_id FROM opdb.model_themes
  UNION ALL SELECT 'model_reward_types', opdb_id FROM opdb.model_reward_types
  UNION ALL SELECT 'model_gameplay_features', opdb_id FROM opdb.model_gameplay_features
  UNION ALL SELECT 'model_relationships', opdb_id FROM opdb.model_relationships
  UNION ALL SELECT 'model_export_markets', opdb_id FROM opdb.model_export_markets
  UNION ALL SELECT 'model_images', opdb_id FROM opdb.model_images
  UNION ALL SELECT 'model_abbreviations', opdb_id FROM opdb.model_abbreviations
  UNION ALL SELECT 'model_series', opdb_id FROM opdb.model_series
) AS r
WHERE NOT EXISTS (SELECT 1 FROM opdb.models AS m WHERE m.opdb_id = r.opdb_id);

-- Every id OPDB has ever issued appears once in `opdb.model_ids`. The id
-- universe is a UNION, so an id in both the export and the changelog is already
-- one row; what this now catches is a CHANGELOG holding two retirements for one
-- id, which the join to it would fan out. Nothing at the source layer asserts
-- that, and it is a real thing for an upstream to get wrong -- an id both moved
-- and deleted has two answers to "where is it now".
INSERT INTO checks.violations
SELECT 'mart', 'opdb_model_id_duplicated', concat(opdb_id, ' x', count(*))
FROM opdb.model_ids GROUP BY opdb_id HAVING count(*) > 1;

-- A `moved` id that resolves nowhere. The chain walk terminates on a replacement
-- nothing has since MOVED, so a NULL here means it never terminated -- a cycle,
-- or a link the recursion could not follow. Silently NULL, it would read as a
-- deletion, which is the one thing a move is not.
INSERT INTO checks.violations
SELECT 'mart', 'opdb_moved_id_unresolved', opdb_id
FROM opdb.model_ids WHERE status = 'moved' AND current_opdb_id IS NULL;

-- Every published mart relation carries a one-line COMMENT.
--
-- Guards coverage only: it cannot tell whether a description is right or stale.
-- It catches a mart relation with no comment at all, the same shape as
-- `opdb_column_not_snake_case` over `duckdb_columns()`.
--
-- The schema list is enumerated rather than derived. `glossary` and `web_cache`
-- are readable and uncommented, and writing their one-liners without reading
-- those relations would put confidently wrong descriptions in the database,
-- which is the failure this whole idiom exists to prevent. Add a schema here
-- once its relations are described.
INSERT INTO checks.violations
SELECT 'mart', 'mart_relation_undocumented', schema_name || '.' || relation_name
FROM (
  SELECT schema_name, view_name AS relation_name, comment
  FROM duckdb_views() WHERE database_name = current_database()
  UNION ALL
  SELECT schema_name, table_name, comment
  FROM duckdb_tables() WHERE database_name = current_database()
)
WHERE schema_name IN ('opdb', 'ipdb', 'ingest')
  AND coalesce(comment, '') = '';

-- Every warning is a count of its own `checks.<check_name>` view, and the two
-- lists have to stay in step. A count with no view is a population stated
-- nowhere, and a view with no count is a worklist nothing prints -- the same
-- thing from the other end: rows nobody is told about.
INSERT INTO checks.violations
SELECT 'mart', 'checks_warning_without_view', w.check_name
FROM checks.warnings AS w
WHERE NOT EXISTS (
  SELECT 1 FROM duckdb_views()
  WHERE database_name = current_database() AND schema_name = 'checks'
    AND view_name = w.check_name
);

INSERT INTO checks.violations
SELECT 'mart', 'checks_view_without_warning', v.view_name
FROM duckdb_views() AS v
WHERE v.database_name = current_database() AND v.schema_name = 'checks'
  AND NOT EXISTS (SELECT 1 FROM checks.warnings AS w WHERE w.check_name = v.view_name);

-- A sentinel manufacturer id reaching a consumer untranslated. Checked because
-- the failure is silent from the outside: the id reads as a real corporate
-- entity, attributing models to a company IPDB explicitly declines to name.
INSERT INTO checks.violations
SELECT 'mart', 'ipdb_models_corporate_entity_sentinel_not_translated',
  'ipdb_id ' || m.ipdb_id || ' -> corporate entity ' || m.ipdb_corporate_entity_id
FROM ipdb.models AS m
INNER JOIN ipdb_ref.corporate_entity_not_a_company AS nac
  ON nac.ipdb_manufacturer_id = m.ipdb_corporate_entity_id;

-- Both of the mart's joins are lookups and neither may multiply. Asserted on the
-- grain itself rather than on each way it could break -- a second
-- `ipdb_ref.duplicate_listings` row for one id fans the mart out, and so would a
-- cause nobody thought of.
INSERT INTO checks.violations
SELECT 'mart', 'ipdb_models_grain_not_one_row_per_staged_model',
  (SELECT count(*) FROM ipdb_stg.models)::VARCHAR || ' staged -> '
    || (SELECT count(*) FROM ipdb.models)::VARCHAR || ' published'
WHERE (SELECT count(*) FROM ipdb.models) <> (SELECT count(*) FROM ipdb_stg.models);

-- One published row per staged specialty assignment.
--
-- The vocabulary lookup may not change the count. A duplicate rule is caught
-- upstream by name; this catches the shape, including a cause nobody thought of.
-- Equality rather than `<=` because the join is INNER, and a dropped assignment
-- is the quieter fault -- fewer rows read as a smaller corpus.
INSERT INTO checks.violations
SELECT 'mart', 'ipdb_model_specialties_grain_not_one_row_per_assignment',
  (SELECT count(*) FROM ipdb_stg.model_specialties)::VARCHAR || ' staged -> '
    || (SELECT count(*) FROM ipdb.model_specialties)::VARCHAR || ' published'
WHERE (SELECT count(*) FROM ipdb.model_specialties)
   <> (SELECT count(*) FROM ipdb_stg.model_specialties);

------------------------------------------------------------
-- Hand-maintained inputs hold the grain their joins assume
------------------------------------------------------------

-- The acquisition log is hand-maintained and joined into `ingest.watermarks`
-- keyed on `artifact`, so a duplicated entry could silently double a watermark
-- row -- the one view every downstream campaign reads to know what went in.
INSERT INTO checks.violations
SELECT 'ingest', 'artifact_acquisition_duplicated', artifact
FROM ref.artifact_acquisitions
GROUP BY artifact HAVING count(*) > 1;

------------------------------------------------------------
-- Results
------------------------------------------------------------

SELECT category, count(*) AS violations
FROM checks.violations GROUP BY category ORDER BY category;

SELECT CASE
  WHEN count(*) > 0
  THEN error(count(*) || ' contract violation(s) found')
  ELSE 'All checks passed'
END FROM checks.violations;
