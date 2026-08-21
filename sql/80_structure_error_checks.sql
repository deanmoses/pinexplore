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
  AND NOT regexp_matches(column_name, '^[a-z][a-z0-9_]*$');

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
  (SELECT count(*) FROM ipdb_stg.archive_model_specialties)::VARCHAR || ' staged -> '
    || (SELECT count(*) FROM ipdb.model_specialties)::VARCHAR || ' published'
WHERE (SELECT count(*) FROM ipdb.model_specialties)
   <> (SELECT count(*) FROM ipdb_stg.archive_model_specialties);

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
