-- Soft warnings (data quality, not hard build-stopping violations).
-- Creates and fills `checks.warnings`; no later layer adds to it.
--
-- Most of these watch a hand-maintained exception list for going stale, which it
-- does silently by construction: a rule that stops matching suppresses nothing,
-- and nothing raises.
--
-- EVERY WARNING IS A VIEW PLUS A COUNT OF IT. `checks.<check_name>` holds the
-- rows and `checks.warnings` holds the number, and the number is taken FROM the
-- view -- so the count and the worklist cannot disagree, and there is one place
-- to fix when either is wrong.
--
-- The population is a view rather than a query in a comment because DuckDB binds
-- a view at CREATE time: a renamed relation or a moved column fails the build
-- instead of sitting there reading plausibly. `FROM duckdb_views() WHERE
-- schema_name = 'checks'` is a free list of every warning the build can raise.
--
-- These views run BEFORE the marts exist, so they read staging and raw. A
-- worklist wanting mart columns joins them itself.

-- A real table for the same reason as `checks.violations`: a build that dies
-- later still leaves its warnings behind to be read.
CREATE OR REPLACE TABLE checks.warnings (check_name VARCHAR, cnt BIGINT);

------------------------------------------------------------
-- Data quality warnings
------------------------------------------------------------

-- Models the newest IPDB snapshot dropped that an older one still has, so
-- `ipdb_stg.models_merged` is serving a stale observation of them (see 02_raw).
--
-- Each is either a crawl miss or an upstream deletion, and only loading the URL
-- tells them apart. Meant to be worked down: confirm the model is still on
-- ipdb.org and leave it carried forward, or confirm it is gone and file it in
-- `ipdb_ref.retracted`. Left alone, absence stops meaning anything.
CREATE OR REPLACE VIEW checks.ipdb_records_carried_forward AS
SELECT IpdbId, Title, Manufacturer, snapshot_utc
FROM ipdb_stg.models_merged
WHERE carried_forward;

INSERT INTO checks.warnings
SELECT 'ipdb_records_carried_forward', count(*) FROM checks.ipdb_records_carried_forward;

-- A retraction that the newest snapshot contradicts: IPDB is serving the record
-- again, and ipdb_ref.retracted is now suppressing a live model. Stale
-- exceptions fail silently by construction -- the row simply vanishes from
-- `ipdb_stg.models_merged` -- so the contradiction has to be checked for.
CREATE OR REPLACE VIEW checks.ipdb_retraction_contradicted AS
SELECT r.ipdb_id, r.first_absent_on, r.reason, r.evidence_url
FROM ipdb_ref.retracted AS r
WHERE EXISTS (
  SELECT 1 FROM ipdb_raw.xantari_model_snapshots AS s
  WHERE s.IpdbId = r.ipdb_id
    AND s.snapshot_utc = (SELECT max(snapshot_utc) FROM ipdb_raw.xantari_model_snapshots)
);

INSERT INTO checks.warnings
SELECT 'ipdb_retraction_contradicted', count(*) FROM checks.ipdb_retraction_contradicted;

-- One IPDB manufacturer id carrying more than one manufacturer string. The id is
-- the join key every maker lookup uses, so a second string behind it multiplies
-- `ipdb_stg.corporate_entities` and every count taken through it.
--
-- Warns rather than aborting because the cause is upstream: either IPDB now
-- issues the id for two companies, or the dump misparsed a record onto it. A new
-- row here is usually a page to read and a row for
-- `ipdb_ref.model_corporate_entity_misparsed`.
CREATE OR REPLACE VIEW checks.ipdb_corporate_entity_ambiguous AS
SELECT ManufacturerId, list(DISTINCT Manufacturer) AS manufacturers
FROM ipdb_stg.models
WHERE ManufacturerId IS NOT NULL
GROUP BY ManufacturerId
HAVING count(DISTINCT Manufacturer) > 1;

INSERT INTO checks.warnings
SELECT 'ipdb_corporate_entity_ambiguous', count(*) FROM checks.ipdb_corporate_entity_ambiguous;

-- A duplicate-listing pair the newest snapshot no longer supports. Nothing
-- downstream raises when one of the two stops existing or matching -- a consumer
-- simply keeps suppressing a gap that is now real.
--
-- Two ways to go stale: an id vanishes, collapsing the pair, or the titles stop
-- agreeing, which means IPDB re-pointed an id at a different model and is more
-- dangerous because the pair still looks intact. Checked against the merged view
-- rather than the newest snapshot, so absence means IPDB dropped it rather than
-- that one crawl missed it.
CREATE OR REPLACE VIEW checks.ipdb_duplicate_listing_unconfirmed AS
SELECT d.ipdb_id, d.duplicate_of_ipdb_id, d.reason,
       a.Title AS title, b.Title AS duplicate_of_title
FROM ipdb_ref.duplicate_listings AS d
LEFT JOIN ipdb_stg.models_merged AS a ON a.IpdbId = d.ipdb_id
LEFT JOIN ipdb_stg.models_merged AS b ON b.IpdbId = d.duplicate_of_ipdb_id
WHERE a.IpdbId IS NULL
   OR b.IpdbId IS NULL
   OR a.Title IS DISTINCT FROM b.Title;

INSERT INTO checks.warnings
SELECT 'ipdb_duplicate_listing_unconfirmed', count(*) FROM checks.ipdb_duplicate_listing_unconfirmed;

-- The IPDB header line no longer matches the grammar parsed in
-- `ipdb_stg.model_additional_details`. Warns rather than aborting: the shape of
-- this string is xantari's to change, and a reformat upstream should not stop a
-- build. It does mean the parsed date columns are silently empty for those rows.
CREATE OR REPLACE VIEW checks.ipdb_additional_details_unparsed AS
SELECT i.IpdbId, i.Title, i.AdditionalDetails
FROM ipdb_stg.models_merged AS i
JOIN ipdb_stg.model_additional_details AS ad ON ad.IpdbId = i.IpdbId
WHERE i.AdditionalDetails IS NOT NULL
  AND ad.additional_details_ipd_no IS NULL;

INSERT INTO checks.warnings
SELECT 'ipdb_additional_details_unparsed', count(*) FROM checks.ipdb_additional_details_unparsed;

-- Archive pages for models no xantari snapshot lists, which
-- `ipdb_stg.archive_models` drops. Xantari is authoritative on which models
-- exist, so dropping is right -- but each of these is either a listing IPDB has
-- deleted and we have not recorded, or one a xantari crawl missed, and both are
-- worth acting on.
--
-- Ids in `ipdb_ref.retracted` are EXCLUDED, and that exclusion is the difference
-- between a worklist and noise. A confirmed retraction is absent from the dump
-- on purpose, with its evidence already filed; counting it here would leave a
-- warning permanently non-zero and nothing to do about it, which is how a whole
-- warning block stops being read.
CREATE OR REPLACE VIEW checks.ipdb_archive_model_not_in_dump AS
SELECT am.ipdb_id, am."name", am.archive_capture_date, am.source_url
FROM ipdb_raw.archive_models AS am
WHERE NOT EXISTS (
    SELECT 1 FROM ipdb_raw.xantari_model_snapshots AS s WHERE s.IpdbId = am.ipdb_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM ipdb_ref.retracted AS r WHERE r.ipdb_id = am.ipdb_id
  );

INSERT INTO checks.warnings
SELECT 'ipdb_archive_model_not_in_dump', count(*) FROM checks.ipdb_archive_model_not_in_dump;

-- A credit label on an archive page that `ipdb_ref.credit_role.archive_label`
-- does not list, so `ipdb_stg.credits` dropped it.
--
-- The parser finds credits by the "... by" SHAPE of a page label rather than
-- from a fixed list, so IPDB can hand us a role nobody has seen and the parse
-- will happily carry it. Without this the row would simply not join and would
-- vanish. A non-zero count on the first build after a change here means the
-- `archive_label` lists are wrong, not that IPDB invented a role.
CREATE OR REPLACE VIEW checks.ipdb_archive_credit_role_unrecognised AS
SELECT DISTINCT c.role
FROM ipdb_stg.archive_models AS am, unnest(am.credits) AS t(c)
WHERE NOT EXISTS (
  SELECT 1 FROM ipdb_ref.credit_role AS cr WHERE list_contains(cr.archive_label, c.role)
);

INSERT INTO checks.warnings
SELECT 'ipdb_archive_credit_role_unrecognised', count(*) FROM checks.ipdb_archive_credit_role_unrecognised;

-- A machine the specialty census lists that the xantari dump has never listed,
-- so `ipdb_stg.specialty_census` dropped it and its specialties are unpublished.
--
-- The counterpart of `ipdb_archive_model_not_in_dump` above, and it means the
-- opposite. There, a machine missing from the dump is ambiguous -- the capture
-- may predate a deletion. Here the census is the NEWER source, so a machine it
-- lists and the dump does not is one IPDB has added since the dump was taken.
-- The remedy is not research, it is a fresh xantari snapshot, and a count that
-- climbs over successive censuses is how that need becomes visible.
CREATE OR REPLACE VIEW checks.ipdb_specialty_census_model_not_in_dump AS
SELECT c.ipdb_id, c."name", c.manufacturer, c.date_text
FROM ipdb_raw.specialty_census AS c
WHERE NOT EXISTS (
    SELECT 1 FROM ipdb_raw.xantari_model_snapshots AS s WHERE s.IpdbId = c.ipdb_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM ipdb_ref.retracted AS r WHERE r.ipdb_id = c.ipdb_id
  );

INSERT INTO checks.warnings
SELECT 'ipdb_specialty_census_model_not_in_dump', count(*)
FROM checks.ipdb_specialty_census_model_not_in_dump;

-- A field where the census and the dump describe the same model differently.
--
-- The census is a live read of IPDB and the dump is months old, so a
-- disagreement is one of three things and the row does not say which: IPDB
-- edited the record, the dump mis-scraped it, or this build mis-parsed the
-- results table. All three are worth a look, and none is worth failing a build
-- over -- hence a warning, and hence the row carrying both values rather than
-- picking one.
--
-- The census does not overrule the dump anywhere. Nothing downstream reads these
-- columns as values; `ipdb.models` is unchanged and still states what xantari
-- states. This is the whole use the census's non-specialty columns are put to.
--
-- Expected to be small and nearly static. A count that jumps is more likely a
-- parse regression here than IPDB revising thousands of records.
CREATE OR REPLACE VIEW checks.ipdb_specialty_census_disagrees_with_dump AS
SELECT ipdb_id, field, census_value, dump_value
FROM ipdb_stg.specialty_census_vs_dump;

INSERT INTO checks.warnings
SELECT 'ipdb_specialty_census_disagrees_with_dump', count(*)
FROM checks.ipdb_specialty_census_disagrees_with_dump;

-- A specialty an archived page states that the census does not, or the reverse,
-- for a model both describe.
--
-- The one question the census cannot answer alone: what IPDB used to say. The
-- archive pages no longer feed `ipdb.model_specialties` -- the census replaced
-- them outright -- and this is the reason to keep them staged anyway. Each row
-- is IPDB having RECLASSIFIED a machine between the capture and the download,
-- which is a fact about the source's own revisions and cannot be recovered once
-- the capture is dropped.
--
-- Restricted to models the archive actually covers, because everywhere else the
-- absence is the archive's silence rather than a removal.
CREATE OR REPLACE VIEW checks.ipdb_specialty_reclassified AS
WITH covered AS (SELECT DISTINCT ipdb_id FROM ipdb_stg.archive_models)
SELECT
  coalesce(a.ipdb_id, c.ipdb_id) AS ipdb_id,
  coalesce(a.specialty, c.specialty) AS specialty,
  CASE WHEN a.specialty IS NULL THEN 'added since capture' ELSE 'dropped since capture' END AS change,
  a.archive_capture_date
FROM ipdb_stg.archive_model_specialties AS a
FULL OUTER JOIN (
  SELECT ms.ipdb_id, ms.specialty
  FROM ipdb_stg.model_specialties AS ms
  WHERE ms.ipdb_id IN (SELECT ipdb_id FROM covered)
) AS c
  ON c.ipdb_id = a.ipdb_id AND c.specialty = a.specialty
WHERE a.specialty IS NULL OR c.specialty IS NULL;

INSERT INTO checks.warnings
SELECT 'ipdb_specialty_reclassified', count(*) FROM checks.ipdb_specialty_reclassified;

-- A model whose header-line date this build TYPED one way and IPDB's own listing
-- marks the other.
--
-- IPDB prints a `*` beside a date it is stating as a Project Date, and the
-- census carries that mark. `additional_details_date_kind` reaches the same
-- question by inference from the dump, on models no archive page confirms. Where
-- both speak, IPDB's mark is evidence and the inference is not, so a
-- disagreement is the inference being wrong.
--
-- Nothing is rewired on the strength of this: `ipdb_stg.models` still types the
-- date the way it did. The rows are here to be read before that changes.
CREATE OR REPLACE VIEW checks.ipdb_specialty_census_date_kind_disagrees AS
SELECT
  m.IpdbId, m.Title, m.AdditionalDetails,
  m.additional_details_date_kind AS inferred_kind,
  CASE WHEN c.date_is_project_date THEN 'project' ELSE 'manufacture' END AS census_mark
FROM ipdb_stg.models AS m
INNER JOIN ipdb_stg.specialty_census AS c ON c.ipdb_id = m.IpdbId
WHERE c.date_year IS NOT NULL
  AND m.additional_details_date_kind IS NOT NULL
  AND c.date_is_project_date <> (m.additional_details_date_kind LIKE 'project%');

INSERT INTO checks.warnings
SELECT 'ipdb_specialty_census_date_kind_disagrees', count(*)
FROM checks.ipdb_specialty_census_date_kind_disagrees;

-- Models whose header-line date is being read as a project date on inference
-- rather than on evidence -- `additional_details_date_kind = 'project_inferred'`.
-- Why that inference is not evidence is on the CASE in `ipdb_stg.models`.
--
-- A FETCH WORKLIST: every row is a model no archive page has confirmed, and
-- fetching its page either confirms the project date or turns up the manufacture
-- date the dump dropped. Ordered by manufacturer, which is how to find the
-- tractable clusters -- IPDB names the makers that routinely hold both dates,
-- and those are where a listing in this state is project-only.
CREATE OR REPLACE VIEW checks.ipdb_archive_header_date_inferred AS
SELECT IpdbId, Title, Manufacturer, AdditionalDetails
FROM ipdb_stg.models
WHERE additional_details_date_kind = 'project_inferred';

INSERT INTO checks.warnings
SELECT 'ipdb_archive_header_date_inferred', count(*) FROM checks.ipdb_archive_header_date_inferred;

-- A date segment matched the grammar but no month name recognised it, so the
-- year/month/day columns are NULL while the string is present. Signals a date
-- format IPDB has started using that try_strptime does not cover.
CREATE OR REPLACE VIEW checks.ipdb_additional_details_date_unrecognised AS
SELECT IpdbId, additional_details_date_string
FROM ipdb_stg.model_additional_details
WHERE additional_details_date_string IS NOT NULL
  AND additional_details_date_year IS NULL;

INSERT INTO checks.warnings
SELECT 'ipdb_additional_details_date_unrecognised', count(*) FROM checks.ipdb_additional_details_date_unrecognised;

------------------------------------------------------------
-- OPDB changelog vs the export
------------------------------------------------------------

-- A `move` whose replacement is neither in the export nor retired in turn, so
-- following it lands on an id nothing accounts for.
--
-- Per LINK, which covers a chain without recursing over one: OPDB moves an id
-- more than once, and a chain that ends nowhere ends at a link this fires on. It
-- names the broken link rather than the retired id a caller started from, which
-- is the more useful of the two to go and look at anyway.
--
-- A warning rather than an error because the two artifacts are downloaded
-- separately: a replacement created after the export was taken has nowhere to be
-- yet. A row here means re-download the export.
CREATE OR REPLACE VIEW checks.opdb_changelog_replacement_unresolved AS
SELECT c.changelogId, c.opdbIdDeleted, c.opdbIdReplacement, c.createdAt
FROM opdb_raw.changelog AS c
WHERE c.action = 'move'
  AND NOT EXISTS (SELECT 1 FROM opdb_stg.machines AS m WHERE m.opdb_id = c.opdbIdReplacement)
  AND NOT EXISTS (SELECT 1 FROM opdb_raw.changelog AS c2 WHERE c2.opdbIdDeleted = c.opdbIdReplacement);

INSERT INTO checks.warnings
SELECT 'opdb_changelog_replacement_unresolved', count(*) FROM checks.opdb_changelog_replacement_unresolved;

-- Ids the changelog has retired that the export still lists -- the size of the
-- gap between two artifacts downloaded at different times. Only a row PREDATING
-- the export is a fault, and the export carries no date to test that against
-- (see `ingest.watermarks`).
CREATE OR REPLACE VIEW checks.opdb_changelog_retired_id_still_in_export AS
SELECT c.changelogId, c.action, c.opdbIdDeleted, c.createdAt
FROM opdb_raw.changelog AS c
WHERE EXISTS (SELECT 1 FROM opdb_stg.machines AS m WHERE m.opdb_id = c.opdbIdDeleted);

INSERT INTO checks.warnings
SELECT 'opdb_changelog_retired_id_still_in_export', count(*) FROM checks.opdb_changelog_retired_id_still_in_export;

-- One OPDB manufacturer id carrying more than one name or full name. OPDB ships
-- no manufacturer list -- the names ride on each machine -- so a second spelling
-- publishes the id twice in `opdb.manufacturers` and doubles anything counted
-- through it. Upstream's to fix, hence a warning; watched because a duplicated
-- id reads as two companies to anyone joining on it.
CREATE OR REPLACE VIEW checks.opdb_manufacturer_ambiguous AS
SELECT opdb_manufacturer_id,
       list(DISTINCT manufacturer_name) AS names,
       list(DISTINCT manufacturer_full_name) AS full_names
FROM opdb_stg.machines
WHERE is_model AND opdb_manufacturer_id IS NOT NULL
GROUP BY opdb_manufacturer_id
HAVING count(DISTINCT manufacturer_name) > 1
    OR count(DISTINCT manufacturer_full_name) > 1;

INSERT INTO checks.warnings
SELECT 'opdb_manufacturer_ambiguous', count(*) FROM checks.opdb_manufacturer_ambiguous;

-- Two images marked primary for the same machine and type, which makes "the
-- backglass" ambiguous. Upstream's to fix, hence a warning -- but watched here
-- because it breaks a consumer silently: a query picking `is_primary AND
-- image_type = 'backglass'` starts returning two rows where it returned one.
CREATE OR REPLACE VIEW checks.opdb_multiple_primary_images_for_type AS
SELECT m.opdb_id, m."name", img."type" AS image_type, count(*) AS n_primary
FROM opdb_stg.machines AS m, unnest(m.images) AS t(img)
WHERE img."primary"
GROUP BY 1, 2, 3 HAVING count(*) > 1;

INSERT INTO checks.warnings
SELECT 'opdb_multiple_primary_images_for_type', count(*) FROM checks.opdb_multiple_primary_images_for_type;

-- An alias whose manufacturers could not be compared, so the rule returned no
-- verdict and `variant_of` is NULL for want of an answer rather than by
-- judgement. Empty in every export so far; watched because the silent
-- alternative is treating an unknown manufacturer as a match.
CREATE OR REPLACE VIEW checks.opdb_variant_parent_relation_undetermined AS
SELECT l.opdb_id, m."name", m.manufacturer_name, l.variant_parent_id
FROM opdb_stg.alias_lineage AS l
JOIN opdb_stg.machines AS m ON m.opdb_id = l.opdb_id
WHERE l.variant_parent_relation IS NULL;

INSERT INTO checks.warnings
SELECT 'opdb_variant_parent_relation_undetermined', count(*)
FROM checks.opdb_variant_parent_relation_undetermined;

-- The acquisition log no longer matches its artifact, either way: a new download
-- landed and nobody updated `ref.artifact_acquisitions` (`acquired_on` is now
-- confidently wrong -- worse than the NULL it replaced; the record count is the
-- tripwire, since a fresh dump almost always changes it), or the log names an
-- artifact the watermarks no longer carry (a rename orphaned the row, and the
-- real artifact silently reverted to an unrecorded acquisition). Update date and
-- count together; fix or remove orphaned rows.
CREATE OR REPLACE VIEW checks.artifact_acquisition_log_stale AS
SELECT a.artifact, a.acquired_on, a.n_records_at_acquisition, w.n_records
FROM ref.artifact_acquisitions AS a
LEFT JOIN ingest.watermarks AS w USING (artifact)
WHERE w.n_records IS DISTINCT FROM a.n_records_at_acquisition;

INSERT INTO checks.warnings
SELECT 'artifact_acquisition_log_stale', count(*)
FROM checks.artifact_acquisition_log_stale;
