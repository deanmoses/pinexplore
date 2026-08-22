-- Soft warnings (data quality, not hard build-stopping violations).
-- Creates and fills `checks.warnings`; no later layer adds to it.
-- All warnings are printed at the end of the build by `90_print_warnings.sql`.
--
-- Most of these watch a hand-maintained exception list for going stale, which it
-- does silently by construction: a rule that stops matching suppresses nothing,
-- and nothing raises.

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
INSERT INTO checks.warnings
SELECT 'ipdb_records_carried_forward', count(*)
FROM ipdb_stg.models_merged
WHERE carried_forward;

-- A retraction that the newest snapshot contradicts: IPDB is serving the record
-- again, and ipdb_ref.retracted is now suppressing a live model. Stale
-- exceptions fail silently by construction -- the row simply vanishes from
-- `ipdb_stg.models_merged` -- so the contradiction has to be checked for.
INSERT INTO checks.warnings
SELECT 'ipdb_retraction_contradicted', count(*)
FROM ipdb_ref.retracted AS r
WHERE EXISTS (
  SELECT 1 FROM ipdb_raw.xantari_model_snapshots AS s
  WHERE s.IpdbId = r.ipdb_id
    AND s.snapshot_utc = (SELECT max(snapshot_utc) FROM ipdb_raw.xantari_model_snapshots)
);

-- One IPDB manufacturer id carrying more than one manufacturer string. The id is
-- the join key every maker lookup uses, so a second string behind it multiplies
-- `ipdb_stg.corporate_entities` and every count taken through it.
--
-- Warns rather than aborting because the cause is upstream: either IPDB now
-- issues the id for two companies, or the dump misparsed a record onto it. A new
-- row here is usually a page to read and a row for
-- `ipdb_ref.model_corporate_entity_misparsed`.
-- Details: SELECT ManufacturerId, list(DISTINCT Manufacturer) FROM ipdb_stg.models
--   WHERE ManufacturerId IS NOT NULL GROUP BY 1 HAVING count(DISTINCT Manufacturer) > 1
INSERT INTO checks.warnings
SELECT 'ipdb_corporate_entity_ambiguous', count(*)
FROM (
  SELECT ManufacturerId
  FROM ipdb_stg.models
  WHERE ManufacturerId IS NOT NULL
  GROUP BY ManufacturerId
  HAVING count(DISTINCT Manufacturer) > 1
);

-- A duplicate-listing pair the newest snapshot no longer supports. Nothing
-- downstream raises when one of the two stops existing or matching -- a consumer
-- simply keeps suppressing a gap that is now real.
--
-- Two ways to go stale: an id vanishes, collapsing the pair, or the titles stop
-- agreeing, which means IPDB re-pointed an id at a different model and is more
-- dangerous because the pair still looks intact. Checked against the merged view
-- rather than the newest snapshot, so absence means IPDB dropped it rather than
-- that one crawl missed it.
-- Details: SELECT * FROM ipdb_ref.duplicate_listings d
--   LEFT JOIN ipdb_stg.models_merged a ON a.IpdbId = d.ipdb_id
--   LEFT JOIN ipdb_stg.models_merged b ON b.IpdbId = d.duplicate_of_ipdb_id
INSERT INTO checks.warnings
SELECT 'ipdb_duplicate_listing_unconfirmed', count(*)
FROM ipdb_ref.duplicate_listings AS d
LEFT JOIN ipdb_stg.models_merged AS a ON a.IpdbId = d.ipdb_id
LEFT JOIN ipdb_stg.models_merged AS b ON b.IpdbId = d.duplicate_of_ipdb_id
WHERE a.IpdbId IS NULL
   OR b.IpdbId IS NULL
   OR a.Title IS DISTINCT FROM b.Title;

-- The IPDB header line no longer matches the grammar parsed in
-- `ipdb_stg.model_additional_details`. Warns rather than aborting: the shape of
-- this string is xantari's to change, and a reformat upstream should not stop a
-- build. It does mean the parsed date columns are silently empty for those rows.
-- Details: SELECT IpdbId, AdditionalDetails FROM ipdb_stg.models_merged i
--   WHERE NOT EXISTS (SELECT 1 FROM ipdb_stg.model_additional_details ad
--     WHERE ad.IpdbId = i.IpdbId AND ad.additional_details_ipd_no IS NOT NULL)
INSERT INTO checks.warnings
SELECT 'ipdb_additional_details_unparsed', count(*)
FROM ipdb_stg.models_merged AS i
JOIN ipdb_stg.model_additional_details AS ad ON ad.IpdbId = i.IpdbId
WHERE i.AdditionalDetails IS NOT NULL
  AND ad.additional_details_ipd_no IS NULL;

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
-- Details: SELECT am.ipdb_id, am."name", am.archive_capture_date
--   FROM ipdb_raw.archive_models am
--   WHERE NOT EXISTS (SELECT 1 FROM ipdb_raw.xantari_model_snapshots s WHERE s.IpdbId = am.ipdb_id)
--     AND NOT EXISTS (SELECT 1 FROM ipdb_ref.retracted r WHERE r.ipdb_id = am.ipdb_id)
INSERT INTO checks.warnings
SELECT 'ipdb_archive_model_not_in_dump', count(*)
FROM ipdb_raw.archive_models AS am
WHERE NOT EXISTS (
    SELECT 1 FROM ipdb_raw.xantari_model_snapshots AS s WHERE s.IpdbId = am.ipdb_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM ipdb_ref.retracted AS r WHERE r.ipdb_id = am.ipdb_id
  );

-- A credit label on an archive page that `ipdb_ref.credit_role.archive_label`
-- does not list, so `ipdb_stg.credits` dropped it.
--
-- The parser finds credits by the "... by" SHAPE of a page label rather than
-- from a fixed list, so IPDB can hand us a role nobody has seen and the parse
-- will happily carry it. Without this the row would simply not join and would
-- vanish. A non-zero count on the first build after a change here means the
-- `archive_label` lists are wrong, not that IPDB invented a role.
-- Details: SELECT DISTINCT c.role FROM ipdb_stg.archive_models am, unnest(am.credits) t(c)
--   WHERE NOT EXISTS (SELECT 1 FROM ipdb_ref.credit_role cr WHERE list_contains(cr.archive_label, c.role))
INSERT INTO checks.warnings
SELECT 'ipdb_archive_credit_role_unrecognised', count(*)
FROM (
  SELECT DISTINCT c.role
  FROM ipdb_stg.archive_models AS am, unnest(am.credits) AS t(c)
  WHERE NOT EXISTS (
    SELECT 1 FROM ipdb_ref.credit_role AS cr WHERE list_contains(cr.archive_label, c.role)
  )
);

-- Models whose header-line date is being read as a project date on inference
-- rather than on evidence -- `additional_details_date_kind = 'project_inferred'`.
-- Why that inference is not evidence is on the CASE in `ipdb_stg.models`.
--
-- A FETCH WORKLIST: every row is a model no archive page has confirmed, and
-- fetching its page either confirms the project date or turns up the manufacture
-- date the dump dropped. The Details query orders by manufacturer, which is how
-- to find the tractable clusters -- IPDB names the makers that routinely hold
-- both dates, and those are where a listing in this state is project-only.
-- Details: SELECT ipdb_id, title, corporate_entity_text FROM ipdb.models
--   WHERE additional_details_date_kind = 'project_inferred' ORDER BY corporate_entity_text
INSERT INTO checks.warnings
SELECT 'ipdb_archive_header_date_inferred', count(*)
FROM ipdb_stg.models
WHERE additional_details_date_kind = 'project_inferred';

-- A date segment matched the grammar but no month name recognised it, so the
-- year/month/day columns are NULL while the string is present. Signals a date
-- format IPDB has started using that try_strptime does not cover.
INSERT INTO checks.warnings
SELECT 'ipdb_additional_details_date_unrecognised', count(*)
FROM ipdb_stg.model_additional_details
WHERE additional_details_date_string IS NOT NULL
  AND additional_details_date_year IS NULL;

------------------------------------------------------------
-- OPDB changelog vs the export
------------------------------------------------------------

-- A `move` whose replacement id is in neither the export nor a later changelog
-- row -- so following the move lands nowhere and the machine is unreachable
-- under either id.
--
-- Expected to be ZERO but not made an error, because the two artifacts are
-- downloaded separately: a replacement created after the export was taken has
-- nowhere to be yet. A row here means "re-download the export", not "the data is
-- wrong". Chased one hop, since OPDB moves an id more than once.
-- Details: SELECT * FROM opdb.changelog c WHERE c.action = 'move' AND NOT EXISTS
--   (SELECT 1 FROM opdb.machines m WHERE m.opdb_id = c.opdb_id_replacement)
--   AND NOT EXISTS (SELECT 1 FROM opdb.changelog c2 WHERE c2.opdb_id_deleted = c.opdb_id_replacement)
INSERT INTO checks.warnings
SELECT 'opdb_changelog_replacement_unresolved', count(*)
FROM opdb_raw.changelog AS c
WHERE c.action = 'move'
  AND NOT EXISTS (SELECT 1 FROM opdb_stg.machines AS m WHERE m.opdbId = c.opdbIdReplacement)
  AND NOT EXISTS (SELECT 1 FROM opdb_raw.changelog AS c2 WHERE c2.opdbIdDeleted = c.opdbIdReplacement);

-- Ids the changelog has retired that the export still lists.
--
-- Normal, and it is the changelog being AHEAD rather than the export being
-- stale: the changelog is downloaded after the export and its newest rows retire
-- ids the export was taken too early to have dropped. The number is the size of
-- that gap. Only a row whose `created_at` PREDATES the export is a real fault,
-- and telling those apart needs an export date the file does not carry -- see
-- `ingest.watermarks`.
INSERT INTO checks.warnings
SELECT 'opdb_changelog_retired_id_still_in_export', count(*)
FROM opdb_raw.changelog AS c
WHERE EXISTS (SELECT 1 FROM opdb_stg.machines AS m WHERE m.opdbId = c.opdbIdDeleted);

-- More than one image marked primary for the same machine AND type, which makes
-- "the backglass" ambiguous and leaves any single-image pick arbitrary.
--
-- Upstream's to fix, not ours, hence a warning. Currently zero -- which is the
-- reason to watch it: a query written against today's data picks
-- `is_primary AND image_type = 'backglass'` and quietly starts returning two.
-- Details: SELECT opdb_id, image_type, count(*) FROM opdb.machine_images
--   WHERE is_primary GROUP BY 1, 2 HAVING count(*) > 1
INSERT INTO checks.warnings
SELECT 'opdb_multiple_primary_images_for_type', count(*)
FROM (
  SELECT m.opdbId, img."type"
  FROM opdb_stg.machines AS m, unnest(m.images) AS t(img)
  WHERE img."primary"
  GROUP BY 1, 2 HAVING count(*) > 1
);

