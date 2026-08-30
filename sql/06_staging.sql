-- Per-source staging: merging, parsing and correcting each dump.
--
-- No cross-source joins.

------------------------------------------------------------
-- OPDB staged
------------------------------------------------------------

-- OPDB's machines and aliases in one relation, in our spelling.
--
-- An alias is a specific edition of a machine, keyed by a third id segment;
-- splitting them across two arrays is a fact about the export format, not about
-- the machines. `UNION ALL BY NAME` rather than a column list, because the two
-- arrays disagree on their column sets and a list would drop whichever field
-- OPDB adds next.
--
-- OPDB encodes the hierarchy INTO the id: `G50L9-MDxXD` is machine `MDxXD` of
-- group `G50L9`, and a third segment makes it an alias. Split here so every join
-- through it takes the parts the same way.
--
-- The RENAME spells OPDB's camelCase our way and the star carries a field it
-- gains upstream through to the mart, where `opdb_column_not_snake_case` fails
-- the build until it is named here.
--
-- The columns below the star are the ones a rename cannot express:
--
--   * both flags forced to real booleans. Upstream states only the true one on
--     each row, so a straight union leaves the other NULL and `WHERE NOT
--     is_alias` -- the obvious way to write the obvious filter -- matches nothing.
--   * OPDB says "nothing" as both NULL and `''` on the same text column, in
--     numbers that dwarf the real values, so a consumer writing the obvious `IS
--     NULL` gets the wrong answer.
--   * `physical_machine` is OPDB's 0/1 flag, read as the boolean it is.
--   * `manufacturer` flattened: no RENAME reaches inside a struct, and its keys
--     are camelCase too.
--
-- DO NOT compare `manufacture_date` below year precision. OPDB stores a full
-- DATE but knows a full date for few machines, so it pads what it does not know
-- to `01` -- most rows land on day 1, many on January 1st. Comparing month or
-- day against a catalog date reports the padding as disagreement.
CREATE OR REPLACE TABLE opdb_stg.machines AS
SELECT
  u.* EXCLUDE (manufacturer, commonName, shortname, description, physicalMachine, isMachine, isAlias)
      RENAME (
        opdbId          AS opdb_id,
        ipdbId          AS ipdb_id,
        manufactureDate AS manufacture_date,
        playerCount     AS player_count,
        createdAt       AS created_at,
        updatedAt       AS updated_at
      ),
  coalesce(u.isMachine, false)        AS is_machine,
  coalesce(u.isAlias, false)          AS is_alias,
  nullif(u.commonName, '')            AS common_name,
  nullif(u.shortname, '')             AS shortname,
  nullif(u.description, '')           AS description,
  CAST(u.physicalMachine AS BOOLEAN)  AS physical_machine,
  u.manufacturer.manufacturerId       AS opdb_manufacturer_id,
  u.manufacturer.name                 AS manufacturer_name,
  u.manufacturer.fullName             AS manufacturer_full_name,
  tg.slug                             AS technology_generation_slug,
  dt.slug                             AS display_type_slug,
  -- Flipcommons stores a year plus an OPTIONAL month, which is exactly the
  -- precision OPDB really has. It pads what it does not know to `01`, so a
  -- January on the 1st is padding or a real January and nothing distinguishes
  -- them -- dropped. A January on any other day is stated, and kept: padding
  -- can only ever produce the 1st.
  CAST(year(u.manufactureDate) AS SMALLINT) AS production_year,
  CASE WHEN month(u.manufactureDate) = 1 AND day(u.manufactureDate) = 1 THEN NULL
       ELSE CAST(month(u.manufactureDate) AS TINYINT) END AS production_month,
  -- OPDB says a model is a remake and never says what OF, so this is a flag and
  -- not a pointer. Filling `remake_of` would mean guessing at the title's oldest
  -- non-remake, which is wrong wherever OPDB files a remake in its own group --
  -- Metallica Remastered is a separate group from Metallica.
  --
  -- Read through `opdb_ref.feature` on OUR slug rather than matching OPDB's
  -- `Remake` inline. That table is the single statement of where each OPDB
  -- feature goes, and a lineage flag that restates upstream's string can drift
  -- from it silently; keyed this way the two cannot disagree, and the only
  -- literal left is a slug we control. `EXISTS` also spares this the
  -- `list_contains` NULL trap the flags below document.
  EXISTS (SELECT 1 FROM opdb_ref.feature AS f
          WHERE f.target_value = 'is_remake'
            AND list_contains(u.features, f.opdb_feature)) AS is_remake,
  -- Whether this row becomes a Flipcommons Model. OPDB's non-physical rows are
  -- containers holding a set of gameplay-identical machines and correspond to
  -- nothing in the catalog. Defined ONCE here because every `opdb.model_*` view
  -- needs it: spelled out at each consumer, the one that forgets publishes rows
  -- keyed to an id `opdb.models` does not contain, and the join just returns
  -- nothing.
  is_alias OR physical_machine                                     AS is_model,
  split_part(u.opdbId, '-', 1)        AS group_id,
  split_part(u.opdbId, '-', 2)        AS machine_id,
  nullif(split_part(u.opdbId, '-', 3), '') AS alias_id
FROM (
  SELECT m.* REPLACE (true AS isMachine), false AS isAlias FROM opdb_raw.machines AS m
  UNION ALL BY NAME
  SELECT a.* REPLACE (true AS isAlias), false AS isMachine FROM opdb_raw.aliases AS a
) AS u
LEFT JOIN opdb_ref.technology_generation AS tg ON u."type" = tg.opdb_type
LEFT JOIN opdb_ref.display_type AS dt ON u.display = dt.opdb_display;

-- OPDB's machine groups, in our spelling.
--
-- `description` is cast because it arrives typed JSON: it is empty on every row
-- of the export, which is what `read_json_auto` infers from when it has no value
-- to go on, and a predicate against a JSON column raises rather than returning
-- false. The cast is a no-op the day OPDB writes a string into it.
--
-- `year` is a string upstream. `TRY_CAST` rather than CAST so a year OPDB
-- malforms is reported by `opdb_group_year_not_a_number` instead of crashing the
-- layer; the check is what stops it being silently NULL.
CREATE OR REPLACE VIEW opdb_stg.machine_groups AS
SELECT
  g.* EXCLUDE (shortname, description, year)
      RENAME (
        opdbId           AS opdb_id,
        isMachineGroup   AS is_machine_group,
        pinballPrimerUrl AS pinball_primer_url,
        pinballCardsUrl  AS pinball_cards_url,
        bobsGuideUrl     AS bobs_guide_url,
        pinballRulesUrl  AS pinball_rules_url,
        createdAt        AS created_at,
        updatedAt        AS updated_at
      ),
  nullif(g.shortname, '')                    AS shortname,
  nullif(CAST(g.description AS VARCHAR), '') AS description,
  TRY_CAST(g.year AS SMALLINT)               AS year
FROM opdb_raw.machine_groups AS g;

-- One row per model per coded value, OPDB's wording beside its decode. The mart
-- splits these by `target_entity_type` into a view or column per Flipcommons
-- entity; nothing downstream of the mart sees the discriminator.
--
-- INNER joins, so the two `_unmapped` checks are what stop a value OPDB invents
-- from being dropped here in silence.
CREATE OR REPLACE VIEW opdb_stg.model_features AS
SELECT m.opdb_id, m."name", t.feature, f.target_entity_type, f.target_value
FROM opdb_stg.machines AS m, unnest(m.features) AS t(feature)
JOIN opdb_ref.feature AS f ON f.opdb_feature = t.feature
WHERE m.is_model;

-- `target_entity_type IS NOT NULL` drops the deliberate `no-target` verdicts.
-- They are decoded, just to nothing, and carrying them further would put rows in
-- the mart that name no catalog fact.
CREATE OR REPLACE VIEW opdb_stg.model_keywords AS
SELECT m.opdb_id, m."name", t.keyword, k.target_entity_type, k.target_value
FROM opdb_stg.machines AS m, unnest(m.keywords) AS t(keyword)
JOIN opdb_ref.keyword AS k ON k.opdb_keyword = t.keyword
WHERE m.is_model AND k.target_entity_type IS NOT NULL;

-- What each OPDB alias IS to the machine it hangs off, in Flipcommons terms.
--
-- OPDB states THAT two machines are related and WHICH one is the parent. It
-- never states WHAT the relation is. Its alias tree means "these play
-- identically", and Flipcommons splits that single idea across `variant_of`, a
-- `copy` edge, `conversion` / `conversion_kit`, `retheme`, `remake_of` and
-- `export_edition_of`. Publishing the parent as `variant_of` asserts a choice
-- among those that OPDB did not make.
--
-- MANUFACTURER IS THE DISCRIMINATOR. A same-manufacturer alias is an edition of
-- one company's own machine. A cross-manufacturer alias is the licensed-copy
-- shape -- Cavaleiro Negro by Taito do Brasil against Williams' Black Knight --
-- which Flipcommons carries as a `copy` edge and never as a variant.
--
-- `variant_parent_relation`:
--
--   conversion          OPDB tags it a conversion. A `ModelRelationship` edge and
--                       not a variant: it files Challenger V as an alias of Star
--                       Trek where Flipcommons holds it as a conversion OF it.
--   cross_manufacturer  The two disagree on manufacturer. Deliberately NOT named
--                       for a Flipcommons relationship type: measured against the
--                       catalog these carry a `copy` edge on fewer than half the
--                       rows, and `retheme`, `export_edition_of`, `remake_of` or
--                       nothing at all on the rest. Two of them hold a `copy` edge
--                       against a DIFFERENT model than the one OPDB names here, so
--                       a borrowed type would not merely be unconfirmed -- it
--                       would mispoint. This states what the test found and stops.
--   variant             Same manufacturer. The only verdict that fills `variant_of`.
--   NULL                A manufacturer is missing, so the test could not be run.
--                       No row in the export exercises it; asserting nothing beats
--                       falling through to `variant`.
--
-- EXPORT IS NOT A VERDICT, it is an additional fact. `export_edition_of` is its
-- own scalar FK and is filled independently, so an export edition still gets
-- whatever verdict its manufacturers earn it. That is what the catalog does:
-- OPDB's export aliases are almost all cross-manufacturer and hold
-- `export_edition_of` alone, while the same-manufacturer one holds `variant_of`
-- alone. Making export outrank the manufacturer test would get the second wrong
-- to no benefit, since the FK does not come from the verdict.
--
-- TWO KINDS OF CLAIM, TWO COLUMNS. This is the shape that matters here, and the
-- reason a single `variant_of` with a discriminator beside it was worth undoing.
--
-- `variant_of` is a DECODE and nothing else: OPDB named a real machine as the
-- parent and the manufacturers matched. Nothing derived, nothing elected.
--
-- `sibling_set_primary_id` is a PROPOSAL. OPDB hangs many aliases off a
-- NON-PHYSICAL container -- not a machine but a holder it invents for a set of
-- gameplay-identical ones. Godzilla Premium, LE and 70th all hang off "Godzilla
-- (Premium/LE)", a row no factory built. Flipcommons has no container: it
-- elevates the broadest member and points the rest at it. So a primary is
-- ELECTED, by `opdb_ref.edition_rank` with earliest manufacture date breaking a
-- tie, and published as the proposal it is rather than as `variant_of`. Godzilla
-- comes out right: LE and 70th both onto Premium.
--
--   = opdb_id   this model IS the elected primary -- a variant of nothing
--   <> opdb_id  the proposed `variant_of`
--   NULL        no election -- see below
--
-- READ IT WITH THE OTHER TWO LINEAGE COLUMNS. NULL here covers THREE different
-- situations, and neither flag separates them alone:
--
--   variant_parent_is_model      an alias of a real machine. There was never a
--                                set to elect within.
--   NOT is_model, relation
--     <> 'conversion'            a container member nobody could call. THE
--                                WORKLIST.
--   NOT is_model, relation
--      = 'conversion'            a conversion filed inside a container. Dropped
--                                before ranking, so it never competed -- which
--                                is what keeps it OUT of that worklist. Cactus
--                                Canyon (Lyman Upgrade) is the one such row.
--
-- A consumer filtering on NULL alone gets all three.
--
-- A container is undecidable when either
--
--   * two members tie at the top -- a CE and an LE, nothing broader than either, or
--   * any member carries no edition tag. An untagged member cannot be placed on
--     the ladder, and it is usually the BROAD one, so ranking it last produces
--     the answer exactly backwards. Cactus Canyon Remake Special is untagged
--     beside a Remake LE, and Flipcommons holds those two as siblings with no
--     variant link at all.
--
-- `variant_parent_id` is OPDB's own answer and is always set: the id the alias
-- hangs off, container or machine. It is the sibling-set key, and it is what
-- remains to compare on where no verdict and no election survive.
CREATE OR REPLACE VIEW opdb_stg.alias_lineage AS
WITH alias AS (
  SELECT
    a.opdb_id,
    a.group_id || '-' || a.machine_id AS variant_parent_id,
    -- The parent is a real machine rather than one of OPDB's containers.
    -- `coalesce` because a container's own `physical_machine` is false and an
    -- alias's is NULL, and only the first of those is a parent to point at.
    coalesce(p.physical_machine, false) AS parent_is_real,
    -- Both flags read through `opdb_ref.feature` on OUR slug and entity type, so
    -- editing the decode cannot leave a lineage read behind. `Converted game`
    -- and `Conversion kit` are BOTH conversions and naming either inline is how
    -- the other gets missed. `EXISTS` rather than `list_contains` directly: it
    -- yields false on a NULL `features` list where `list_contains` yields NULL,
    -- and a NULL here would silently drop `variant_of` from the row.
    EXISTS (SELECT 1 FROM opdb_ref.feature AS f
            WHERE f.target_value = 'export_edition_of'
              AND list_contains(a.features, f.opdb_feature)) AS is_export,
    EXISTS (SELECT 1 FROM opdb_ref.feature AS f
            WHERE f.target_entity_type = 'model-relationship'
              AND list_contains(a.features, f.opdb_feature)) AS is_conversion,
    -- NULL when either side is unknown, so the verdict below can decline to
    -- answer. `IS DISTINCT FROM` would call a missing manufacturer a difference.
    CASE WHEN a.opdb_manufacturer_id IS NULL OR p.opdb_manufacturer_id IS NULL THEN NULL
         ELSE a.opdb_manufacturer_id <> p.opdb_manufacturer_id END AS crosses_manufacturer,
    -- The broadest edition this row is tagged with, or NULL when it carries no
    -- edition tag. Deliberately NOT defaulted to a sentinel rank: NULL is the
    -- signal that its container cannot be decided at all.
    (SELECT min(er.breadth_rank) FROM opdb_ref.edition_rank AS er
     WHERE list_contains(a.features, er.opdb_feature)) AS breadth_rank,
    a.manufacture_date
  FROM opdb_stg.machines AS a
  JOIN opdb_stg.machines AS p ON p.opdb_id = a.group_id || '-' || a.machine_id
  WHERE a.is_alias
),
verdict AS (
  SELECT
    *,
    CASE WHEN is_conversion            THEN 'conversion'
         WHEN crosses_manufacturer     THEN 'cross_manufacturer'
         WHEN NOT crosses_manufacturer THEN 'variant'
         END AS variant_parent_relation
  FROM alias
),
-- Container members that compete to be primary. Conversions are dropped rather
-- than ranked, so a container holding one is still decidable on the rest.
contained AS (
  SELECT * FROM verdict WHERE NOT parent_is_real AND NOT is_conversion
),
ranked AS (
  SELECT
    *,
    -- `rank()` and not `row_number()`: a tie has to stay visibly a tie, where
    -- row_number would silently appoint whichever row arrived first.
    rank() OVER (PARTITION BY variant_parent_id
                 ORDER BY breadth_rank NULLS LAST, manufacture_date NULLS LAST) AS breadth_place,
    first_value(opdb_id) OVER (PARTITION BY variant_parent_id
                 ORDER BY breadth_rank NULLS LAST, manufacture_date NULLS LAST) AS elected_id,
    count(*) FILTER (breadth_rank IS NULL) OVER (PARTITION BY variant_parent_id) AS n_untagged
  FROM contained
),
decided AS (
  SELECT
    *,
    n_untagged = 0
      AND count(*) FILTER (breadth_place = 1) OVER (PARTITION BY variant_parent_id) = 1 AS is_decided
  FROM ranked
)

-- One row per alias. The election is a LEFT JOIN rather than a branch: it adds a
-- column to the rows it decided and leaves every other row alone, where the
-- previous three-branch union existed only to give container members a different
-- `variant_of` from stated ones -- which is exactly what they no longer get.
SELECT
  v.opdb_id,
  v.variant_parent_id,
  v.parent_is_real AS variant_parent_is_model,
  v.variant_parent_relation,
  CASE WHEN v.variant_parent_relation = 'variant' AND v.parent_is_real
       THEN v.variant_parent_id END                                  AS variant_of,
  CASE WHEN d.is_decided THEN d.elected_id END                       AS sibling_set_primary_id,
  CASE WHEN v.is_export     AND v.parent_is_real
       THEN v.variant_parent_id END                                  AS export_edition_of,
  CASE WHEN v.is_conversion AND v.parent_is_real
       THEN v.variant_parent_id END                                  AS conversion_donor_id
FROM verdict AS v
LEFT JOIN decided AS d ON d.opdb_id = v.opdb_id;

------------------------------------------------------------
-- IPDB staged
------------------------------------------------------------

-- One row per model: the newest snapshot that observed it wins, whole.
--
-- Record-level, never field-level. A snapshot is one atomic observation, so a
-- row mixing fields across snapshots would describe a record that never existed
-- upstream and could not be cited as evidence.
--
-- An older snapshot contributes only whole records the newest scrape missed, and
-- those are flagged rather than blended in: a carried-forward row predates the
-- encoding fix that landed between the 2025-02 and 2026-04 snapshots, so it can
-- still hold U+FFFD mojibake the fresh rows no longer have.
CREATE OR REPLACE TABLE ipdb_stg.models_merged AS
SELECT
  s.*,
  s.snapshot_utc < (SELECT max(snapshot_utc) FROM ipdb_raw.xantari_model_snapshots) AS carried_forward
FROM ipdb_raw.xantari_model_snapshots AS s
WHERE NOT EXISTS (SELECT 1 FROM ipdb_ref.retracted AS r WHERE r.ipdb_id = s.IpdbId)
QUALIFY row_number() OVER (PARTITION BY s.IpdbId ORDER BY s.snapshot_utc DESC) = 1;

-- The archive.org pages, flattened and cut down to the models xantari knows.
--
-- Drops every model xantari has never listed: xantari is authoritative on which
-- models EXIST, and holding a 2018 capture of a page is not evidence IPDB still
-- lists it. Restricting against `ipdb_stg.models_merged` rather than the raw
-- snapshots also inherits the retraction filter, so a deleted listing stays
-- deleted. `ipdb_archive_model_not_in_dump` reports what this drops.
--
-- The array columns stay unflattened. The views below unnest them, and reading
-- from here rather than raw is what gives them the same id restriction.
CREATE OR REPLACE VIEW ipdb_stg.archive_models AS
SELECT
  am.ipdb_id,

  -- IPDB's two LABELLED date rows. These are the whole point of this source:
  -- not the values, which xantari's header line already carries, but the fact
  -- that IPDB says which kind each one is. See `additional_details_date_kind`.
  am.manufacture_date.year  AS manufacture_date_year,
  am.project_date.year      AS project_date_year,

  am.production.units          AS production_units,
  am.production.status         AS production_status,
  am.production.never_produced AS production_never_produced,

  am.rating.score       AS rating_score,
  am.rating.ratings     AS rating_ratings,
  am.rating."comments"  AS rating_comments,
  am.rating.provisional AS rating_provisional,

  am.serial_number_database_url,
  am.owners_list_url,
  am.easter_eggs.text AS easter_eggs_text,
  am.easter_eggs.url  AS easter_eggs_url,

  am.themes,
  am.specialties,
  am.documents,
  am.credits,

  am.source_url AS archive_source_url,
  am.archive_capture_date
FROM ipdb_raw.archive_models AS am
WHERE EXISTS (
  SELECT 1 FROM ipdb_stg.models_merged AS mm WHERE mm.IpdbId = am.ipdb_id
);

-- Parse the IPDB page header line, which xantari captured verbatim into
-- `AdditionalDetails` without modelling its parts:
--
--     IPD No. 5755 / 2011 / 4 Players
--
-- The date segment is the reason to parse this. IPDB pages carry either a "Date
-- Of Manufacture" row or a "Project Date" row and the header renders whichever
-- exists; xantari scraped only the former. Where the header is the sole
-- surviving date, it is NOT a manufacture date -- spot-checking found both
-- project dates (Bally bingos, mostly) and dates the dump simply missed -- so
-- confirm against the IPDB page before using one.
--
-- Year/month/day are split rather than emitted as a DATE because the header
-- states three precisions ("March 21, 1961", "August, 1941", "1932"). A DATE
-- would pad the missing parts to 1 and read as a genuine January 1st.
--
-- Built over `ipdb_stg.models_merged`, not `ipdb_stg.models`: the latter reads
-- this view, and sourcing it there would close the cycle.
--
-- The grammar is strict, admitting four shapes (id alone; id + date;
-- id + players; all three). Strictness makes upstream drift visible rather than
-- mis-binding -- a permissive date group swallows "1 Player" as a date. Month
-- names go through try_strptime, so an unfamiliar one lands as a NULL year and
-- trips `ipdb_additional_details_date_unrecognised`.
CREATE OR REPLACE VIEW ipdb_stg.model_additional_details AS
WITH parsed AS (
  SELECT
    im.IpdbId,
    regexp_extract(
      im.AdditionalDetails,
      '^IPD No\. (\d+)(?: / ([A-Za-z]+ \d{1,2}, \d{4}|[A-Za-z]+, \d{4}|\d{4}))?(?: / (\d+) Players?)?$',
      ['ipd_no', 'date_text', 'players']
    ) AS g
  FROM ipdb_stg.models_merged AS im
),
typed AS (
  SELECT
    IpdbId,
    TRY_CAST(nullif(g.ipd_no, '') AS INTEGER)   AS additional_details_ipd_no,
    TRY_CAST(nullif(g.players, '') AS UTINYINT) AS additional_details_players,
    nullif(g.date_text, '')                     AS additional_details_date_string,
    try_strptime(nullif(g.date_text, ''), '%B %d, %Y') AS d_day,
    try_strptime(nullif(g.date_text, ''), '%B, %Y')    AS d_month,
    try_strptime(nullif(g.date_text, ''), '%Y')        AS d_year
  FROM parsed
)
SELECT
  IpdbId,
  -- Redundant with IpdbId and Players on every row today. Kept because that
  -- redundancy is the tripwire `ipdb_additional_details_parse_misaligned` reads:
  -- if the capture groups ever slip,
  -- these disagree and the date is wrong too.
  additional_details_ipd_no,
  additional_details_players,
  additional_details_date_string,
  CAST(year(COALESCE(d_day, d_month, d_year)) AS SMALLINT) AS additional_details_date_year,
  CAST(month(COALESCE(d_day, d_month)) AS UTINYINT)        AS additional_details_date_month,
  CAST(day(d_day) AS UTINYINT)                             AS additional_details_date_day
FROM typed;


-- Add technology generation slug and system/subgeneration via MPU match,
-- plus the parsed header date.
--
-- One row per IPDB model, no exclusions -- `ipdb_stg.models_merged` enriched,
-- with the counts asserted equal by `ipdb_stg_models_drops_rows`. A consumer
-- that cannot use an unknown manufacturer excludes it in its own WHERE.
-- Filtering here is silent: every downstream count inherits it, and gap analysis
-- loses the obscure records likeliest to be missing from the catalog.
CREATE OR REPLACE VIEW ipdb_stg.models AS
SELECT
  -- `ManufacturerShortName` is dropped because is xantari's own derivation, not a
  -- field on the IPDB page: on some records it is the trade name ("Gottlieb"), on
  -- more than half it is the entire `Manufacturer` string, years and trade-name
  -- bracket included. Which one you get tracks whether IPDB recorded a LOCATION.
  -- Counting it distinctly does not rescue it either: it
  -- varies per corporate entity, so one brand across two incarnations counts as two
  -- manufacturers. The brand is `ipdb_stg.corporate_entities.manufacturer_name`.
  -- `TypeShortName` goes for the same reason as `ManufacturerShortName` above:
  -- it is xantari's own derivation rather than a field on the IPDB page, and it
  -- is an incomplete one. It holds EM and SS and is EMPTY on every Pure
  -- Mechanical machine, though the dump's own `Type` says "Pure Mechanical (PM)"
  -- on all of them, and IPDB's page and advanced search both state the code
  -- plainly. `type_code` below replaces it, sliced from the field xantari
  -- actually scraped.
  im.* EXCLUDE (ManufacturerShortName, TypeShortName)
  -- REPLACE nulls both manufacturer fields for the models listed in
  -- `ipdb_ref.model_corporate_entity_misparsed`, so a consumer sees the absence
  -- IPDB's page shows rather than the company the dump invented.
  --
  -- This is staging's job rather than the mart's, and the distinction is worth
  -- keeping straight: staging corrects the DUMP against the SOURCE -- here, a
  -- name the scrape lifted out of prose that IPDB's own page denies -- while the
  -- mart translates the SOURCE's vocabulary into ours. Ids 0 and 328, IPDB's two
  -- ways of writing "no manufacturer", are faithfully scraped and are therefore
  -- the mart's to translate, not staging's to correct.
  --
  -- That list is written by hand, not detected. Recognising that a manufacturer
  -- name is a fragment of prose lifted out of the Notes means reading the page,
  -- so every row carries the URL it was read from and nothing lands there
  -- automatically.
       REPLACE (
         CASE WHEN misparsed.ipdb_id IS NULL THEN im.Manufacturer   END AS Manufacturer,
         CASE WHEN misparsed.ipdb_id IS NULL THEN im.ManufacturerId END AS ManufacturerId,
         -- The only two values taken from the archive pages, and both only
         -- where xantari is silent. A NAMED LIST, not a blanket coalesce: a
         -- xantari NULL does not mean IPDB is empty, it can equally mean IPDB
         -- withdrew the value after the capture, so every fill is a decision
         -- someone made rather than a rule that fires on absence.
         COALESCE(im.AverageFunRating, arc.rating_score)    AS AverageFunRating,
         COALESCE(im.ProductionNumber, arc.production_units) AS ProductionNumber
       ),

  -- IPDB's type code, from the parenthesis in `Type` rather than from the dump's
  -- blank-on-PM short name. `Type` has only ever held four values -- the three
  -- codes and NULL -- and `ipdb_type_code_underivable` fails the build if a fifth
  -- appears rather than letting it arrive as a silent NULL.
  --
  -- NULL, not '', where IPDB states no type at all: an empty string here would be
  -- the same value that used to mean "Pure Mechanical, unrecorded".
  nullif(regexp_extract(im."Type", '\(([A-Z]+)\)$', 1), '') AS type_code,

  tg1.slug AS technology_generation_slug,
  ad.additional_details_date_string,
  ad.additional_details_date_year,
  ad.additional_details_date_month,
  ad.additional_details_date_day,

  -- WHAT KIND OF DATE the header line is stating. The archive pages' entire
  -- contribution to dates, and it is a label, never a value: on every model
  -- they resolve, their date equals the header parse to the day.
  --
  -- IPDB changed how the header picks a date on 2018-04-26, announced at
  -- https://pinside.com/pinball/forum/topic/display-of-ipdb-project-dates-and-manufacture-dates
  -- Since then a listing holding both dates shows the MANUFACTURE date in the
  -- header (before, it showed the project date), and a listing holding one shows
  -- it in the body too, labelled. Both xantari snapshots post-date that change,
  -- so `DateOfManufacture` being present means the header is showing it.
  --
  -- Which is why the last branch is `project_inferred` and not `project`. By
  -- IPDB's rule a header date with no `DateOfManufacture` IS a project date --
  -- but that reasoning trusts xantari to have captured a manufacture date
  -- wherever IPDB states one, and archive pages show plain `Date Of Manufacture`
  -- rows that the dump simply dropped.
  -- The inference is sound about IPDB and unreliable about xantari, so the rows
  -- resting on it are marked and reported by
  -- `ipdb_archive_header_date_inferred` rather than mixed in with the observed
  -- ones.
  CASE
    WHEN ad.additional_details_date_year IS NULL       THEN NULL
    WHEN im.DateOfManufacture IS NOT NULL              THEN 'manufacture'
    WHEN arc.manufacture_date_year IS NOT NULL         THEN 'manufacture'
    WHEN arc.project_date_year IS NOT NULL             THEN 'project'
    ELSE 'project_inferred'
  END AS additional_details_date_kind,

  -- Which source the filled value above came from. Only these two fills exist,
  -- so only these two markers do.
  CASE WHEN im.AverageFunRating IS NOT NULL THEN 'xantari'
       WHEN arc.rating_score    IS NOT NULL THEN 'archive' END AS rating_src,
  CASE WHEN im.ProductionNumber IS NOT NULL THEN 'xantari'
       WHEN arc.production_units IS NOT NULL THEN 'archive' END AS production_src,

  -- Archive-only fields: xantari has no column for any of these, so there is
  -- nothing to coalesce against and no marker to carry -- their source is
  -- implied by their existing at all.
  --
  -- `production_never_produced` is the substantial one. IPDB records a
  -- never-produced design by writing a STATUS where a unit count would go, and
  -- the dump models only the count, so the status was invisible to us.
  arc.rating_ratings,
  arc.rating_comments,
  arc.rating_provisional,
  arc.production_status,
  arc.production_never_produced,
  arc.serial_number_database_url,
  arc.owners_list_url,
  arc.easter_eggs_text,
  arc.easter_eggs_url,

  -- What a patch cites when the claim rests on the archive page: the address,
  -- and the date the words on it were true.
  arc.archive_source_url,
  arc.archive_capture_date
FROM ipdb_stg.models_merged AS im
LEFT JOIN ipdb_ref.model_corporate_entity_misparsed AS misparsed
  ON misparsed.ipdb_id = im.IpdbId
LEFT JOIN ipdb_stg.model_additional_details AS ad
  ON ad.IpdbId = im.IpdbId
LEFT JOIN ipdb_stg.archive_models AS arc
  ON arc.ipdb_id = im.IpdbId
-- One join, not two. The second leg existed only to reach Pure Mechanical
-- through `Type` because the short name was blank there; the derived `type_code`
-- above carries PM like any other code, so the full-text key is gone.
LEFT JOIN ipdb_ref.technology_generation AS tg1
  ON tg1.type_code = nullif(regexp_extract(im."Type", '\(([A-Z]+)\)$', 1), '');

-- Distinct corporate entities parsed from IPDB manufacturer strings.
-- Splits the structured string into company name, trade name, years, location,
-- and HQ city/state/country (with US state detection and override handling).
--
-- Staging because `ipdb_stg.files` reads it to name a file's maker.
-- `ipdb.corporate_entities` is the published translation.
CREATE OR REPLACE VIEW ipdb_stg.corporate_entities AS
WITH raw_extractions AS (
  -- Run each regex on Manufacturer exactly once, named for what it produces.
  SELECT DISTINCT
    ManufacturerId        AS ipdb_manufacturer_id,
    Manufacturer          AS raw_name,
    regexp_replace(Manufacturer, '\s*\[Trade Name:.*?\]', '')                     AS _sans_trade,
    regexp_extract(Manufacturer, '\[Trade Name:\s*(.+?)\]', 1)                    AS trade_name,
    regexp_extract(Manufacturer, '\((\d{4})-', 1)                                 AS _year_start_raw,
    regexp_extract(Manufacturer, '\(\d{4}-(\d{4})\)', 1)                          AS _year_end_raw,
    regexp_extract(Manufacturer, '\((\d{4})\)', 1)                                AS _single_year_raw,
    regexp_extract(Manufacturer, ',\s*of\s+(.+?)(?:\s*\(\d|\s*\[Trade|\s*$)', 1) AS _location_raw
  FROM ipdb_stg.models
  WHERE Manufacturer IS NOT NULL
    AND ManufacturerId NOT IN (SELECT ipdb_manufacturer_id FROM ipdb_ref.corporate_entity_not_a_company)
),
parsed AS (
  SELECT
    ipdb_manufacturer_id,
    raw_name,
    trade_name,

    -- Company name: strip years and ", of ..." from the already-trade-stripped string
    trim(trailing ',' FROM trim(
      regexp_replace(
        regexp_replace(_sans_trade, '\s*\(\d+.*?\)', ''),
        ',\s*of\s+.*$', '')
    )) AS company_name,

    -- Year range: each pattern extracted once above, cast here
    CASE WHEN _year_start_raw  != '' THEN CAST(_year_start_raw  AS INTEGER) END AS year_start,
    CASE WHEN _year_end_raw    != '' THEN CAST(_year_end_raw    AS INTEGER) END AS year_end,
    CASE WHEN _single_year_raw != '' AND raw_name NOT LIKE '%-%(%'
         THEN CAST(_single_year_raw AS INTEGER) END AS single_year,

    -- The ", of ..." segment, as IPDB writes it. Left whole rather than split into
    -- city/state/country: telling "St. Paul, Minnesota" from "Paris, France" needs a
    -- vocabulary of US states, and the catalog holds one. Splitting it here without
    -- that vocabulary reads Minnesota as a country.
    COALESCE(trim(trailing ',' FROM _location_raw), '') AS location

  FROM raw_extractions
)
SELECT
  p.ipdb_manufacturer_id, p.raw_name,
  p.company_name, p.trade_name,
  -- IPDB states a trade name only where it differs from the company name, so the
  -- brand is the trade name when there is one and the company name otherwise.
  CASE WHEN p.trade_name != '' THEN p.trade_name ELSE p.company_name END AS manufacturer_name,
  p.year_start, p.year_end, p.single_year, p.location,
  model_years.year_of_first_model,
  model_years.year_of_last_model
FROM parsed p
LEFT JOIN (
  -- Date-kind-blind ON PURPOSE: this is the entity's ACTIVITY span, and a project year
  -- is real activity, so it counts alongside manufacture years. Reading the padded
  -- `DateOfManufacture` is safe only because this takes the YEAR alone -- padding
  -- fabricates the month and day, never the year. Neither property survives being
  -- copied to a per-model date, which reads the header parse and its kind instead.
  SELECT
    ManufacturerId,
    MIN(EXTRACT(YEAR FROM CAST(DateOfManufacture AS DATE)))::INTEGER AS year_of_first_model,
    MAX(EXTRACT(YEAR FROM CAST(DateOfManufacture AS DATE)))::INTEGER AS year_of_last_model
  FROM ipdb_stg.models
  WHERE DateOfManufacture IS NOT NULL
  GROUP BY ManufacturerId
) model_years ON model_years.ManufacturerId = p.ipdb_manufacturer_id;

------------------------------------------------------------
-- IPDB credits
------------------------------------------------------------

-- IPDB's seven credit fields, flattened to one row per credited person.
--
-- The sentinel filter is what makes a row mean "IPDB credits this person". IPDB
-- spells an unknown credit a dozen ways -- '(undisclosed)', 'unknown', '?', an
-- empty string -- each of which would otherwise become a person of that name,
-- credited on hundreds of models.
--
-- Names are not resolved to catalog records: which person a name means is a
-- question about the catalog, answered better by its own alias pool.
--
-- Materialized because the seven-branch UNION runs once per query otherwise.
CREATE OR REPLACE TABLE ipdb_stg.credits AS
WITH raw AS (
  SELECT IpdbId, 'Design' AS role, TRIM(UNNEST(string_split(DesignBy, ','))) AS person_name FROM ipdb_stg.models_merged WHERE DesignBy <> ''
  UNION ALL
  SELECT IpdbId, 'Art', TRIM(UNNEST(string_split(ArtBy, ','))) FROM ipdb_stg.models_merged WHERE ArtBy <> ''
  UNION ALL
  SELECT IpdbId, 'Dots/Animation', TRIM(UNNEST(string_split(DotsAnimationBy, ','))) FROM ipdb_stg.models_merged WHERE DotsAnimationBy <> ''
  UNION ALL
  SELECT IpdbId, 'Mechanics', TRIM(UNNEST(string_split(MechanicsBy, ','))) FROM ipdb_stg.models_merged WHERE MechanicsBy <> ''
  UNION ALL
  SELECT IpdbId, 'Music', TRIM(UNNEST(string_split(MusicBy, ','))) FROM ipdb_stg.models_merged WHERE MusicBy <> ''
  UNION ALL
  SELECT IpdbId, 'Sound', TRIM(UNNEST(string_split(SoundBy, ','))) FROM ipdb_stg.models_merged WHERE SoundBy <> ''
  UNION ALL
  SELECT IpdbId, 'Software', TRIM(UNNEST(string_split(SoftwareBy, ','))) FROM ipdb_stg.models_merged WHERE SoftwareBy <> ''
  UNION ALL
  -- Archive-only roles. `xantari_field IS NULL` prevents this branch from
  -- overriding dump data; an unmatched page label is reported by
  -- `ipdb_archive_credit_role_unrecognised`.
  SELECT am.ipdb_id, cr.ipdb_role, TRIM(c."name")
  FROM ipdb_stg.archive_models AS am,
       unnest(am.credits) AS t(c)
  JOIN ipdb_ref.credit_role AS cr
    ON cr.xantari_field IS NULL
   AND list_contains(cr.archive_label, c.role)
)
SELECT r.IpdbId, r.role, cr.role_slug, r.person_name
FROM raw r
LEFT JOIN ipdb_ref.credit_role cr ON cr.ipdb_role = r.role
WHERE LOWER(r.person_name) NOT IN (
  '(undisclosed)', 'undisclosed', 'unknown', 'missing', 'null', 'undefined',
  'n/a', 'none', 'tbd', 'tba', '?', ''
)
  AND r.person_name NOT ILIKE '%(undisclosed)%'
  AND r.person_name NOT ILIKE '%unknown%';

------------------------------------------------------------
-- IPDB file trove
------------------------------------------------------------

-- IPDB's per-model file arrays, flattened to one row per file. Its seven
-- array names collapse two different axes: `rom` and `service_bulletin` say
-- what a file *is*, `image` and `file` say how it is delivered.
CREATE OR REPLACE VIEW ipdb_stg.model_files AS
SELECT
  IpdbId AS ipdb_id,
  Title AS model_name,
  f.Url AS file_url,
  f."Name" AS file_name,
  category
FROM ipdb_stg.models_merged, (
  SELECT unnest(ImageFiles) AS f, 'image' AS category
  UNION ALL SELECT unnest(Documentation), 'documentation'
  UNION ALL SELECT unnest(Files), 'file'
  UNION ALL SELECT unnest(RuleSheetUrls), 'rule_sheet'
  UNION ALL SELECT unnest(ROMs), 'rom'
  UNION ALL SELECT unnest(ServiceBulletins), 'service_bulletin'
  UNION ALL SELECT unnest(MultimediaFiles), 'multimedia'
);

-- One row per file with the delivery axis resolved and its model's context
-- carried alongside. Everything here is mechanical: the container is the URL's
-- extension, and the rest is the model's own attributes.
--
-- `model_mpu` is IPDB's free-text platform string, carried so a consumer can
-- group by platform without a vocabulary. Every join is on a unique key, so none
-- multiplies the file count.
--
-- Staging because `ipdb_stg.file_class_matches` below reads it. `ipdb.model_documents`
-- is the published answer to the same question.
CREATE OR REPLACE VIEW ipdb_stg.files AS
SELECT
  f.ipdb_id,
  f.model_name,
  f.category AS ipdb_category,
  f.file_name,
  f.file_url,
  regexp_extract(f.file_url, '([^/]+)$', 1) AS file_basename,
  nullif(lower(regexp_extract(f.file_url, '\.([A-Za-z0-9]{1,5})$', 1)), '') AS container,
  corporate_entity.manufacturer_name AS model_manufacturer,
  s.MPU AS model_mpu
FROM ipdb_stg.model_files AS f
LEFT JOIN ipdb_stg.models AS s ON s.IpdbId = f.ipdb_id
LEFT JOIN ipdb_stg.corporate_entities AS corporate_entity
  ON corporate_entity.ipdb_manufacturer_id = s.ManufacturerId;

-- Applying the declared patterns to the staged files: one row per file per
-- class it matches. The judgement is all in `ipdb_ref.document_class_pattern`;
-- this only runs it.
--
-- In two stages for the cost reason `ipdb_ref.document_class_pattern` describes:
-- stage one pairs a file with a pattern only when one of that pattern's declared
-- literals appears in the name, a substring test; stage two runs the real regex
-- on the few survivors. `required_any` is necessary only, so the verdict stays
-- entirely the regex's.
--
-- Materialized so the pairing runs once per build rather than once per query.
CREATE OR REPLACE TABLE ipdb_stg.file_class_matches AS
WITH needles AS (
  SELECT
    document_class,
    pattern,
    allow_containers,
    deny_pattern,
    unnest(required_any) AS needle
  FROM ipdb_ref.document_class_pattern
),
candidates AS (
  SELECT DISTINCT
    f.ipdb_id,
    f.file_url,
    f.ipdb_category,
    f.file_name,
    f.container,
    n.document_class,
    n.pattern,
    n.allow_containers,
    n.deny_pattern
  FROM ipdb_stg.files AS f
  INNER JOIN needles AS n ON contains(lower(f.file_name), n.needle)
)
-- DISTINCT because a class may be detected by more than one pattern row, and
-- which row fired is not projected: two matches on the same class would be
-- indistinguishable rows, and would double any count taken off this table.
SELECT DISTINCT
  cd.ipdb_id,
  cd.file_url,
  cd.ipdb_category,
  cd.file_name,
  cd.container,
  cd.document_class,
  c.source_kind
FROM candidates AS cd
INNER JOIN ipdb_ref.document_class AS c USING (document_class)
WHERE regexp_matches(lower(cd.file_name), cd.pattern)
  AND (
    cd.allow_containers IS NULL
    OR list_contains(cd.allow_containers, coalesce(cd.container, ''))
  )
  AND (
    cd.deny_pattern IS NULL
    OR NOT regexp_matches(lower(cd.file_name), cd.deny_pattern)
  );

------------------------------------------------------------
-- IPDB archive pages, unnested
------------------------------------------------------------

-- The three list-valued page fields, one row per element.
--
-- STAGING, not mart, and deliberately so. Themes and documents are here to be
-- COMPARED against what xantari already gives us -- IPDB's theme string is one
-- field in the dump and a list on the page, and the page's document rows carry a
-- section, kind and size the dump's flat url/name pairs do not. Whether any of
-- that is worth taking is a question to answer by querying these, and a mart
-- view would promise flippatch an answer nobody has reached yet.

-- IPDB's themes as the page lists them, against `ipdb.models.theme`, which is
-- xantari's single delimited string for the same thing.
CREATE OR REPLACE VIEW ipdb_stg.archive_model_themes AS
SELECT ipdb_id, unnest(themes) AS theme
FROM ipdb_stg.archive_models
WHERE len(themes) > 0;

-- The page's document listings, richer than `ipdb_stg.model_files`: that view
-- has a url and a name, these rows also say which section of the page the file
-- was listed under and what IPDB calls it.
CREATE OR REPLACE VIEW ipdb_stg.archive_model_documents AS
SELECT
  am.ipdb_id,
  d.section,
  d."name" AS file_name,
  d.url    AS file_url,
  d.kind,
  d.size,
  d.credit
FROM ipdb_stg.archive_models AS am, unnest(am.documents) AS t(d);

-- Page specialties in IPDB's wording. Capture provenance stays on each unnested
-- row rather than depending on a later join whose uniqueness is not asserted.
--
-- No longer the mart's source -- `ipdb_stg.model_specialties` below is, from the
-- census. These rows are kept for the one thing the census cannot do: say what
-- IPDB used to say. `ipdb_specialty_reclassified` diffs the two.
CREATE OR REPLACE VIEW ipdb_stg.archive_model_specialties AS
SELECT
  ipdb_id,
  unnest(specialties) AS specialty,
  archive_source_url,
  archive_capture_date
FROM ipdb_stg.archive_models
WHERE len(specialties) > 0;

------------------------------------------------------------
-- IPDB specialty census
------------------------------------------------------------

-- The census, cut down to the models xantari knows.
--
-- Same restriction as `ipdb_stg.archive_models`, for a weaker reason. There the
-- rule keeps a decade-old capture from asserting a listing still exists; here
-- the census is hours old, so a machine it lists and the dump does not is almost
-- certainly one IPDB has ADDED since. It is still dropped, because xantari
-- remains the source of which models exist and half a model -- specialties but
-- no name, date or manufacturer -- is worse than none. Reading through
-- `models_merged` also inherits the retraction filter.
-- `ipdb_specialty_census_model_not_in_dump` reports what this drops, and a
-- growing count there means it is time for a fresh dump.
CREATE OR REPLACE VIEW ipdb_stg.specialty_census AS
SELECT c.*
FROM ipdb_raw.specialty_census AS c
WHERE EXISTS (
  SELECT 1 FROM ipdb_stg.models_merged AS mm WHERE mm.IpdbId = c.ipdb_id
);

-- One row per model per specialty IPDB currently assigns it.
--
-- The mart's source. Provenance rides on the row rather than joining for it: the
-- struct already carries the search URL the assignment was read from, so a
-- consumer citing this needs no join whose uniqueness nobody asserted.
--
-- `observed_on` is one date for every row, which is the point: unlike the archive
-- captures this replaced, the whole census was taken at one moment.
--
-- Taken from `ingest.watermarks` rather than `ref.artifact_acquisitions`
-- directly, so it inherits that view's count gate. The pages state no date about
-- themselves, so the acquisition log is the only source for it and is kept by
-- hand -- which means a new download dropped in without touching the log would
-- otherwise stamp every published row with the PREVIOUS download's date. Gated,
-- that case publishes NULL instead, and `artifact_acquisition_log_stale` says
-- why.
CREATE OR REPLACE VIEW ipdb_stg.model_specialties AS
SELECT
  c.ipdb_id,
  s.specialty,
  s.specialty_id,
  s.source_url,
  (SELECT acquired_on FROM ingest.watermarks
   WHERE artifact = 'ipdb/ipdb_specialty/census.jsonl') AS observed_on
FROM ipdb_stg.specialty_census AS c, unnest(c.specialties) AS t(s);

-- Where the census and the xantari dump disagree about the same model.
--
-- The census's non-specialty columns are not published as rival values -- the
-- dump is the field-level source and this view does not overrule it. They are
-- carried to be a LIVE READ against a dump that is months old, on thousands of
-- models at once, which is a cross-check nothing else in this build performs.
--
-- Long rather than wide: one row per disagreeing field, so a new field joins the
-- comparison by adding a leg rather than a column, and the warning that counts
-- these needs no list of what to look at.
--
-- Rating is deliberately absent. IPDB recomputes it as votes arrive, so it
-- differs on a tenth of the overlap by design and would drown the rows that mean
-- something. Dates compare only where both state one: xantari scraped Date Of
-- Manufacture alone, so IPDB's several hundred Project Date models are simply
-- blank there, and that absence is `date_is_project_date`, not a disagreement.
CREATE OR REPLACE VIEW ipdb_stg.specialty_census_vs_dump AS
WITH paired AS (
  SELECT
    c.ipdb_id,
    c."name",
    c.date_year, c.date_month, c.players, c.type_code,
    c.model_number, c.production_units,
    m.Title, m.ModelNumber, m.ProductionNumber,
    m.Players AS dump_players,
    -- From `ipdb_stg.models`, which is where the dump's blank-on-PM short name is
    -- corrected. The other columns come from `models_merged` instead, because
    -- `ipdb_stg.models` fills production and rating from archive pages and this
    -- view compares the census against the DUMP, not against the dump's fills.
    sm.type_code AS dump_type_code,
    CAST(m.DateOfManufacture AS DATE) AS dump_date
  FROM ipdb_stg.specialty_census AS c
  INNER JOIN ipdb_stg.models_merged AS m ON m.IpdbId = c.ipdb_id
  INNER JOIN ipdb_stg.models AS sm ON sm.IpdbId = c.ipdb_id
)
-- `nullif(..., '')` on every dump string: xantari writes an empty string where
-- IPDB is silent, and an empty string is an absence rather than a differing
-- value. Without this each one reads as a disagreement.
SELECT ipdb_id, 'name' AS field, "name" AS census_value, Title AS dump_value
FROM paired WHERE "name" IS NOT NULL AND nullif(Title, '') IS NOT NULL AND "name" <> Title
UNION ALL
SELECT ipdb_id, 'players', CAST(players AS VARCHAR), CAST(dump_players AS VARCHAR)
FROM paired WHERE players IS NOT NULL AND dump_players IS NOT NULL AND players <> dump_players
UNION ALL
SELECT ipdb_id, 'type_code', type_code, dump_type_code
FROM paired WHERE type_code IS NOT NULL AND dump_type_code IS NOT NULL
  AND type_code <> dump_type_code
UNION ALL
SELECT ipdb_id, 'model_number', model_number, ModelNumber
FROM paired WHERE model_number IS NOT NULL AND nullif(ModelNumber, '') IS NOT NULL
  AND model_number <> ModelNumber
UNION ALL
SELECT ipdb_id, 'production_units', CAST(production_units AS VARCHAR), CAST(ProductionNumber AS VARCHAR)
FROM paired WHERE production_units IS NOT NULL AND ProductionNumber IS NOT NULL
  AND production_units <> ProductionNumber
UNION ALL
SELECT ipdb_id, 'date_year', CAST(date_year AS VARCHAR), CAST(year(dump_date) AS VARCHAR)
FROM paired WHERE date_year IS NOT NULL AND dump_date IS NOT NULL AND date_year <> year(dump_date)
UNION ALL
SELECT ipdb_id, 'date_month', CAST(date_month AS VARCHAR), CAST(month(dump_date) AS VARCHAR)
FROM paired WHERE date_month IS NOT NULL AND dump_date IS NOT NULL AND date_month <> month(dump_date);

------------------------------------------------------------
-- Fandom staged
------------------------------------------------------------

-- Fandom is parsed but never compared. It duplicates information we already
-- hold from better sources and we have not acquired its ids, so there is no
-- `fandom` mart -- comparing would only surface the ids we chose not to get.

CREATE OR REPLACE VIEW fandom_stg.games AS
SELECT
  page_id,
  title AS fandom_name,
  regexp_extract(wikitext, '\|manufacturer\s*=\s*\[\[([^\]]+)', 1) AS manufacturer,
  regexp_extract(wikitext, '\|system\s*=\s*\[\[([^\]]+)', 1) AS system,
  TRY_CAST(regexp_extract(wikitext, '\|release\s*=\s*.*?\[\[(\d{4})', 1) AS INTEGER) AS year,
  TRY_CAST(replace(regexp_extract(wikitext, '\|production\s*=\s*([\d,]+)', 1), ',', '') AS INTEGER) AS production,
  wikitext
FROM fandom_raw.games;

CREATE OR REPLACE VIEW fandom_stg.manufacturers AS
SELECT
  page_id,
  title AS fandom_name,
  wikitext
FROM fandom_raw.manufacturers;

CREATE OR REPLACE VIEW fandom_stg.people AS
SELECT
  page_id,
  title AS fandom_name,
  wikitext
FROM fandom_raw.people;
