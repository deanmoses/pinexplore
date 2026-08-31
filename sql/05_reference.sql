-- Reference tables, macros, and exception lists.
-- Domain knowledge that doesn't come from any data file.

------------------------------------------------------------
-- Source-specific code mappings
------------------------------------------------------------

CREATE OR REPLACE VIEW opdb_ref.technology_generation AS
SELECT * FROM (VALUES
  ('em', 'electromechanical'),
  ('ss', 'solid-state'),
  ('me', 'pure-mechanical')
) AS t(opdb_type, slug);

CREATE OR REPLACE VIEW opdb_ref.display_type AS
SELECT * FROM (VALUES
  ('reels',        'score-reels'),
  ('lights',       'backglass-lights'),
  ('alphanumeric', 'alphanumeric'),
  ('cga',          'cga'),
  ('dmd',          'dot-matrix'),
  ('lcd',          'lcd')
) AS t(opdb_display, slug);

-- Where each OPDB `features` value goes in Flipcommons terms, the counterpart of
-- `ipdb_ref.specialty`.
--
-- OPDB carries in one flat array what Flipcommons splits across a reward type, a
-- cabinet, a tag, a gameplay feature and a relationship edge. `target_entity_type`
-- names which, and the mart splits the rows out to a view or column per entity so
-- no consumer has to know this discriminator exists.
--
-- `model-relationship` is the one value that names a Flipcommons STRUCTURE
-- rather than an entity: a `ModelRelationship` edge whose type is stated and
-- whose donor usually is not.
--
-- `model-lineage` is the second such value, and it means the fact lands as a
-- COLUMN on `opdb.models` rather than a row in a `model_*` view: Flipcommons
-- carries `export_edition_of` as a scalar FK, and OPDB never says what a remake
-- is a remake OF, so `is_remake` is a flag. Both are read off the alias tree
-- rather than out of this table -- but they are listed, because this table is
-- the index of where EVERY OPDB feature goes, and a value missing from it reads
-- as one nobody has considered.
--
-- `target_value` IS NEVER A CLAIM ABOUT THE CATALOG. Pinexplore reads no catalog,
-- so it cannot say whether a value resolves there and must not pretend to:
-- `pro-edition` is written the same way as `limited-edition` whether or not the
-- catalog has either. Resolving a value, or deciding it should not resolve, is
-- flippatch's -- beside the live records, where the answer can be checked.
--
-- NOR IS IT ALWAYS SLUG-SHAPED, and forcing it to be broke a live lookup once.
-- The catalog's aliases are DISPLAY WORDING -- gameplay feature `head-to-head`
-- carries the alias `Head-to-head play` -- so a bucketed value has to keep the
-- source's phrasing to match one. Hyphenating it into `head-to-head-play` made
-- it match nothing, and no one would ever author that as an alias.
--
-- WHETHER A VALUE IS TRANSLATED TURNS ON THE TARGET, not on which of the two
-- decode tables the row lives in. A target in a small closed catalog vocabulary
-- -- tag, reward type, cabinet, series, none of which the catalog gives an alias
-- system -- is written in the catalog's wording: `Home model` -> `home-use`,
-- `Cocktail table` -> `cocktail`. A target in one of the large vocabularies that
-- DOES carry aliases -- theme, gameplay feature -- keeps OPDB's wording, because
-- matching it to the catalog is what those aliases are for. Hence the single
-- gameplay-feature row here reads `Head-to-head play` and not the catalog's
-- `head-to-head`; that exact phrase is the alias the catalog carries.
--
-- The test is whether the vocabulary has aliases, not how big it is. Size is why
-- it has them, and is the thing that will have changed by the time anyone reads
-- this.
-- `docs/plans/OpdbMappings.md` has the rule.
CREATE OR REPLACE VIEW opdb_ref.feature AS
SELECT * FROM (VALUES
  ('Add-a-ball',        'reward-type',        'add-a-ball'),
  ('Replay',            'reward-type',        'replay'),
  ('Cocktail table',    'cabinet',            'cocktail'),
  ('Export edition',    'model-lineage',      'export_edition_of'),
  ('Remake',            'model-lineage',      'is_remake'),
  ('Conversion kit',    'model-relationship', 'conversion_kit'),
  ('Converted game',    'model-relationship', 'conversion'),
  ('Head-to-head play', 'gameplay-feature',   'Head-to-head play'),
  ('Widebody',          'tag',                'widebody'),
  ('Home model',        'tag',                'home-use'),
  ('Limited edition',   'tag',                'limited-edition'),
  ('Premium edition',   'tag',                'premium-edition'),
  ('Pro edition',       'tag',                'pro-edition'),
  ('Vault edition',     'tag',                'vault-edition')
) AS t(opdb_feature, target_entity_type, target_value);

-- Where each OPDB `keywords` value goes. Mostly themes, and the reason this is a
-- second table rather than a second column on the one above is that the two
-- vocabularies overlap: OPDB says `Widebody` in BOTH, and both must land on the
-- one catalog tag.
--
-- ALMOST NOTHING IS TRANSLATED HERE, because almost every keyword targets a
-- theme -- a catalog vocabulary with an alias system;
-- knowing that `automotive` canonicalizes to `cars` is that system's job,
-- not this file's. Pinexplore puts a theme-like keyword in the theme bucket and
-- stops; flippatch resolves it through the aliases, rules it permanently out, or
-- ignores the bucket entirely. The same rule as `opdb_ref.feature` above, which
-- states it in full -- it just happens to land the other way here.
--
-- OPDB's keyword list is loose in a way its feature list is not -- free-text
-- tagging rather than a controlled vocabulary -- so a few of them name nothing
-- the catalog could ever model. `no-target` is a DELIBERATE verdict that a
-- keyword dies here rather than reaching flippatch, and it exists so the
-- build-blocking check below can tell "decided to drop this" from "nobody has
-- looked yet". Removing a row is never how a keyword gets dropped.
CREATE OR REPLACE VIEW opdb_ref.keyword AS
SELECT
  opdb_keyword,
  nullif(target_entity_type, 'no-target') AS target_entity_type,
  nullif(target_value, '') AS target_value
FROM (VALUES
  ('Bathurst',         'theme',            'bathurst'),
  ('Fantasy',          'theme',            'fantasy'),
  -- Peter Brock, whose name OPDB puts on the Holden machines. A theme, not a
  -- credited person.
  ('brock',            'theme',            'brock'),
  ('Holden',           'theme',            'holden'),
  ('Torana',           'theme',            'torana'),
  ('board-game',       'theme',            'board-game'),
  ('cards',            'theme',            'cards'),
  ('commodore',        'theme',            'commodore'),
  ('monster',          'theme',            'monster'),
  ('movie',            'theme',            'movie'),
  ('music',            'theme',            'music'),
  ('poker',            'theme',            'poker'),
  ('racing',           'theme',            'racing'),
  ('time-travel',      'theme',            'time-travel'),
  ('video-games',      'theme',            'video-games'),
  ('automotive',       'theme',            'automotive'),
  ('tv',               'theme',            'tv'),
  ('safari-adventure', 'theme',            'safari-adventure'),
  ('geriatric',        'theme',            'geriatric'),
  -- The overlap with `opdb_ref.feature`: one catalog tag, two OPDB fields.
  ('Widebody',         'tag',              'widebody'),
  -- Licensed IP. Tag-like, so it goes to the tag bucket, which is the right
  -- place for flippatch to pick it up from -- it will not create a tag, it will
  -- read it as part of the licensing on a relationship.
  ('licensed',         'tag',              'licensed'),
  ('action-button',    'gameplay-feature', 'action-button'),
  ('staged-flippers',  'gameplay-feature', 'staged-flippers'),
  -- Fragments of a machine's own title rather than anything about it: `eight`
  -- and `ball` come off Eight Ball. These die here.
  ('ball',             'no-target',        ''),
  ('eight',            'no-target',        ''),
  -- Gottlieb/Premier's Street Level product line, which OPDB puts on exactly its
  -- six members. Flipcommons models a family of related games as a Series, so
  -- that is the bucket. Note the grain: OPDB tags the machines and Flipcommons
  -- hangs a Series off the Title, so `opdb.model_series` states what OPDB stated
  -- and the roll-up to titles is flippatch's, beside the live records.
  ('street-level',     'series',           'street-level')
) AS t(opdb_keyword, target_entity_type, target_value);

-- OPDB's edition tags, ordered BROADEST FIRST.
--
-- The order is the whole content here. Flipcommons hangs cosmetic variants off
-- the broadest model of a set -- an LE is a variant of the Premium, never the
-- reverse -- and this ladder is the only machine-readable statement of which
-- model that is. `opdb_stg.alias_lineage` elects a primary with it.
--
-- Absence from this list is not a gap: `Export edition`, `Remake`, `Conversion
-- kit` and `Home model` are OPDB feature tags too, but they say what KIND of
-- relative a model is rather than how broad it is, and Flipcommons carries them
-- on their own FKs. Adding one here would make it compete to be a primary.
CREATE OR REPLACE VIEW opdb_ref.edition_rank AS
SELECT * FROM (VALUES
  ('Pro edition',     1),
  ('Premium edition', 2),
  ('Vault edition',   3),
  ('Limited edition', 4)
) AS t(opdb_feature, breadth_rank);

-- IPDB's three type codes in catalog vocabulary.
CREATE OR REPLACE VIEW ipdb_ref.technology_generation AS
SELECT * FROM (VALUES
  ('EM', 'electromechanical'),
  ('SS', 'solid-state'),
  ('PM', 'pure-mechanical')
) AS t(type_code, slug);

-- A credit role IPDB states, its catalog slug, and the source allowed to supply
-- it. `xantari_field` names the dump column; NULL lets archive pages supply a role
-- until the dump gains that field, an expiry enforced by
-- `ipdb_credit_role_xantari_field_unclaimed`. `archive_label` is a list because
-- one role may have several page labels; a label no row lists warns via
-- `ipdb_archive_credit_role_unrecognised` rather than vanishing.
CREATE OR REPLACE VIEW ipdb_ref.credit_role AS
SELECT * FROM (VALUES
  ('Design',         'design',    'DesignBy',        ['Design by']),
  ('Art',            'art',       'ArtBy',           ['Art by']),
  ('Dots/Animation', 'animation', 'DotsAnimationBy', ['Dots/Animation by', 'Animation by']),
  ('Mechanics',      'mechanics', 'MechanicsBy',     ['Mechanics by']),
  ('Music',          'music',     'MusicBy',         ['Music by']),
  ('Sound',          'sound',     'SoundBy',         ['Sound by']),
  ('Software',       'software',  'SoftwareBy',      ['Software by']),
  -- NULL lets the archive supply Concept until the dump gains it.
  ('Concept',        'concept',   NULL,              ['Concept by'])
) AS t(ipdb_role, role_slug, xantari_field, archive_label);

-- IPDB Specialty is basic machine classification absent from the xantari dump
-- but carried by archive pages. The values were transcribed from the dropdown at:
-- <https://www.ipdb.org/search.pl?specialty=12&sortby=date&searchtype=advanced>.
--
-- `target_entity_type` names a Flipcommons entity type. `model-relationship` is
-- the exception: it says only that the model has an edge whose donor still needs
-- research.
--
-- `target_value` IS NEVER A CLAIM ABOUT THE CATALOG, the same rule as
-- `opdb_ref.feature` above, which states it in full. Pinexplore reads no catalog
-- and cannot know whether a value resolves; flippatch answers that beside the
-- live records. A slug-shaped value here is a translation into a small closed
-- vocabulary, and IPDB's display wording is what a target the catalog has no
-- word for looks like -- but neither spelling is a verdict, and nothing asserts
-- which is which.
CREATE OR REPLACE VIEW ipdb_ref.specialty AS
SELECT * FROM (VALUES
  ('Cocktail Table',                      'cabinet',            'cocktail'),
  -- One IPDB heading over two of our cabinets, `tabletop` and `countertop`.
  -- Same shape as Payout Machine: per-model research, not new vocabulary.
  ('Table Top/Counter Game',              'cabinet',            'Table Top/Counter Game'),
  ('Vertical Pinball Machine',            'cabinet',            'Vertical Pinball Machine'),
  ('Conversion Kit',                      'model-relationship', 'conversion_kit'),
  ('Converted Game',                      'model-relationship', 'conversion'),
  ('Re-themed Game',                      'model-relationship', 'retheme'),
  ('Add-A-Ball',                          'reward-type',        'add-a-ball'),
  ('Novelty Play',                        'reward-type',        'novelty'),
  ('Redemption Game',                     'reward-type',        'ticket-payout'),
  -- IPDB's single word covers what we split into `cash-payout` and
  -- `merchant-paid`, and the page does not say which. Reading the models is the
  -- only way to tell them apart, so this stays a worklist rather than guessing.
  ('Payout Machine',                      'reward-type',        'Payout Machine'),
  ('Bagatelle',                           'game-format',        'bagatelle'),
  ('Bat Game',                            'game-format',        'pitch-and-bat'),
  ('Bingo Machine',                       'game-format',        'bingo-pinball'),
  ('Rolldown Game',                       'game-format',        'rolldown'),
  ('Shaker Ball Machine',                 'game-format',        'Shaker Ball Machine'),
  ('Cue Game',                            'game-format',        'Cue Game'),
  ('Gun Game',                            'game-format',        'gun-game'),
  ('Horserace Game',                      'game-format',        'Horserace Game'),
  ('Not A Pinball',                       'game-format',        'Not A Pinball'),
  ('One Ball Game',                       'game-format',        'one-ball'),
  ('Widebody',                            'tag',                'widebody'),
  ('Non-Commercial Machine [Home Model]', 'tag',                'home-use'),
  ('WWII Contract',                       'tag',                'wwii-contract'),
  ('Flipperless',                         'tag',                'flipperless'),
  ('Mechanical Backbox Animation',        'gameplay-feature',   'Mechanical Backbox Animation'),
  ('Head-to-Head Play',                   'gameplay-feature',   'Head-to-Head Play'),
  ('Zipper Flippers',                     'gameplay-feature',   'Zipper Flippers')
) AS t(ipdb_specialty, target_entity_type, target_value);

------------------------------------------------------------
-- Retracted IPDB listings
------------------------------------------------------------

-- IPDB records confirmed deleted upstream, which `ipdb_stg.models_merged` drops
-- even though an older snapshot still carries them.
--
-- A record's absence from the newest snapshot is ambiguous: when the 2026-04
-- scrape dropped six ids, five were still live and only the crawl had missed
-- them. So the merge carries every dropped record forward and forgets only what
-- is listed here.
--
-- Only add a row after loading the URL and confirming the machine is gone: a
-- deleted record renders IPDB's bare page chrome, exactly as a nonexistent id
-- like 99999 does.
CREATE OR REPLACE VIEW ipdb_ref.retracted AS
SELECT * FROM (VALUES
  (3239, DATE '2026-04-11', 'IPDB deleted this listing, having announced the deletion in the record itself: "Sixty-Two Baseball" was a longhand-year artifact ("1962 Baseball") mistaken for a title, re-designated Not A Pinball pending removal. The real machine is Midway 1962 Deluxe Baseball, IPDB 656.', 'https://www.ipdb.org/machine.cgi?id=3239')
) AS t(ipdb_id, first_absent_on, reason, evidence_url);

------------------------------------------------------------
-- IPDB manufacturer records that name no company
------------------------------------------------------------

-- IPDB manufacturer ids standing for the absence of a manufacturer rather than
-- for one. Gap analysis asking "who made this" must not report them as a company
-- we are missing. `ipdb.models` translates them to NULL; why that is the mart's
-- job and not each consumer's is stated there.
--
-- Corporate-entity grain, not manufacturer -- see `ipdb.corporate_entities`.
CREATE OR REPLACE VIEW ipdb_ref.corporate_entity_not_a_company AS
SELECT * FROM (VALUES
  (0,   'No manufacturer id on the record; the Manufacturer field is null too.'),
  (328, 'IPDB''s explicit placeholder, rendered as the literal string "Unknown Manufacturer".')
) AS t(ipdb_manufacturer_id, reason);

------------------------------------------------------------
-- IPDB records the dump misparsed
------------------------------------------------------------

-- Machines whose manufacturer fields in the xantari dump are a scrape artifact
-- rather than anything IPDB asserts. Staging nulls both fields for these, so a
-- consumer sees the absence IPDB's own page shows.
--
-- Keyed on the machine, not the offending string: the id is wrong as well as the
-- name, so dropping the name alone would leave the machine attributed to a real
-- company IPDB's page explicitly denies.
--
-- Add a row only after reading the page and quoting it. This list overrides the
-- source, so a wrong row here silently deletes true data.
CREATE OR REPLACE VIEW ipdb_ref.model_corporate_entity_misparsed AS
SELECT * FROM (VALUES
  (6505, 'IPDB names no manufacturer for this 2017 re-theme; the dump lifted a fragment of the Notes as one. The note reads: "The backglass references \"T & M Sales Co.\" but is not the 1940''s manufacturer. It represents the first names of the two people who re-themed this game." The dump stored the phrase "the 1940''s manufacturer" as the name and linked it to id 309, T and M Sales Company -- the company that sentence rules out.', 'https://www.ipdb.org/machine.cgi?id=6505')
) AS t(ipdb_id, reason, evidence_url);

------------------------------------------------------------
-- Duplicate IPDB listings
------------------------------------------------------------

-- One machine listed twice by IPDB, under two of its own manufacturer records
-- for the same company.
--
-- Unlike `ipdb_ref.retracted`, this does NOT filter `ipdb_stg.models_merged`: a
-- duplicate listing is still live at its own URL, so dropping it would misstate
-- IPDB. It is an annotation for consumers resolving IPDB ids against a catalog,
-- where the catalog links one of the pair and the other reads as a gap.
--
-- Weaker evidence than a retraction. Nothing here is IPDB's own statement --
-- each row infers from two listings agreeing on title, year, players and city
-- while naming two spellings of one maker. Read both URLs before trusting a row.
--
-- `duplicate_of_ipdb_id` is the fuller listing, chosen on IPDB's evidence and
-- not on which one our catalog links. A catalog linked to the other one is a
-- real finding, not a stale exception.
CREATE OR REPLACE VIEW ipdb_ref.duplicate_listings AS
SELECT * FROM (VALUES
  (4127, 5269, 'Both are Gold Star, 1976, 1 player, of Messina, Italy, under two IPDB manufacturer records for one company: 104 "Ditta Ripepi s.p.a." and 457 "Ripepi". 5269 carries the type, theme, features and art credit; 4127 carries only the note that it is a single-player copy of Gottlieb''s Fast Draw.', 'https://www.ipdb.org/machine.cgi?id=4127'),
  (4783, 5458, 'Both are Space Orbit!, 1972, 1 player, of Bologna, Italy, under two IPDB manufacturer records for one company: 442 "Skillgame d.b.a. Renato Montanari Giochi [Trade Name: R.M.G.]" and 359 "Renato Montanari Giochi [Trade Name: R.M.G.]". 5458 carries the fuller feature list and the collector notes identifying it as a copy of Gottlieb''s 1972 add-a-ball Space Orbit!.', 'https://www.ipdb.org/machine.cgi?id=4783')
) AS t(ipdb_id, duplicate_of_ipdb_id, reason, evidence_url);

------------------------------------------------------------
-- Document classification vocabulary
------------------------------------------------------------

-- What kind of source a class describes. A manufacturer's manual, a patent and
-- a trade-press article are each addressed differently — by document, by
-- jurisdiction and number, by publication and issue — so the kind is declared
-- rather than inferred from the class name.
CREATE OR REPLACE VIEW ipdb_ref.source_kind AS
SELECT * FROM (VALUES
  ('document',      'A document published about a machine, system or maker'),
  ('patent',        'A government patent publication'),
  ('trade_article', 'An article or advertisement in a trade periodical')
) AS t(source_kind, description);

-- Every class, including grouping nodes. `marketing` and `service_reference`
-- carry no detection pattern and never match a filename: they exist only as
-- parents, which is why the vocabulary cannot live in the pattern table.
--
-- The web cache carries its own `document_class_vocab`, unsynced with this one,
-- because they answer different questions: a class here is one the patterns
-- below may EMIT, a class there is one a person may ASSIGN. A class that only
-- ever arrives by judgment belongs there and not here.
CREATE OR REPLACE VIEW ipdb_ref.document_class AS
SELECT * FROM (VALUES
  ('manual',              'document'),
  ('operations_manual',   'document'),
  ('service_manual',      'document'),
  ('owners_manual',       'document'),
  ('installation_instructions', 'document'),
  ('handbook',            'document'),
  ('schematic',           'document'),
  ('wiring_diagram',      'document'),
  ('engineering_drawing', 'document'),
  ('cad_file',            'document'),
  ('bill_of_material',    'document'),
  ('specification',       'document'),
  ('parts_list',          'document'),
  ('service_reference',   'document'),
  ('switch_matrix',       'document'),
  ('coil_chart',          'document'),
  ('dip_switch',          'document'),
  ('score_motor_chart',   'document'),
  ('rubber_ring_chart',   'document'),
  ('adjustments',         'document'),
  ('service_bulletin',    'document'),
  ('notice_to_operators', 'document'),
  ('marketing',           'document'),
  ('flyer',               'document'),
  ('brochure',            'document'),
  ('advertisement',       'document'),
  ('feature_matrix',      'document'),
  ('press_release',       'document'),
  ('price_card',          'document'),
  ('promotional_photo',   'document'),
  ('instruction_card',    'document'),
  ('warranty',            'document'),
  ('packing_list',        'document'),
  ('certificate',         'document'),
  ('rules_of_play',       'document'),
  ('strategy_guide',      'document'),
  ('game_description',    'document'),
  ('interview',           'document'),
  ('credits',             'document'),
  ('correspondence',      'document'),
  ('artwork_scan',        'document'),
  ('photo_set',           'document'),
  ('game_audio',          'document'),
  ('video',               'document'),
  ('rom_set',             'document'),
  ('accessory_kit',       'document'),
  ('supplement',          'document'),
  ('patent',              'patent'),
  ('trade_article',       'trade_article')
) AS t(document_class, source_kind);

-- Parent edges, one row each so a class may later carry two parents.
--
-- Not checkable as a standing view: an edge is only corroborated by the data
-- when the child's name lexically contains the parent's, and a wiring diagram is
-- a schematic without saying so, so low agreement there is silence rather than
-- contradiction.
CREATE OR REPLACE VIEW ipdb_ref.document_class_parent AS
SELECT * FROM (VALUES
  ('operations_manual',   'manual'),
  ('service_manual',      'manual'),
  ('owners_manual',       'manual'),
  ('wiring_diagram',      'schematic'),
  ('switch_matrix',       'service_reference'),
  ('coil_chart',          'service_reference'),
  ('dip_switch',          'service_reference'),
  ('score_motor_chart',   'service_reference'),
  ('rubber_ring_chart',   'service_reference'),
  ('adjustments',         'service_reference'),
  ('flyer',               'marketing'),
  ('brochure',            'marketing'),
  ('advertisement',       'marketing'),
  ('feature_matrix',      'marketing'),
  ('press_release',       'marketing'),
  ('price_card',          'marketing'),
  ('promotional_photo',   'marketing')
) AS t(document_class, parent_class);

-- How a class is detected in an IPDB filename. Several rows may name the same
-- class, and patterns **deliberately overlap**: "Schematic Manual" is both, so
-- every match is emitted and the collisions are the useful output. A
-- first-match-wins rule would hide them.
--
-- `allow_containers` lets the file extension constrain the class, set only where
-- it discriminates: "sound" appears in a factory kit sheet and an EPROM dump as
-- readily as in a recording. It also keeps a jpg named "Rodeo Patents Notice" --
-- a photo of numbers on a cabinet -- out of `patent`. Not drawn for the
-- marketing classes, whose flyers and instruction cards live in IPDB's gallery
-- as scans, so they match images on purpose.
--
-- `deny_pattern` covers what the container cannot: "Sound ROMs V1.0" and
-- "Monster Bash All Sound Files" are both zips, and only the word ROM says which
-- is a chip dump.
--
-- `required_any` is what makes the match affordable. The pattern comes from a
-- column rather than a literal, so DuckDB recompiles the regex per row --
-- millions of RE2 constructions, a thousand times the cost of matching. These
-- lowercase literals let a substring test discard almost every pair first.
--
-- Necessary, never sufficient -- the regex still decides. Widen when in doubt: a
-- broad needle costs time, a narrow one silently drops matches. Every top-level
-- branch needs its own needle, so `newpaper|newspaper` needs both spellings, and
-- a needle must be a literal its branch cannot match without ("how" for
-- `how.to.play`, not "how to play").
CREATE OR REPLACE VIEW ipdb_ref.document_class_pattern AS
SELECT * FROM (VALUES
  ('operations_manual',   'operation(s)? manual|operating manual', ['operation', 'operating'], NULL, NULL),
  ('service_manual',      'service manual|repair manual|shop manual|maintenance manual', ['service manual', 'repair manual', 'shop manual', 'maintenance manual'], NULL, NULL),
  ('owners_manual',       'owner.?s? manual', ['owner'], NULL, NULL),
  ('installation_instructions', 'installation instruction', ['installation instruction'], NULL, NULL),
  ('manual',              '\bmanual\b', ['manual'], NULL, NULL),
  ('handbook',            'handbook', ['handbook'], NULL, NULL),

  ('schematic',           'schematic', ['schematic'], NULL, NULL),
  ('wiring_diagram',      'wiring|cabinet diagram|harness', ['wiring', 'cabinet diagram', 'harness'], NULL, NULL),
  ('engineering_drawing', 'engineering drawing|assembly drawing|drawing package', ['engineering drawing', 'assembly drawing', 'drawing package'], NULL, NULL),
  ('engineering_drawing', 'factory drawing|pictorial diagram|assembly diagram', ['factory drawing', 'pictorial diagram', 'assembly diagram'], NULL, NULL),
  ('cad_file',            '\bcad\b|fusion 360|\bdxf\b|overlay template', ['cad', 'fusion 360', 'dxf', 'overlay template'], NULL, NULL),
  ('bill_of_material',    'bill of material|\bbom\b', ['bill of material', 'bom'], NULL, NULL),
  ('specification',       'specification|datasheet|theory of operation|test procedure', ['specification', 'datasheet', 'theory of operation', 'test procedure'], NULL, NULL),
  ('parts_list',          'parts (list|catalog|diagram|layout)|part list|parts identification', ['part'], NULL, NULL),

  ('switch_matrix',       'switch matrix|switch(es)?[ ,/].*(matrix|chart|list|data)', ['switch'], NULL, NULL),
  ('switch_matrix',       'lamps?/solenoid|solenoid matrix', ['solenoid'], NULL, NULL),
  ('coil_chart',          'coil (chart|list|renumbering)|solenoid (list|table|identification)', ['coil ', 'solenoid '], NULL, NULL),
  ('dip_switch',          'dip switch', ['dip switch'], NULL, NULL),
  ('score_motor_chart',   'score motor|cam (switch|map|identification)|relay chart|motor switch', ['score motor', 'cam ', 'relay chart', 'motor switch'], NULL, NULL),
  ('rubber_ring_chart',   'rubber ring', ['rubber ring'], NULL, NULL),
  ('adjustments',         'adjustment', ['adjustment'], NULL, NULL),
  ('service_bulletin',    'bulletin', ['bulletin'], NULL, NULL),
  ('notice_to_operators', 'notice to operators|message to operators|customer service notice|shipping notice', ['notice to operators', 'message to operators', 'customer service notice', 'shipping notice'], NULL, NULL),

  ('flyer',               'flyer', ['flyer'], NULL, NULL),
  ('brochure',            'brochure|promotional folder', ['brochure', 'promotional folder'], NULL, NULL),
  ('advertisement',       'advertisement|\badvert', ['advert'], NULL, NULL),
  ('feature_matrix',      'game features (comparison|matrix|presentation)|comparison chart by model', ['game features ', 'comparison chart by model'], NULL, NULL),
  ('feature_matrix',      'features? (list|explanation)', ['feature'], NULL, NULL),
  ('press_release',       'press release|website announcement', ['press release', 'website announcement'], NULL, NULL),
  ('price_card',          'price card|pricing card|plays per quarter|replay card|replays card', ['price card', 'pricing card', 'plays per quarter', 'replay card', 'replays card'], NULL, NULL),
  ('promotional_photo',   'promotional photo|promo photo|promotional postcard', ['promo'], NULL, NULL),

  ('instruction_card',    'instruction card|score card|game card', ['instruction card', 'score card', 'game card'], NULL, NULL),
  ('warranty',            'warrant', ['warrant'], NULL, NULL),
  ('packing_list',        'packing list|check off list|shipping paperwork|inspection slip', ['packing list', 'check off list', 'shipping paperwork', 'inspection slip'], NULL, NULL),
  ('certificate',         'certificate', ['certificate'], NULL, NULL),

  ('rules_of_play',       'rules of play|game rules|how.to.play|rules flowchart|game operation', ['rules of play', 'game rules', 'how', 'rules flowchart', 'game operation'], NULL, NULL),
  ('strategy_guide',      'strategy|tip sheet|quick tips|quick take|\bguide\b|rule ?sheet', ['strategy', 'tip sheet', 'quick tips', 'quick take', 'guide', 'rulesheet', 'rule sheet'], NULL, NULL),

  ('game_description',    'game (description|history)|description of game|narrative|proposed game theme', ['game ', 'description of game', 'narrative', 'proposed game theme'], NULL, NULL),
  ('interview',           'interview|discusses|discussing|\bq&a\b|letter from|open letter', ['interview', 'discuss', 'q&a', 'letter'], NULL, NULL),
  ('credits',             'personnel credits|autograph|contributors', ['personnel credits', 'autograph', 'contributors'], NULL, NULL),
  ('correspondence',      'inter-office|communication to|\bmemo\b', ['inter-office', 'communication to', 'memo'], NULL, NULL),

  ('artwork_scan',        'artwork|plastic scans|plastics|decal|apron art|spinner graphics|insert graphic', ['artwork', 'plastic', 'decal', 'apron art', 'spinner graphics', 'insert graphic'], NULL, NULL),
  ('photo_set',           'factory photos|additional (external|internal) pictures|image portfolio|images of', ['factory photos', 'additional ', 'image portfolio', 'images of'], NULL, NULL),

  ('game_audio',          '\btune\b|music|sound|audio', ['tune', 'music', 'sound', 'audio'], ['mp3', 'wav', 'zip'], '\brom|prom\b|speech chip|chip u[0-9]'),
  ('video',               'movie|video', ['movie', 'video'], ['mpg', 'mp4', 'mov', 'avi', 'wmv', 'zip'], NULL),
  ('rom_set',             '\brom(s|set)?\b|eprom|\bprom\b|speech chip|gamerom', ['rom', 'speech chip'], ['zip', 'bin', '716', '732', '764', 'hex'], NULL),

  ('accessory_kit',       'accessories|retrofit|\bkit\b', ['accessories', 'retrofit', 'kit'], NULL, NULL),
  ('supplement',          'addendum|errata|supplement', ['addendum', 'errata', 'supplement'], NULL, NULL),

  ('patent',              '\bpatent', ['patent'], ['pdf'], NULL),
  ('trade_article',       'article|journal|magazine|coin slot|automatic age', ['article', 'journal', 'magazine', 'coin slot', 'automatic age'], ['pdf', 'txt'], NULL),
  ('trade_article',       'der automat|spinning reels|newpaper|newspaper|monitor, vol|souvenir guide', ['der automat', 'spinning reels', 'newpaper', 'newspaper', 'monitor, vol', 'souvenir guide'], ['pdf', 'txt'], NULL)
) AS t(document_class, pattern, required_any, allow_containers, deny_pattern);

-- How wide a source's subject is, from the models that reference it. Shared
-- by every view that resolves a subject, so the thresholds are stated once.
--
-- NULL in, NULL out: a source whose identity could not be resolved has an
-- unknown scope, which is not the same as a scope of one model.
CREATE OR REPLACE MACRO ipdb_ref.document_subject_scope(models, manufacturers) AS (
  CASE
    WHEN models IS NULL THEN NULL
    WHEN manufacturers > 1 THEN 'multi_manufacturer'
    WHEN models > 1 THEN 'multi_model'
    ELSE 'single_model'
  END
);
