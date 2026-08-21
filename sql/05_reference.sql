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

CREATE OR REPLACE VIEW ipdb_ref.technology_generation AS
SELECT * FROM (VALUES
  ('EM', NULL,                    'electromechanical'),
  ('SS', NULL,                    'solid-state'),
  (NULL, 'Pure Mechanical (PM)',  'pure-mechanical')
) AS t(type_short_name, type_full, slug);

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
-- and carried only by archive pages. The whole dropdown is transcribed because
-- the cached corpus is partial:
-- <https://www.ipdb.org/search.pl?specialty=12&sortby=date&searchtype=advanced>.
--
-- `target_entity_type` names a Flipcommons entity type. `model-relationship` is
-- the exception: it says only that the model has an edge whose donor still needs
-- research. A lowercase slug-like target is intended as a catalog `public_id`;
-- IPDB's own display wording marks unresolved vocabulary. This build can check
-- only that spelling convention; flippatch must check whether the target exists.
--
-- `target_is_public_id` is NULL on the relationship rows. A relationship type is
-- neither resolvable nor missing vocabulary, and its slug-like spelling would
-- otherwise read as a public_id that no entity can satisfy. NULL drops it from
-- both `WHERE target_is_public_id` and `WHERE NOT target_is_public_id`, so
-- neither the resolve nor the worklist has to special-case it.
CREATE OR REPLACE VIEW ipdb_ref.specialty AS
SELECT
  ipdb_specialty,
  target_entity_type,
  target_public_id,
  CASE WHEN target_entity_type <> 'model-relationship'
       THEN regexp_matches(target_public_id, '^[a-z0-9][a-z0-9_-]*$')
  END AS target_is_public_id
FROM (VALUES
  ('Add-A-Ball',                          'reward-type',        'add-a-ball'),
  ('Bagatelle',                           'game-format',        'bagatelle'),
  ('Bat Game',                            'game-format',        'pitch-and-bat'),
  ('Bingo Machine',                       'game-format',        'bingo-pinball'),
  ('Cocktail Table',                      'cabinet',            'cocktail'),
  ('Conversion Kit',                      'model-relationship', 'conversion_kit'),
  ('Converted Game',                      'model-relationship', 'conversion'),
  ('Cue Game',                            'game-format',        'Cue Game'),
  ('Flipperless',                         'tag',                'Flipperless'),
  ('Gun Game',                            'game-format',        'gun-game'),
  ('Head-to-Head Play',                   'gameplay-feature',   'head-to-head'),
  ('Horserace Game',                      'game-format',        'Horserace Game'),
  ('Mechanical Backbox Animation',        'gameplay-feature',   'mechanical-backbox-animations'),
  ('Non-Commercial Machine [Home Model]', 'tag',                'home-use'),
  ('Not A Pinball',                       'game-format',        'Not A Pinball'),
  ('Novelty Play',                        'reward-type',        'novelty'),
  ('One Ball Game',                       'game-format',        'one-ball'),
  -- IPDB's single word covers what we split into `cash-payout` and
  -- `merchant-paid`, and the page does not say which. Reading the models is the
  -- only way to tell them apart, so this stays a worklist rather than guessing.
  ('Payout Machine',                      'reward-type',        'Payout Machine'),
  ('Re-themed Game',                      'model-relationship', 'retheme'),
  ('Redemption Game',                     'reward-type',        'ticket-payout'),
  ('Rolldown Game',                       'game-format',        'rolldown'),
  ('Shaker Ball Machine',                 'game-format',        'Shaker Ball Machine'),
  -- One IPDB heading over two of our cabinets, `tabletop` and `countertop`.
  -- Same shape as Payout Machine: per-model research, not new vocabulary.
  ('Table Top/Counter Game',              'cabinet',            'Table Top/Counter Game'),
  ('Vertical Pinball Machine',            'cabinet',            'Vertical Pinball Machine'),
  ('Widebody',                            'tag',                'widebody'),
  ('WWII Contract',                       'tag',                'WWII Contract'),
  ('Zipper Flippers',                     'gameplay-feature',   'zipper-flippers')
) AS t(ipdb_specialty, target_entity_type, target_public_id);

-- Approved OPDB/catalog manufacturer disagreements: cases where OPDB attributes a
-- model to one manufacturer and the catalog correctly uses another, each verified
-- by research.
--
-- Nothing here reads it: a comparison exception belongs beside the comparison in
-- flippatch, where the OPDB half of that layer does not exist yet.
--
-- These slugs rot silently. `mecatronics-aka-taito-brazil-a-division-of-taito`
-- already names a manufacturer Flipcommons renamed to `mecatronics`, so that row
-- has stopped matching and its disagreements read as unexplained. Whatever
-- inherits this list needs a check that a slug still resolves.
CREATE OR REPLACE VIEW opdb_ref.manufacturer_exceptions AS
SELECT * FROM (VALUES
  (15, 'sonic', 'OPDB uses parent name Segasa for Sonic-branded games'),
  -- Geiger-Automatenbau GmbH = A.H. Geiger Co. = the Komplett Flipper brand.
  (50, 'komplett-flipper', 'OPDB uses Geiger for Komplett Flipper brand'),
  (50, 'professional-pinball', 'OPDB misattributes to Geiger; IPDB says Professional Pinball'),
  (95, 'the-pinball-company', 'Collaboration: designed by TPC, manufactured by Spooky'),
  (40, 'briarwood', 'OPDB uses parent Brunswick for Briarwood division games'),
  (14, 'bally', 'OPDB uses Midway for Bally-branded game'),
  (2, 'alben', 'OPDB uses Gottlieb for Alben-manufactured game'),
  (20, 'bell-coin-matics', 'OPDB uses Bell Games for Bell Coin Matics game'),
  (3, 'chicago-gaming', 'OPDB uses Chicago Coin for Chicago Gaming game'),
  (4, 'sentinel', 'OPDB uses Cic Play for Sentinel game'),
  -- LAI = Leisure & Allied Industries, Australian.
  (49, 'lai', 'OPDB uses Allied Leisure for LAI game'),
  (90, 'jocmatic-sa', 'OPDB uses Joctronic for Jocmatic game'),
  (73, 'mecatronics-aka-taito-brazil-a-division-of-taito', 'OPDB uses Taito for Brazilian division')
) AS t(opdb_manufacturer_id, manufacturer_slug, reason);

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
