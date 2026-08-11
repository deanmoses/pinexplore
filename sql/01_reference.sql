-- Reference tables, macros, and exception lists.
-- Domain knowledge that doesn't come from any data file.

------------------------------------------------------------
-- Macros
------------------------------------------------------------

-- Normalize manufacturer names by stripping business suffixes.
-- Mirrors normalize_manufacturer_name() in bulk_utils.py.
-- Applied repeatedly to handle compound suffixes like "Sega Enterprises, Ltd."
CREATE OR REPLACE MACRO normalize_mfr_name(name) AS (
  lower(trim(
    regexp_replace(
      regexp_replace(
        regexp_replace(name,
          ',?\s+(?:Manufacturing|Electronics|Industries|Enterprises|Games|Pinball|Technologies|Company|Corporation|Incorporated|Limited|Inc\.?|Ltd\.?|Co\.?|LLC|GmbH|S\.?A\.?|s\.?p\.?a\.?|Kabushikigaisha|Automaten)\s*$',
          '', 'i'),
        ',?\s+(?:Manufacturing|Electronics|Industries|Enterprises|Games|Pinball|Technologies|Company|Corporation|Incorporated|Limited|Inc\.?|Ltd\.?|Co\.?|LLC|GmbH|S\.?A\.?|s\.?p\.?a\.?|Kabushikigaisha|Automaten)\s*$',
        '', 'i'),
      ',?\s+(?:Manufacturing|Electronics|Industries|Enterprises|Games|Pinball|Technologies|Company|Corporation|Incorporated|Limited|Inc\.?|Ltd\.?|Co\.?|LLC|GmbH|S\.?A\.?|s\.?p\.?a\.?|Kabushikigaisha|Automaten)\s*$',
      '', 'i')
  ))
);

-- Detect mojibake: UTF-8 multibyte characters misread as Latin-1.
-- Typical signature: Ã, Â, or Å followed by a non-ASCII byte.
-- Only apply to `name` fields, never to `aliases` (which intentionally
-- store the garbled variants for IPDB source matching).
CREATE OR REPLACE MACRO is_mojibake(s) AS (
  s IS NOT NULL AND regexp_matches(s, 'Ã[^\x00-\x7F]|Â[^\x00-\x7F]|Å[^\x00-\x7F]')
);

------------------------------------------------------------
-- Source-specific code mappings
------------------------------------------------------------

-- OPDB type code -> technology generation slug
CREATE OR REPLACE VIEW ref_opdb_technology_generation AS
SELECT * FROM (VALUES
  ('em', 'electromechanical'),
  ('ss', 'solid-state'),
  ('me', 'pure-mechanical')
) AS t(opdb_type, slug);

-- OPDB display code -> display type slug
CREATE OR REPLACE VIEW ref_opdb_display_type AS
SELECT * FROM (VALUES
  ('reels',        'score-reels'),
  ('lights',       'backglass-lights'),
  ('alphanumeric', 'alphanumeric'),
  ('cga',          'cga'),
  ('dmd',          'dot-matrix'),
  ('lcd',          'lcd')
) AS t(opdb_display, slug);

-- IPDB TypeShortName/Type -> technology generation slug
CREATE OR REPLACE VIEW ref_ipdb_technology_generation AS
SELECT * FROM (VALUES
  ('EM', NULL,                    'electromechanical'),
  ('SS', NULL,                    'solid-state'),
  (NULL, 'Pure Mechanical (PM)',  'pure-mechanical')
) AS t(type_short_name, type_full, slug);

------------------------------------------------------------
-- OPDB manufacturer resolution
------------------------------------------------------------

-- OPDB manufacturer ID → pindata manufacturer slug mapping.
-- For OPDB manufacturers whose name doesn't match a pindata manufacturer
-- (renames, merges, different brand names).
CREATE OR REPLACE VIEW ref_opdb_manufacturer_aliases AS
SELECT * FROM (VALUES
  (25,  'alvin-g'),                -- Alvin G. & Co → Alvin G.
  (37,  'bell-coin-matics'),       -- Bell Coin Matic → Bell Coin Matics
  (149, 'bem'),                    -- Bigliardini Elettronici Milano → BEM
  (82,  'century-consolidated-industries-company'), -- Cisco
  (71,  'coffee-mat'),             -- Coffee Mat → Coffee-Mat
  (19,  'esco'),                   -- Exhibit → ESCO
  (65,  'fascination-int-incorporated'), -- Fascination Game
  (50,  'komplett-flipper'),       -- Geiger → Komplett Flipper
  (63,  'giorgio-massiero'),       -- Giorgio Massiniero → Giorgio Massiero
  (138, 'ice'),                    -- Innovative Concepts (ICE) → ICE
  (31,  'international-concepts'), -- International → International Concepts
  (104, 'christian-tabart'),       -- K.C. Tabart → Christian Tabart
  (55,  'komplett-flipper'),       -- Komplett → Komplett Flipper
  (44,  'mac-sa'),                 -- Maguinas / Mac Pinball → MAC S.A.
  (108, 'marsaplay'),              -- Marsa Play → MarsaPlay
  (28,  'mr-game'),                -- Mr Game → Mr. Game
  (113, 'pmi'),                    -- Pinball Manufacturing Inc. → PMI
  (66,  'playmec'),                -- Playmec Flippers → Playmec
  (56,  'the-valley-company-subsidiary-of-walter-kidde-company-incorporated'), -- Valley
  (94,  'viza-mfg-inc')           -- Viza Manufacturing → Viza Mfg., Inc.
) AS t(opdb_manufacturer_id, manufacturer_slug);

-- Approved OPDB↔pindata manufacturer disagreements.
-- Cases where OPDB attributes a model to one manufacturer but pindata
-- correctly uses a different one (verified by research).
-- (opdb_manufacturer_id, pindata_manufacturer_slug, reason)
CREATE OR REPLACE VIEW ref_opdb_manufacturer_exceptions AS
SELECT * FROM (VALUES
  -- Segasa (15) vs Sonic: OPDB uses "Segasa" for post-rebrand games that
  -- were actually branded "Sonic" (Segasa d.b.a. Sonic). IPDB is correct.
  (15, 'sonic', 'OPDB uses parent name Segasa for Sonic-branded games'),
  -- Geiger (50) vs Komplett Flipper: Geiger-Automatenbau GmbH = A.H. Geiger Co.
  -- = Komplett Flipper brand. OPDB uses company name, pindata uses brand.
  (50, 'komplett-flipper', 'OPDB uses Geiger for Komplett Flipper brand'),
  -- Geiger (50) vs Professional Pinball: OPDB misattributes Challenger to Geiger
  (50, 'professional-pinball', 'OPDB misattributes to Geiger; IPDB says Professional Pinball'),
  -- Spooky Pinball (95) vs The Pinball Company: Jetsons was designed by
  -- The Pinball Company and manufactured by Spooky. IPDB credits designer.
  (95, 'the-pinball-company', 'Collaboration: designed by TPC, manufactured by Spooky'),
  -- Brunswick (40) vs Briarwood: Briarwood was a division of Brunswick.
  -- OPDB uses parent company name.
  (40, 'briarwood', 'OPDB uses parent Brunswick for Briarwood division games'),
  -- Midway (14) vs Bally: some Bally games manufactured by Midway.
  -- OPDB uses Midway, IPDB credits Bally.
  (14, 'bally', 'OPDB uses Midway for Bally-branded game'),
  -- Gottlieb (2) vs Alben: Alben was a French manufacturer/licensee.
  -- OPDB uses Gottlieb, IPDB credits Alben.
  (2, 'alben', 'OPDB uses Gottlieb for Alben-manufactured game'),
  -- Bell Games (20) vs Bell Coin Matics: related companies.
  (20, 'bell-coin-matics', 'OPDB uses Bell Games for Bell Coin Matics game'),
  -- Chicago Coin (3) vs Chicago Gaming: different eras of Chicago-based companies.
  (3, 'chicago-gaming', 'OPDB uses Chicago Coin for Chicago Gaming game'),
  -- Cic Play (4) vs Sentinel: related companies.
  (4, 'sentinel', 'OPDB uses Cic Play for Sentinel game'),
  -- Allied Leisure (49) vs LAI: LAI = Leisure & Allied Industries (Australian).
  (49, 'lai', 'OPDB uses Allied Leisure for LAI game'),
  -- Joctronic (90) vs Jocmatic: related Spanish companies.
  (90, 'jocmatic-sa', 'OPDB uses Joctronic for Jocmatic game'),
  -- Taito (73) vs Mecatronics: Brazilian Taito division.
  (73, 'mecatronics-aka-taito-brazil-a-division-of-taito', 'OPDB uses Taito for Brazilian division')
) AS t(opdb_manufacturer_id, manufacturer_slug, reason);

------------------------------------------------------------
-- Rejected IPDB themes
------------------------------------------------------------

-- Theme terms from any source that are not real themes
-- (metadata, filler, audience tags, gameplay/physical attributes).
CREATE OR REPLACE VIEW ref_themes_dropped AS
SELECT * FROM (VALUES
  ('Activities'),
  ('Children''s Games'),
  ('Commemorative'),
  ('Competition'),
  ('Family'),
  ('Fiction'),
  ('Fictional'),
  ('Fictional Character'),
  ('Fictional Characters'),
  ('Fun'),
  ('Guns'),
  ('Happiness'),
  ('Industry Inside Jokes'),
  ('Juvenilia'),
  ('Licensed'),
  ('Licensed Theme'),
  ('Payout'),
  ('People'),
  ('Recreation'),
  ('Weather'),
  -- OPDB keywords that are gameplay/physical attributes, not themes
  ('Widebody'),
  ('action-button'),
  ('staged-flippers'),
  ('street-level'),
  -- OPDB keywords that are tokenized machine names, not themes
  ('ball'),
  ('eight'),
  ('geriatric'),
  ('brock')
) AS t(theme);

------------------------------------------------------------
-- Rejected IPDB gameplay features
------------------------------------------------------------

-- Gameplay feature terms extracted from IPDB NotableFeatures that are
-- machine-specific mode names rather than general gameplay features.
CREATE OR REPLACE VIEW ref_gameplay_features_dropped AS
SELECT * FROM (VALUES
  ('tiger saw multiball',             'Machine-specific mode on Theatre of Magic (1995); a 2-ball multiball'),
  ('multiball modes',                 'Generic plural reference, not a distinct feature'),
  ('trunk multiball',                 'Machine-specific mode on Cirqus Voltaire (1997)'),
  ('trunk multiball w/vanish lock',   'Machine-specific mode variant on Cirqus Voltaire (1997)')
) AS t(feature, reason);

------------------------------------------------------------
-- Quality/tag cross-reference mappings
------------------------------------------------------------

-- OPDB features mapped to model fields rather than entity aliases.
-- Used by missing_tags_opdb to avoid false positives for features
-- that are already handled via dedicated model columns.
CREATE OR REPLACE VIEW ref_feature_other AS
SELECT * FROM (VALUES
  ('converted game',  'is_conversion', 'true')
) AS t(feature, model_field, field_value);

------------------------------------------------------------
-- Licensed theme overrides
------------------------------------------------------------

-- Titles that sources tag as "Licensed Theme" but are not actually licensed.
-- Investigated and rejected during franchise gap analysis.
CREATE OR REPLACE VIEW ref_not_licensed AS
SELECT * FROM (VALUES
  ('foxy-lady', 'Unlicensed rebrand of Game Plan Black Velvet leftover inventory'),
  ('king',      'Unlicensed Elvis Presley likeness on 40-unit Italian conversion kit by Bell Coin Matics')
) AS t(title_slug, reason);

------------------------------------------------------------
-- Document classification vocabulary
------------------------------------------------------------

-- What kind of source a class describes. A manufacturer's manual, a patent and
-- a trade-press article are each addressed differently — by document, by
-- jurisdiction and number, by publication and issue — so the kind is declared
-- rather than inferred from the class name.
CREATE OR REPLACE VIEW ref_source_kind AS
SELECT * FROM (VALUES
  ('document',      'A document published about a machine, system or maker'),
  ('patent',        'A government patent publication'),
  ('trade_article', 'An article or advertisement in a trade periodical')
) AS t(source_kind, description);

-- Every class, including grouping nodes. `marketing` and `service_reference`
-- carry no detection pattern and never match a filename: they exist only as
-- parents, which is why the vocabulary cannot live in the pattern table.
CREATE OR REPLACE VIEW ref_document_class AS
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

-- Parent edges, one row per edge so a class may later carry two parents without
-- a schema change (`feature_matrix` is plausibly both marketing and service
-- reference, though nothing in the trove has forced that yet).
--
-- An edge is only cross-checkable against the data when the child's name
-- lexically contains the parent's — "Operations Manual" contains "Manual", so
-- the patterns co-fire and `ipdb_document_class_subsumption` corroborates the
-- edge. A wiring diagram is a schematic without saying so, so a low percentage
-- there is silence, not contradiction.
CREATE OR REPLACE VIEW ref_document_class_parent AS
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

-- How a class is detected in an IPDB filename. Separate from the vocabulary
-- above because the two have different lifetimes: the classes and their edges
-- describe documents anywhere, while these patterns read one source's naming
-- habits. Several rows may name the same class.
--
-- Patterns **deliberately overlap**. "Schematic Manual" is both, and a
-- first-match-wins rule would hide that behind whichever row happened to sort
-- first, so every match is emitted and the collisions are the useful output.
--
-- `allow_containers` lets the delivery axis constrain the class axis, set only
-- where it discriminates: "sound" appears in a factory kit sheet and an EPROM
-- dump as readily as in a recording, and the extension separates them. It is
-- also what keeps a jpg named "Rodeo Patents Notice" — a photograph of numbers
-- printed on a cabinet — out of `patent`.
--
-- The picture/document line is deliberately not drawn for the marketing
-- classes. IPDB's gallery is where its flyers and instruction cards live, as
-- scans, so `flyer` and its neighbours match images on purpose.
--
-- `deny_pattern` covers what the container cannot: "Sound ROMs V1.0" and
-- "Monster Bash All Sound Files" are both zips, and only the word ROM says
-- which one is a chip dump.
--
-- `required_any` is what makes the match affordable. DuckDB compiles a regex
-- once when the pattern is a literal, but the pattern here comes from a column,
-- so it recompiles per row — every filename against every pattern is millions
-- of RE2 constructions, which costs a thousand times more than the matching.
-- These are lowercase literals, at least one of which must appear in anything
-- the pattern can match, so a plain substring test can throw out almost every
-- pair before a regex is built.
--
-- The list is a *necessary* condition, never a sufficient one — the regex still
-- decides. Widen it when in doubt: a needle that is too broad only costs time,
-- while one that is too narrow silently drops real matches. Every top-level
-- branch of the pattern needs its own needle, so `newpaper|newspaper` needs
-- both spellings, and a branch's needle must be a literal the branch cannot
-- match without ("how" for `how.to.play`, not "how to play").
CREATE OR REPLACE VIEW ref_document_class_pattern AS
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

-- How wide a source's subject is, from the machines that reference it. Shared
-- by every view that resolves a subject, so the thresholds are stated once.
--
-- NULL in, NULL out: a source whose identity could not be resolved has an
-- unknown scope, which is not the same as a scope of one machine.
CREATE OR REPLACE MACRO document_subject_scope(machines, manufacturers) AS (
  CASE
    WHEN machines IS NULL THEN NULL
    WHEN manufacturers > 1 THEN 'multi_manufacturer'
    WHEN machines > 1 THEN 'multi_machine'
    ELSE 'single_machine'
  END
);
