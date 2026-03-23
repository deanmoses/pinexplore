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

-- OPDB manufacturer ID → pinbase manufacturer slug mapping.
-- For OPDB manufacturers whose name doesn't match a pinbase manufacturer
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

-- Approved OPDB↔pinbase manufacturer disagreements.
-- Cases where OPDB attributes a model to one manufacturer but pinbase
-- correctly uses a different one (verified by research).
-- (opdb_manufacturer_id, pinbase_manufacturer_slug, reason)
CREATE OR REPLACE VIEW ref_opdb_manufacturer_exceptions AS
SELECT * FROM (VALUES
  -- Segasa (15) vs Sonic: OPDB uses "Segasa" for post-rebrand games that
  -- were actually branded "Sonic" (Segasa d.b.a. Sonic). IPDB is correct.
  (15, 'sonic', 'OPDB uses parent name Segasa for Sonic-branded games'),
  -- Geiger (50) vs Komplett Flipper: Geiger-Automatenbau GmbH = A.H. Geiger Co.
  -- = Komplett Flipper brand. OPDB uses company name, pinbase uses brand.
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
  ('Land'),
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
