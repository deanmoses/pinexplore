-- 08_popularity.sql — Title popularity: composite scoring across multiple signals.
-- Depends on: 01_reference.sql, 02_raw.sql, 03_staging.sql, 05_compare.sql
--
-- Two views:
--   title_popularity_signals — raw per-title signals for inspection
--   title_popularity         — weighted composite score with percentile breakdowns
--
-- Known limitations:
--   - production_quantity is missing for ~80% of models, including most modern
--     Stern titles. Titles without production data receive 0th percentile on the
--     heaviest-weighted signal, which significantly suppresses their score.
--   - IPDB ratings cover ~13% of machines. Unrated titles get 0th percentile.
--   - Fandom wiki covers ~380 titles. Absence ≠ unpopularity.
--   - Pinball Map skews toward currently-operating machines.

------------------------------------------------------------
-- Raw popularity signals per title
------------------------------------------------------------

CREATE OR REPLACE VIEW title_popularity_signals AS
WITH
  production AS (
    SELECT
      title_slug,
      sum(CAST(production_quantity AS INT)) AS total_production,
      count(*) FILTER (WHERE production_quantity IS NOT NULL) AS models_with_production
    FROM models
    WHERE title_slug IS NOT NULL
    GROUP BY title_slug
  ),
  model_counts AS (
    SELECT
      title_slug,
      count(*) AS model_count,
      count(*) FILTER (WHERE variant_of IS NOT NULL) AS variant_count,
      count(*) FILTER (WHERE is_conversion) AS conversion_count,
      count(*) FILTER (WHERE remake_of IS NOT NULL) AS remake_count
    FROM models
    WHERE title_slug IS NOT NULL
    GROUP BY title_slug
  ),
  ratings AS (
    SELECT
      m.title_slug,
      round(avg(i.AverageFunRating), 2) AS avg_rating
    FROM models m
    JOIN ipdb_machines i ON m.ipdb_id = i.IpdbId
    WHERE m.title_slug IS NOT NULL AND i.AverageFunRating IS NOT NULL
    GROUP BY m.title_slug
  ),
  longevity AS (
    SELECT
      title_slug,
      min(year) AS first_year,
      max(year) AS last_year,
      max(year) - min(year) AS year_span
    FROM models
    WHERE title_slug IS NOT NULL AND year IS NOT NULL
    GROUP BY title_slug
  ),
  series AS (
    SELECT slug AS title_slug, series_slug
    FROM titles
    WHERE series_slug IS NOT NULL
  ),
  fandom AS (
    SELECT
      title_slug,
      max(length(fandom_wikitext)) AS fandom_article_length
    FROM compare_games_fandom
    WHERE title_slug IS NOT NULL
    GROUP BY title_slug
  ),
  pinball_map AS (
    SELECT
      m.title_slug,
      count(DISTINCT pm.id) AS pinball_map_machine_count
    FROM models m
    JOIN pinballmap_machines pm ON m.ipdb_id = pm.ipdb_id
    WHERE m.title_slug IS NOT NULL
    GROUP BY m.title_slug
  )
SELECT
  t.slug AS title_slug,
  t.name AS title_name,
  COALESCE(p.total_production, 0) AS total_production,
  COALESCE(p.models_with_production, 0) AS models_with_production,
  COALESCE(mc.model_count, 0) AS model_count,
  COALESCE(mc.variant_count, 0) AS variant_count,
  COALESCE(mc.conversion_count, 0) AS conversion_count,
  COALESCE(mc.remake_count, 0) AS remake_count,
  r.avg_rating,
  l.first_year,
  l.last_year,
  COALESCE(l.year_span, 0) AS year_span,
  s.series_slug IS NOT NULL AS in_series,
  f.fandom_article_length IS NOT NULL AS on_fandom,
  COALESCE(f.fandom_article_length, 0) AS fandom_article_length,
  COALESCE(pm.pinball_map_machine_count, 0) AS pinball_map_machine_count
FROM titles t
LEFT JOIN production p ON t.slug = p.title_slug
LEFT JOIN model_counts mc ON t.slug = mc.title_slug
LEFT JOIN ratings r ON t.slug = r.title_slug
LEFT JOIN longevity l ON t.slug = l.title_slug
LEFT JOIN series s ON t.slug = s.title_slug
LEFT JOIN fandom f ON t.slug = f.title_slug
LEFT JOIN pinball_map pm ON t.slug = pm.title_slug;

------------------------------------------------------------
-- Composite popularity score
--
-- Scoring approach:
--   1. Normalize each signal to a 0–1 value (see "normalized" CTE)
--   2. Rank each normalized value as a percentile across ALL titles
--      (missing data = 0, ranked at bottom — not excluded)
--   3. Multiply each percentile by its weight, sum, divide by total weight
--
-- To change weights: edit the "weights" CTE below. Everything else
-- (denominator, header docs) derives from it automatically.
------------------------------------------------------------

CREATE OR REPLACE VIEW title_popularity AS
WITH
  -- === WEIGHTS: single source of truth for tuning ===
  -- Continuous signals: percentile-ranked then weighted
  -- Binary signals: contribute their full weight when present
  weights AS (
    SELECT * FROM (VALUES
      --              weight  notes
      ('production',    5.0), -- total units produced; strongest direct signal
      ('rating',        2.5), -- IPDB avg fun rating percentile
      ('fandom',        2.0), -- Fandom wiki article length as depth proxy
      ('model_count',   1.0), -- distinct models, log-scaled
      ('longevity',     1.0), -- year span, capped at 15
      ('pinball_map',   1.0), -- machine count on Pinball Map
      ('variant',       0.5), -- variant count, log-scaled
      ('remake',        1.5), -- binary: title has been remade
      ('series',        0.5), -- binary: title belongs to a series
      ('conversion',    0.5)  -- conversion kits, capped at weight max
    ) AS t(signal, weight)
  ),
  total_weight AS (
    SELECT sum(weight) AS w FROM weights
  ),
  signals AS (FROM title_popularity_signals),

  -- Normalize raw values before ranking:
  --   ln() compresses counts so 1→2 matters more than 7→8
  --   least() caps outliers
  --   COALESCE() treats missing as 0
  normalized AS (
    SELECT
      *,
      total_production AS norm_production,
      ln(model_count + 1) AS norm_model_count,
      ln(variant_count + 1) AS norm_variant,
      COALESCE(avg_rating, 0) AS norm_rating,
      least(year_span, 15) AS norm_longevity,
      fandom_article_length AS norm_fandom,
      pinball_map_machine_count AS norm_pinball_map
    FROM signals
  ),

  -- Percentile-rank each normalized signal across all titles
  ranked AS (
    SELECT
      *,
      percent_rank() OVER (ORDER BY norm_production) AS production_pct,
      percent_rank() OVER (ORDER BY norm_model_count) AS model_count_pct,
      percent_rank() OVER (ORDER BY norm_variant) AS variant_pct,
      percent_rank() OVER (ORDER BY norm_rating) AS rating_pct,
      percent_rank() OVER (ORDER BY norm_longevity) AS longevity_pct,
      percent_rank() OVER (ORDER BY norm_fandom) AS fandom_depth_pct,
      percent_rank() OVER (ORDER BY norm_pinball_map) AS pinball_map_pct
    FROM normalized
  ),

  scored AS (
    SELECT
      r.*,
      (
        r.production_pct   * w_production.weight
        + r.model_count_pct * w_model_count.weight
        + r.variant_pct     * w_variant.weight
        + r.rating_pct      * w_rating.weight
        + r.longevity_pct   * w_longevity.weight
        + r.fandom_depth_pct * w_fandom.weight
        + r.pinball_map_pct * w_pinball_map.weight
        + CASE WHEN r.remake_count > 0 THEN w_remake.weight ELSE 0 END
        + CASE WHEN r.in_series THEN w_series.weight ELSE 0 END
        + least(r.conversion_count * 0.25, w_conversion.weight)
      ) / tw.w AS popularity_score,

      (CASE WHEN r.total_production > 0 THEN 1 ELSE 0 END
        + 1  -- model_count always present
        + CASE WHEN r.variant_count > 0 THEN 1 ELSE 0 END
        + CASE WHEN r.avg_rating IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN r.year_span > 0 THEN 1 ELSE 0 END
        + CASE WHEN r.fandom_article_length > 0 THEN 1 ELSE 0 END
        + CASE WHEN r.pinball_map_machine_count > 0 THEN 1 ELSE 0 END
        + CASE WHEN r.remake_count > 0 THEN 1 ELSE 0 END
        + CASE WHEN r.in_series THEN 1 ELSE 0 END
        + CASE WHEN r.conversion_count > 0 THEN 1 ELSE 0 END
      ) AS signal_count
    FROM ranked r
    CROSS JOIN total_weight tw
    -- Look up each weight by name
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'production') w_production
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'model_count') w_model_count
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'variant') w_variant
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'rating') w_rating
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'longevity') w_longevity
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'fandom') w_fandom
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'pinball_map') w_pinball_map
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'remake') w_remake
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'series') w_series
    CROSS JOIN (SELECT weight FROM weights WHERE signal = 'conversion') w_conversion
  )
SELECT
  title_slug,
  title_name,
  round(popularity_score, 4) AS popularity_score,
  signal_count,
  -- Raw signals
  total_production,
  model_count,
  variant_count,
  remake_count,
  conversion_count,
  year_span,
  first_year,
  last_year,
  in_series,
  on_fandom,
  fandom_article_length,
  pinball_map_machine_count,
  -- Percentile breakdowns
  round(production_pct, 3) AS production_pct,
  round(model_count_pct, 3) AS model_count_pct,
  round(variant_pct, 3) AS variant_pct,
  round(rating_pct, 3) AS rating_pct,
  round(longevity_pct, 3) AS longevity_pct,
  round(fandom_depth_pct, 3) AS fandom_depth_pct,
  round(pinball_map_pct, 3) AS pinball_map_pct
FROM scored
ORDER BY popularity_score DESC;
