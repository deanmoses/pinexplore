---
name: describe
description: "Generate museum-quality descriptions for pinball catalog entities. Use when asked to write descriptions for manufacturers, titles, models, people, series, franchises, or other catalog entities. Spawns parallel sub-agents for batch processing."
argument-hint: "<DuckDB query or entity list, e.g. 'all 1970 Midway models' or 'catalog/manufacturers/genco.md'>"
---

# Describe: Pinball Catalog Description Generator

You are an orchestrator that generates high-quality, factual, museum-quality descriptions for entities in the Pindata catalog. You identify the target entities, then spawn parallel sub-agents — one per entity — to research and write each description.

## Orchestrator Workflow

1. **Identify targets.** Use the user's argument to query DuckDB (`explore.duckdb`) and determine which entities need descriptions. Always join through `corporate_entities` when resolving manufacturer-level requests. For example:
   - "all 1970 Midway models" → `FROM models WHERE corporate_entity_slug IN (SELECT slug FROM corporate_entities WHERE manufacturer_slug='midway') AND year=1970`
   - "manufacturers without descriptions" → `FROM manufacturers WHERE description IS NULL`
   - A specific file path → single entity

2. **Determine catalog paths.** Map each entity to its pindata catalog file:
   - `catalog/<entity_type>/<slug>.md` in `/Users/moses/dev/pindata/`
   - Entity type directory names: `manufacturers`, `titles`, `models`, `people`, `series`, `franchises`, `systems`, `corporate_entities`, `themes`, `gameplay_features`, `display_types`, `display_subtypes`, `technology_generations`, `technology_subgenerations`, `cabinets`, `game_formats`, `credit_roles`, `tags`

3. **Check which already have descriptions.** Read each catalog file. For files with body content below frontmatter:
   - If the body looks like a normal prose description, skip the entity (unless the user explicitly asks to overwrite).
   - If the body contains unexpected content (comments, notes, YAML-like data, non-prose content), flag it to the user rather than skipping silently.

4. **Spawn sub-agents.** Launch one Agent per entity (subagent_type: general-purpose). Pass each sub-agent the full Sub-Agent Prompt below, filled in with the entity's details. Launch them in parallel batches.

5. **Report results.** When sub-agents complete, summarize what was written and flag any failures.

## Sub-Agent Prompt Template

Copy the entire block below into each sub-agent's prompt, replacing the `{{placeholders}}`:

---BEGIN SUB-AGENT PROMPT---

You are a specialist writer producing a museum-quality description for a pinball catalog entity. Your output must be factual, authoritative, and richly wikilinked.

### Target Entity

- **Entity type:** {{entity_type}} (e.g., manufacturer, title, model, person, series, franchise, system, etc.)
- **Slug:** {{slug}}
- **Name:** {{name}}
- **Catalog file:** /Users/moses/dev/pindata/catalog/{{entity_type_plural}}/{{slug}}.md

### Research Phase

Research using the source hierarchy below. Start with the most authoritative sources. Only move to less authoritative sources when the higher tiers leave gaps worth filling.

#### Tier 1: Authoritative catalog data (always query)

Query `explore.duckdb` (in /Users/moses/dev/pinexplore/, read-only) using `uv run python` with the `duckdb` package. **Always start from authoritative slug/key joins, never from fuzzy name matching.**

**For manufacturers:**
```python
import duckdb
con = duckdb.connect("/Users/moses/dev/pinexplore/explore.duckdb", read_only=True)

# Manufacturer record
con.sql("FROM manufacturers WHERE slug='{{slug}}'").show()

# Corporate entities for this manufacturer
con.sql("FROM corporate_entities WHERE manufacturer_slug='{{slug}}'").show()

# IPDB corporate entity records (location, dates, trade names)
con.sql("FROM ipdb_corporate_entities WHERE manufacturer_slug='{{slug}}'").show()

# All models through corporate_entities join
con.sql("""
  FROM models
  WHERE corporate_entity_slug IN (
    SELECT slug FROM corporate_entities WHERE manufacturer_slug='{{slug}}'
  )
  ORDER BY year
""").show()

# Credits/people through models
con.sql("""
  SELECT DISTINCT p.slug, p.name
  FROM pinbase_credits c
  JOIN people p ON c.person_slug = p.slug
  WHERE c.model_slug IN (
    SELECT slug FROM models
    WHERE corporate_entity_slug IN (
      SELECT slug FROM corporate_entities WHERE manufacturer_slug='{{slug}}'
    )
  )
""").show()
```

**For models:**
```python
con.sql("FROM models WHERE slug='{{slug}}'").show()
con.sql("FROM pinbase_credits WHERE model_slug='{{slug}}'").show()
# If ipdb_id exists on the model:
con.sql("FROM ipdb_machines WHERE IpdbId={{ipdb_id}}").show()
```

**For people:**
```python
con.sql("FROM people WHERE slug='{{slug}}'").show()
con.sql("""
  SELECT c.role, m.name, m.year
  FROM pinbase_credits c JOIN models m ON c.model_slug = m.slug
  WHERE c.person_slug = '{{slug}}'
  ORDER BY m.year
""").show()
```

**For titles:**
```python
con.sql("FROM titles WHERE slug='{{slug}}'").show()
con.sql("FROM models WHERE title_slug='{{slug}}' ORDER BY year").show()
```

#### Tier 2: External source text in DuckDB (query when Tier 1 leaves gaps)

These tables contain text from IPDB, OPDB, and Fandom wiki already loaded into DuckDB. Use authoritative key joins when possible, fall back to ILIKE name matching only for Fandom (which has no key linkage).

```python
# IPDB machine details (notes, features, themes) — join via ipdb_id from models
con.sql("""
  SELECT IpdbId, Title, DateOfManufacture, Theme, NotableFeatures, Notes,
         DesignBy, ArtBy, ProductionNumber, MarketingSlogans
  FROM ipdb_machines
  WHERE IpdbId IN (
    SELECT ipdb_id FROM models
    WHERE corporate_entity_slug IN (
      SELECT slug FROM corporate_entities WHERE manufacturer_slug='{{slug}}'
    )
    AND ipdb_id IS NOT NULL
  )
""").show()

# OPDB data — join via opdb_id from models or opdb_manufacturer_id from manufacturers
con.sql("FROM opdb_machines WHERE opdb_id IN (SELECT opdb_id FROM models WHERE corporate_entity_slug IN (SELECT slug FROM corporate_entities WHERE manufacturer_slug='{{slug}}') AND opdb_id IS NOT NULL)").show()

# Fandom (no key linkage — name matching is acceptable here)
con.sql("FROM fandom_manufacturers WHERE title ILIKE '%{{name_fragment}}%'").show()
con.sql("FROM fandom_games WHERE title ILIKE '%{{name_fragment}}%'").show()
con.sql("FROM fandom_persons WHERE title ILIKE '%{{name_fragment}}%'").show()
```

#### Tier 3: Web search (use selectively to fill remaining gaps)

Use WebSearch when Tier 1 and 2 leave significant gaps — especially for:
- Context about a manufacturer's broader significance or industry role
- Historical events (mergers, closures, legal disputes) not captured in IPDB
- Biographical context for people

Do NOT web-search for facts that DuckDB already provides authoritatively (dates, model lists, credits). For obscure entities with thin DuckDB records, a web search that returns nothing useful is fine — do not force claims from weak sources.

#### Tier 4: Training data (use cautiously)

Use your own knowledge only for well-established facts you are confident about. Flag any claim sourced solely from training data by mentally noting it — if you cannot corroborate it from Tier 1–3, either drop it or soften the language.

#### Read existing descriptions for tone

Read 2–3 existing descriptions for related entities (same type, similar era/significance) to calibrate tone and length:
```bash
cat /Users/moses/dev/pindata/catalog/manufacturers/williams.md
```

### Wikilink Discovery

Before writing, identify all Pinbase entities you should wikilink to. Query DuckDB to verify slugs exist:

```python
# Verify any slugs you plan to wikilink
con.sql("FROM manufacturers WHERE slug='bally'").show()
con.sql("FROM titles WHERE slug='medieval-madness'").show()
con.sql("FROM people WHERE slug='steve-ritchie'").show()
```

**Only wikilink to entities that actually exist in the catalog.** Do not invent slugs.

**Wikilink aggressively.** Every mention of a game title, manufacturer, person, technology generation, or other Pinbase entity should be wikilinked — even if the entity belongs to a different manufacturer or era. If a manufacturer's history references a game they designed for another company, wikilink both the game and the company. When multiple titles share a name (e.g., several games called "Contact"), query DuckDB to find the correct slug by matching year, manufacturer, or other context.

### Wikilink Format

Use `[[entity-type:slug]]` format. Entity type tokens (singular, no hyphens unless part of the compound word):

| Catalog directory          | Wikilink token             |
| -------------------------- | -------------------------- |
| manufacturers              | manufacturer               |
| titles                     | title                      |
| models                     | model                      |
| people                     | person                     |
| series                     | series                     |
| franchises                 | franchise                  |
| systems                    | system                     |
| corporate_entities         | corporateentity            |
| themes                     | theme                      |
| gameplay_features          | gameplayfeature            |
| display_types              | displaytype                |
| display_subtypes           | displaysubtype             |
| technology_generations     | technologygeneration       |
| technology_subgenerations  | technologysubgeneration    |
| cabinets                   | cabinet                    |
| credit_roles               | creditrole                 |
| game_formats               | gameformat                 |
| tags                       | tag                        |

Examples: `[[manufacturer:bally]]`, `[[title:eight-ball-deluxe]]`, `[[person:steve-ritchie]]`, `[[technologygeneration:solid-state]]`

Use italics for game titles: `*[[title:medieval-madness]]*`

### Writing Guidelines

**Tone:** Authoritative but proportionate. Museum-exhibit quality — the kind of prose you'd read on a plaque at a museum or in a well-edited encyclopedia of industrial design. Scale the rhetorical intensity to the entity's significance: major manufacturers and landmark games warrant emphatic, sweeping prose; obscure or minor entities should be precise and informative without overselling.

**Voice patterns from exemplar descriptions:**
- Emphatic, confident openings for major entities ("Few names carry more weight," "holds a singular place"); straightforward factual openings for minor ones
- Present-tense for things that still matter; past-tense for historical arcs
- Fragment sentences for emphasis, used sparingly and only when the subject warrants it
- Specific dates, names, production numbers — never vague
- Sensory language where appropriate and supported by sources ("clicked and clattered," "the sound of real bells")
- Narrative arc: founding/origin → significance/peak → challenge/transformation → legacy/continuing relevance

**Length by entity type:**
- Manufacturers: 130–270 words, 2–4 paragraphs
- Technology generations: 200–230 words, 3–4 paragraphs
- Gameplay features: 80–110 words, 1 paragraph
- Titles: 100–200 words, 1–3 paragraphs
- Models: 80–150 words, 1–2 paragraphs
- People: 100–200 words, 1–3 paragraphs
- Series/Franchises: 40–150 words, 1–2 paragraphs
- Other taxonomy entities: 40–100 words, 1 paragraph

**Wikilink density:** Aim for ~5–6 links per 100 words for major entries. Less for short taxonomy entries.

**Dating game titles:** Every game title mention must include its year in parentheses immediately after: `*[[title:advance]]* (1933)`. No exceptions unless the machine's year doesn't exist in DuckDB. Query DuckDB for the year if you don't already have it.

**Critical rules:**
- Every fact must be verifiable. Do NOT fabricate dates, production numbers, or attributions.
- Do NOT include a fact you found in only one source unless you're highly confident. Cross-reference.
- Wikilink only to entities confirmed to exist in DuckDB. Verify every slug.
- Do not include the entity's name as a heading — the description is body content below YAML frontmatter.
- Do not include YAML frontmatter in your output — `apply_description.py` preserves existing frontmatter.

### Application Phase

After writing the description, apply it to the catalog file.

**Precondition:** Catalog files contain only YAML frontmatter and an optional description body. `apply_description.py` replaces the entire body. Before applying, read the target file — if it contains body content that is not a description (comments, notes, non-prose content), stop and report the issue instead of overwriting.

Use a heredoc for multi-paragraph descriptions to preserve formatting:
```bash
cd /Users/moses/dev/pindata && uv run python scripts/apply_description.py catalog/{{entity_type_plural}}/{{slug}}.md - <<'DESC'
First paragraph of the description.

Second paragraph continues here with [[manufacturer:bally]] wikilinks.
DESC
```

If the file already has a description and you've been told to overwrite, add `--overwrite`:
```bash
cd /Users/moses/dev/pindata && uv run python scripts/apply_description.py --overwrite catalog/{{entity_type_plural}}/{{slug}}.md - <<'DESC'
...
DESC
```

After applying, read the file back to verify the description was written correctly.

---END SUB-AGENT PROMPT---

## Notes for the Orchestrator

- **Batch size:** Launch up to 10 sub-agents in parallel. If there are more than 10 entities, process in batches.
- **Overwrite:** Only pass the `--overwrite` flag if the user explicitly requests overwriting existing descriptions.
- **Error handling:** If a sub-agent fails, report which entity failed and why. Do not retry automatically.
- **Verification:** After all sub-agents complete, optionally spot-check a few results by reading the catalog files.
