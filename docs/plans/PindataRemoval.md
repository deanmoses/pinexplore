# Moving off Pindata

## Context

Pinexplore's DuckDB database is currently geared towards comparing the [Pindata JSON](~/dev/pindata/) and external sources like IPDB OPDB Pinside etc, to see what is missing or different.

This is a problem because Pindata is obsolete. It lived a good life bootstrapping Flipcommons, but after we went live in April 2026, we stopped updating it or looking at it. The live database in Flipcommons is now the source of truth for our catalog data, we haven't touched the Pindata repo since.

We haven't used Pinexplore's DuckDB database much since, except to sometimes look at the original IPDB / OPDB data -- never the Pindata portions of the database. But now, we're at a point where we're acquiring additional data from external sources, such as:

- More recent Xantari dumps of IPBD
- Scrapes of IPDB from archive.org that contain more fields than the Xantari dumps
- OPDB changelogs

Instead of comparing this data with Pindata's JSON, we need to compare it with Flipcommons' database.

For example, we just integrated a new Xantari IPDB dump into Pinexplore's DuckDB. That added logic to merge multiple Xantari dumps together, to present a more cohesive view, and to catch missing records and other 'bad' changes in the new dump. The new merged IPDB data set fails checks against **Pindata** because it has new data, but that comparison is meaningless. What I want to know is whether it fails similar checks against **Flipcommons**.

## Design

There's an analytics system over Flipcommons: `~/dev/flipcommons/scripts/analysis/README.md`. This is what needs to integrate with Pinexplore's DuckDB.

But rather than reference Flipcommons analytics from Pinexplore's DuckDB, we need to rejigger how the functionality currently living in Pinexplore's DuckDB works.

### Comparisons move to Flippatch

The comparisons should be run from the [Flippatch project](~/dev/flippatch/), which is responsible for creating the [data patches](~/dev/flippatch/docs/Patches.md) that add data to the Flipcommons database. AI sessions in Flippatch will create ad hoc queries and data patch campaigns that do the comparisons.

So we will move and rewrite the Pindata-aware views from Pinexplore to Flippatch. I suggest `~/dev/flippatch/scripts/analysis/external_data_sources/`. Never call this concept 'sources' as a standalone word but always qualify it as 'external data sources' because Flipcommons has other things also called source, such as a citation source. I assume we will have multiple `.sql` files in here? This would not be a new DuckDB but SQL bridge files.

Flippatch campaigns already join with Pinexplore's DuckDB, I suspect this new SQL will do it similarly, something like `ATTACH IF NOT EXISTS '../pinexplore/explore.duckdb' AS px (READ_ONLY);` such as in campaigns (0268, 0269, 0277, 0278) like `~/dev/flippatch/campaigns/0268-ipdb-project-dates/project_dates.sql`. Feel free to propose something better.

- **Worklists not errors**. The comparisons in Flippatch (such as "Flippatch is missing a model that IPDB has") should produce worklists / warnings, NOT build-blocking errors - unlike some of today's comparisons against Pindata which block the build.
- **Drop bidirectional comparisons**. Some of the pindata comparisons are bidirectional, such as "find credits that disagree, whether they are missing from pindata or IPDB". Flipcommons is now a superset of IPDB and will have things IPDB does not. Do not port the "missing in external data source" comparisons.

#### Comparison output

The public output of the comparison layer should be a single view of all findings. It'd look a lot like the Flipcommons audit system. It's not the SAME system, though: we wouldn't have the bridge write its findings into the actual Flipcommons audit views. The audit system is designed to lint Flipcommons and every finding is about a single record, whereas the comparison layer, where the findings are about a pair. Making this pair representable in the audit system would weaken the system's self-checks.

I would imagine that a finding has a severity, error or warning. The bar for error: our data is currrently wrong.

I do NOT want this turning int a giant system. It's probably quite easy to over-engineer this.

#### Dismissing

Findings need to be dismissable. If a finding is in error, we need to be able to make it go away permanently. We should be able to attach a text note to the dismissal, recording why. And maybe the date dismissed?

This belongs in Flippatch, not Pinexplore.

I do NOT want this turning int a giant system. It's probably quite easy to over-engineer this.

#### Keep `evidence.sql` separate

The `~/dev/flippatch/scripts/analysis/` folder also contains `evidence.sql` which joins to the Pinexplore web cache; it's a separate concern, let's not combine this data with that.

That's what Pinexplore is about. Pinexplore should provide all of those disparate layers of IPDB / OBDB / Pinside / etc dumps , the parsing and normalization of those dumps, and ways of comparing those dumps to Flipcommons. That's just what the pinexplore layer does now, but it does it over pindata rather than flipcommons.

The immediate urgent thing is that we just got a new xantari IPDB dump. It fails checks against pindata because it has new data, but I don't know if it would fail checks against Flipcommons because Flipcommons has way more and newer data than pindata.

#### Provide an external data sources context view

The Flippatch external data sources SQL should provide something like an `external_data_sources_context` view the way `evidence.sql` carries `evidence_context` — the Xantari snapshot date, the OPDB export date — so a campaign can tell "same query, newer dump" from a broken reproduction.

#### Why comparisons don't belong in Flipcommons

Flipcommons analytics is about exploring Flipcommons, NOT about looking at other data sources.

#### Why comparisons don't belong in Pinexplore

If Pinexplore imports the Flipcommons DuckDB, I suspect that would cause cycles when Flippatch imports the Pinexplore DuckDB and ALSO adds Flipcommons DuckDB stuff. Or even if it doesn't cause cycles now, it might cause cycles in the future.

Also, having comparisons in Flippatch is just a better separation of concerns that is easier to reason about.

### What stays in Pinexplore

Pinexplore's DuckDB becomes simpler. It's job is to get the external data sources ready to be compared against in Flippatch.

That means things like:

- **Combine dumps**. Like combine all the disparate IPDB dumps (multiple versions of Xantari, the IPDB JSONL from web cache) into a single cohesive view of the external data for IPDB.
- **Validating the externsl sources**. Like ensuring the IPDB ID from each ingested dump conforms to the IPDB ID format.
- **Parsing / regex information out of the sources**. Like breaking apart IPDB theme field into multiple themes.
- **Dropping 'known bad' records**. Don't do that in Flippatch.

Pinexplore's DuckDB should NOT:

- **Compare between external data sources**. I don't think this ever needs to happen, compare between say IPDB and OPDB. But if it does, that'd be a job for Flippatch.

I had to revert a previous AI session's attempt at refactoring this analytics because it put too much of the prep work into Flippatch. So before we get started, let's get very clear with specific tables/views/responsibilities on where they'll go in the new world.

### What goes to Flippatch

An incomplete list:

- **Joining to Flipcommons records**. Most of the Pinexplore sources go parse → normalize → resolve-to-catalog-slug. That last step, resolve-to-catalog-slug, needs to change. Almost every record already exists in Flippatch, and contains the IDs of the external systems: IPDB ID, OPDB ID etc. Most of the joining in Flippatch will connect up those IDs. The external data source records that do NOT connect up, where there is NOT an ID for it in Flipcommons, those are now errors/exceptions that need to be addressed.

Themes, gameplay features, and corporate entities currently get parsed out of their source string fields in ways that are aware of pindata data. This is messy, complicated, error-prone and actually not all that valuable anymore. Initial thoughts:

- **Themes**: I'm not super interested in IPDB's theme data now that we've built out a richer theme system. We should just NOT bring it over. Even for new records and changes, it's not so interesting. We've reassigned themes on a bunch of machines and there would be a lot of exceptions to deal with as a one-off. Keep the parsing that can stay in Pinexplore, if any.
- **Corporate entities**: we've extracted all the corporate entity info from IPDB; any new ones in IPDB will be newly added, and those will be infrequent and best handled manually. If there's a new corporate entity from IPDB, somehow stick it in an exceptions bucket in Flippatch for us to deal with as a one-off.
- **Gameplay features**: we've mostly extracted all the gameplay features. Maybe don't bring it over, at least for now? Is there some way to put unrecognized features in an exception bucket in Flippatch.
- **Reward types**: IPDB doesn't have a notion of reward type. Instead, we take reward type information from multiple places. We should continue to do so. This is a small, closed set of items and can be handled entirely in Pinexplore without reference to Flipcommons. It's similar to [IPDB Specialties](#ipdb-specialties).

### What might drop

Flipcommons has an alias system. I _think_ it completely replaces Pinexplore's handcrafted alias tables, but I'm not sure. Let's tread cautiously here.

### Integrate Archive IPDB

We are now acquiring IPDB data from a new source: archive.org. See `~/dev/flippatch/docs/plans/MissingIpdbData.md`. I want to integrate the archive.org JSONL with the Xantari records. As of Aug 21 2026 it was just a few hundred records, but we plan to grow it.

One issue is that the Xantari records are authoritative: as of Aug 20 2026, they are from 2026, whereas the Archive.org pages are mostly from 2018. So where the Xantari records have info, it MUST win. The value of Archive.org is to fill in missing information. For example, Xantari does not provide fields like `Specialty`, so we'd take that from Archive.org.

The current way that Pinexplore's DuckDB combines Xantari dumps is per-record: if the most recent dump has a record, use it. The old dump only fills in missing whole records. This is fine, but the Archive.org dump has to work differently. It should only fill in fields that are missing from Xantari.

If a future Xantari dump adds those fields, the Xantari version must automatically take precedence.

Rules:

- If Archive.org has a model that Xantari does not, drop it. Xantari is authoritative on the existence of records.
- Archive.org should provide 'Concept by' as any other credit role that Xantari explicitly does not have. If Xantari adds the role in the future, we should no longer accept that role from Archive.org.
- Bring in Archive.org theme data as a separate thing. Staging not mart. It's already an array, we're going to do some ad-hoc comparison in Pinexplore to see if there's stuff worth taking.
- Bring in Archive.org document data as a separate thing. Staging not mart. We're going to do some ad-hoc comparison in Pinexplore to see if there's stuff worth taking.

### Other tasks

#### Rename IPDB machine -> model

IPDB uses the word 'machine' for the concept we call 'model'. Right now the IPDB views use the word machine, and AIs leak the 'machine' term out from that. I want to stop that leakage. Rename IPDB machines to models at an earlier point, in staging. Like maybe rename `ipdb_machines_staged` -> `ipdb_models_staged`. Keep 'machines' for OPDB because it is representing a concept we don't have: a row could be either a title OR a model.

### IPDB Specialties

IPDB has a field called Specialties that the Xantari dump doesn't have but Archive.org's web pages do. It's quite important because it contains basic machine classification info like bingo that we've had to mostly synthesize from other sources up to this point.

The pinexplore db should prep IPDB specialty data and put it into the flipcommons language. IPDB has a dropdown that lists its specialties at <https://www.ipdb.org/search.pl?specialty=12&sortby=date&searchtype=advanced>. The list:

Add-A-Ball
Bagatelle
Bat Game
Bingo Machine
Cocktail Table
Conversion Kit
Converted Game
Cue Game
Flipperless
Gun Game
Head-to-Head Play
Horserace Game
Mechanical Backbox Animation
Non-Commercial Machine [Home Model]
Not A Pinball
Novelty Play
One Ball Game
Payout Machine
Re-themed Game
Redemption Game
Rolldown Game
Shaker Ball Machine
Table Top/Counter Game
Vertical Pinball Machine
Widebody
WWII Contract
Zipper Flippers

Note that IPDB models have more than one specialty: bingo, shaker ball, payout.

These specialities are not one cohesive thing. Different specialities are represented as different fields in Flipcommons, like game format, gameplay feature, reward type. One thing that is the reponsibility of Pinexplore's DuckDB is to take information like Specialties and map it to Flipcommons concepts. For example:

- `Redemption Game`-> `reward-type`: `ticket-payout`
- `Head-to-Head Play` -> `gameplay-feature`: `head-to-head`
- `Bagatelle` -> `game-format`: `bagatelle`
- `Re-themed Game` -> retheme edge
- `Widebody` -> `tag`: `widebody`
- `Flipperless` -> `tag`: `Flipperless`, which doesn't exist. It's a signal to consider adding the tag.
- `WWII Contract` -> `tag`: `WWII Contract`, which doesn't exist. It's a signal to consider adding the tag.
- `Table Top/Counter Game` -> `cabinet`: `Table Top/Counter Game`, which doesn't exist. It's a signal to look closer at the models and decide which it is.
- `Vertical Pinball Machine`-> `cabinet`: `Vertical Pinball Machine`, which doesn't exist. It's a signal to consider adding the cabinet type.
- `Not A Pinball` -> `game-format`: `Not A Pinball`, which doesn't exist. It's a signal to look closer at the model and figure out its game format, perhaps create a new game format.
- `Horserace Game` -> `game-format`: `Horserace Game`, which doesn't exist. It's a signal to consider adding the game format.
- `Cue Game` -> `game-format`: `Cue Game`, which doesn't exist. It's a signal to consider adding the game format.
- `Shaker Ball Machine` -> `game-format`: `Shaker Ball Machine`, which doesn't exist. It's a signal to consider adding the game format.
- `Payout Machine` -> `reward-type`: `Payout Machine`, which doesn't exist. I suspect we have more types of payout than IPDB does.

We can do this in Pinexplore without referencing Flipcommons. I think these are hard-coded rules. We'd use flipcommons slugs for things, so if we re-slugged `bagatelle` things would break. That's okay, we fix when it breaks. And we never re-slug these vocabulary things.

Every bit of this Specialty information is useful. If we DON'T have a rule dealing with a particular Specialty value, that's an error or warning or something.

### Other closed vocabularies

Beyond [IPDB Specialties](#ipdb-specialties), here are other small, closed vocabularies to hard-code in Pinexplore:

- **MPU→system_slug** — MPUs have all been extracted and new ones could be handled as a one-off, but `12_documents` uses `machine_system_slug` for `systems_referencing`, so I guess we still need it. Also, could we call this IPDB_MPU -> system slug. Not including IPDB leave scope for confusion.

Do not include:

- `ref_location_*_aliases`: we don't need Locations from IPDB anymore. Whenever we add a new Corporate Entity, we will handle its location manually. That's the only place Locations are used.
- `ref.normalize_mfr_name`: this is currently used to compare pindata to OPDB, fandom etc. If we need it, move it to Flippatch, because its only job is to compare between Flipcommons and external sources, and that normalization for comparison should live where the comparison happens.

### Fandom

Fandom isn't used. Keep the staging views, but do not compare against Flipcommons. We never ingested Fandom, it's a duplicate of info we already have, and comparing would only surface that we have not acquired Fandom IDs for things, which we haven't and won't.

### It's okay to break all campaigns

Break Flippatch campaign SQL all you want. Each Flippatch campaign is a point-in-time: once completed it is shipped to prod and the SQL will never be run again. If we have any campaigns active while doing this work and the SQL breaks, we will fix it in that campaign, not here.

### Standardized entity names

Flipcommons has a SINGLE spelling of each entity type. Use it. **Regularize the spelling of entities**. All tables/views/field names that spell an entity should use the Flipcommons entity names. Get rid of abbreviations like CE: spell out the Flipcommons Corporate Entity entity names. This closes down one vector of confusion, makes the names more self-documenting, makes it easier for AIs to guess the names without looking them up.

The names are in Flipcommons `entity_registry` / `entity_names` / `entity_subjects`: it's the entity type system, and the name pool across all entity types. It is reachable from both SQL and Python.

This applies both to existing views/tables as well as new views/tables, both in Pinexplore and Flippatch.

### Use DuckDB namespaces

The Pinexplore DuckDB doesn't currently use namespaces. I want it to. Here's a proposal. The following proposed list is not exhaustive, it does not list every table.

- **IPDB**
  - **`ipdb_ref`**: curated by hand, no upstream file. IPDB-specific lookups, IPDB-specific aliases
    - `ipdb_ref.specialty`: IPDB specialty -> Flipcommons vocabulary
    - `ipdb_ref.technology_generation`
    - `ipdb_ref.document_*`: all the IPDB document classification vocabulary
    - `ipdb_ref.retracted`
    - `ipdb_ref.duplicate_listings`
    - ...
  - **`ipdb_raw`**: raw layer, reads of source files, used only by the staging layer and `ipdb.ingest_watermarks`. `raw` means the `FROM` names a file. If it names another table, it isn't `raw`.
    - `ipdb_raw.xantari_model_snapshots` -- all snapshots in a single table
    - `ipdb_raw.archive_models` -- the JSONL from the web cache
  - **`ipdb_stg`**: staging layer, only used internally by higher layers
    - `ipdb_stg.models`: combines Xantari snapshots and archive.org into one row per IPDB model
    - `ipdb_stg.credits`: unnest of `DesignBy`/`ArtBy`/... + sentinel filter
    - `ipdb_stg.model_additional_details`: parse of the "IPD No. N / date / players" string
  - **`ipdb`**: the public, published mart - IPDB in our vocabulary
    - `ipdb.ingest_watermarks`: one watermark row per ingested artifact. One row per xantari dump, one row for the web_cache JSONL read.
    - `ipdb.models`: merged, validated, published models. Includes fields that allow callers to judge staleness.
    - `ipdb.models_provenance`: same grain, the `_src` columns projected out
    - `ipdb.model_specialties`: per-model specialty assignments
    - `ipdb.credits`, `ipdb.files`, `ipdb.patents`, `ipdb.trade_articles`
    - `ipdb.documents`: the classified IPDB file trove (`12_documents.sql`)
- **Other external sources**: if a layer is empty, don't create it
  - `opdb_ref.*` / `opdb_raw.*` / `opdb_stg.*` / `opdb.*`
  - `fandom_raw.*` / `fandom_stg.*` /`fandom.*`
- **Glossaries**
  - `glossary.ipdb`
  - `glossary.kineticist`
  - `glossary.pinball_primer`
  - `compared`: rename of `compare_glossaries`. Yes, this compares across external data sources, an exception to the 'pinexplore doesn't compare across external data sources' rule.
- **Web cache**
  - `web_cache.pages`
  - `web_cache.fetches`
  - `web_cache.library`: the cache's index of works
- **Ref**: generic hand-curated stuff used across multiple external data sources. I do NOT expect much in here if anything. If nothing ends up in here, don't create it.
  - `ref.*`
- **Ingest**:
  - `ingest.watermarks`: combine all the external data source watermark views into one global view, one row per artifact
- **System**
  - `checks.violations`: build fails on any row
  - `checks.warnings`: build prints them and continues
  - `main`: deliberately empty. This is an invaraint, let's check it. Ensure it catches macros too.

**Prevent Flippatch from accessing private layers**. The Flippatch comparison layer should have a check that prevents reading from `*_ref`, `*_raw`, `*_stg`.

#### Flippatch namespaces

Here's a Flippatch comparison layer namespace proposal. I don't like it yet because I think there's more architectural layers here, but maybe not?

- `ext`: the bridge attach, ingest/dump watermark info
- `ext_ipdb`: IPDB stuff
  - `ext_ipdb.credits_checks`: credits-related checks. The runner looks for `_checks` views.
- `ext_opdb`: OPDB stuff

All layers public. Prefix private stuff with underscore like `_name`, like everything else in Flipcommons.

This requires teaching the Flipcommons runner, `analyze`, about the `ext*` namespaces so that it discovers checks and describes the views/tables/macros.

## Read these before designing architecture or writing SQL

The existing Pinexplore SQL already solves most of this against pindata. Read it before designing anything. The shapes are correct. It has been extensively tested, hardened and used over many months and will be more correct than anything you write yourself. Only the comparison target changes.

- `sql/07_compare.sql` — how each source joins the catalog, and at which grain: `compare_models_ipdb`, `compare_models_opdb` (models, by opdb_id),
  `compare_titles_opdb` (titles, by opdb_group_id).
- `sql/05_error_checks.sql` `opdb_machine_missing_model` — OPDB gap analysis is scoped `is_machine = true AND physical_machine = 1`. Without both filters the alias rows and non-physical records read as gaps.
- `sql/02_raw.sql` `opdb_machines` — opdb_id is already split into group_id / machine_id / alias_id. Do not re-split it.
- `sql/04_staging.sql` `ref_feature_*` — the existing external-string → catalog-vocabulary maps. Note their targets are pindata's, and three now point at tags Flipcommons deleted.
- Flipcommons `entity_registry` / `entity_names` / `entity_subjects` — the entity type system, and the name pool across all 21 types.

Also you CANNOT do this deep data work, which is all about the nuanced semantics how how different systems represent the same data, without understanding the [domain model](~/dev/flipcommons/docs/DomainModel.md).

## Sequencing

- ✅ DONE: **Comparison layer in Flippatch**. A first draft exists but it doesn't much look like the Flipcommons audit system.
- ✅ DONE: **Delete pindata**. Delete the pindata-centric SQL from Pinexplore only after all the useful comparison stuff has been extracted. This is where the Pinexplore DuckDB build goes green again.
- ✅ DONE: **Namespacing Pinexplore**
- ✅ DONE: **IPDB JSONL + Xantari combining**
- ✅ DONE: **IPDB Specialties**
- **Move the OPDB manufacturer exceptions to Flippatch**. `opdb_ref.manufacturer_exceptions` was deleted from Pinexplore: nothing read it, and a comparison exception belongs beside the comparison. The 13 researched rows are recoverable with `git log -S opdb_ref.manufacturer_exceptions -p`. Whatever inherits them needs the check its own comment asked for — that each `manufacturer_slug` still resolves in the catalog.
- **Widen `opdb_mart_view_undocumented` past `opdb`**. Every `opdb` mart view now carries a one-line SQL `COMMENT` and a check in `80_structure_error_checks.sql` keeps that coverage complete. The check is scoped to `schema_name = 'opdb'` deliberately: the `ipdb`, `ingest`, `glossary` and `web_cache` marts are uncommented, and writing their descriptions without reading those views would put confidently wrong text in the database. Comment them, then drop the schema filter. Natural companion to the IPDB dialect work.
- **Bring IPDB into line with OPDB**. OPDB went first deliberately. `opdb_ref.feature` / `opdb_ref.keyword` now emit `target_value` alone — slug-shaped, never a claim about what the catalog holds — and the `opdb.model_*` views publish the value with no `*_exists` flag beside it. `ipdb_ref.specialty` still carries `target_public_id` and `target_is_public_id`, and `ipdb.specialties` / `ipdb.model_specialties` still publish the flag. Once the OPDB shape has been lived with, apply the same three changes to IPDB: drop the flag, rename to `target_value`, and split translated-vs-bucketed by the rule in `OpdbMappings.md` — IPDB Specialties are a small closed vocabulary, so they translate.
- **Namespacing Flippatch**.

NEVER `make pull` or `make push` through any of this. Neither command is allowed for AIs. There's no data on R2 that you need. All of this is happening on Moses' computer and the data is already there.
