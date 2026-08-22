# OPDB mappings

How to translate from OPDB's schema to Flipcommons' schema.

## Read domain model first

First you must understand the Variant and Title concepts, explained in `~/dev/flipcommons/docs/DomainModel.md`

## Titles

OPDB Groups map to Flipcommons Titles. I believe Flipcommons Titles have an `opdb_id` field that's actually the OPDB group ID. Probably should rename it in Flipcommons at some point (not now).

## Aliases

An alias / variant is _gameplay identical_ to the parent model. Variants have only cosmetic differences that don't affect gameplay. Therefore, IPDB doesn't include some of the information on variants / aliases, because it's assumed to be the same. However, in Flipcommons, an alias _is_ a true model, with all of the information. The only distinguishing thing is the alias_of pointer to the parent model.

An OPDB "non-physical" row is a virtual container for a collection of gameplay-identical machines. Gameplay equivalence is super important for OPDB because their raison d'etre is supplying info to tournaments, where gameplay-equivalence is one of the most important characteristics.

However, Flipcommons treats these records differently from OPDB: we do NOT create a virtual container, but instead choose the most broad model to be the 'parent' of all the variants. For example, we'd make a limited edition model be the variant_of a non-limited-edition model. Example: Godzilla LE and Godzilla 70th Anniversary would both be variant_of Godzilla Premium. OPDB puts those three under a virtual "non-physical" row.

In the case of these non-physical machines, I'm not 100% sure we can create a deterministic rule for which model to elevate as the primary. I forgot if we had a rule or not in the old OPDB integration that we just got rid of. This needs exploration.

For most of the virtual cases, Flipcommons has already promoted a model as the primary. I would expect to only see cases where the variant_of relationships have not already been established in models that Flipcommons doesn't have or has only acquired since the last OPDB dump.

## Proposed schema

The following proposal will NOT list every view and field exhaustively; it is meant to convey the patterns, not the full thing.

### Staging (`opdb_stg` namespace)

Some of the views:

- `features`: distinct OPDB features - this is NOT Flipcommons gameplay features, but OPDB's concept of features
- `keywords`: distinct OPDB keywords - this is NOT Flipcommons tags, but OPDB's concept of keywords
- `model_features`: one row per model per OPDB feature with where it should go in Flipcommons, similar to [`ipdb.model_specialties`](#ipdbmodel_specialties)
- `model_keywords`: one row per model per OPDB keyword (aka theme). All unmapped values go into `opdb.model_themes`.

### Mart (`opdb` namespace)

Rule of thumb: mart names are named after Flipcommons entities. If a view or column doesn't represent something in Flipcommons, you're usually on the wrong track.

Some of the views:

- `titles`: OPDB's groups as Flipcommons Titles
- `models`: OPDB's machines as Flipcommons Models, with variant lineage resolved
  - `cabinet`: for example, set by `opdb_stg.model_features='Cocktail table' -> cabinet='cocktail'`. Flipcommons has cabinet types like floor and tabletop that OPDB doesn't represent anywhere. If OPDB doesn't set a value, it's null, meaning undefined / does not say.
  - `export_edition_of`: from the OPDB feature `Export edition`. If the machine is an `alias_of` then we can also fill in the `export_edition_of` pointer to the same machine. If not, we create a `model_export_markets` row with no target.
  - `technology_generation`: sourced from OPDB's `type` field with its OPDB values (`em`/`ss`/`me`) translated to Flipcommons slugs: `solid-state` etc.
  - `variant_parent_id`: if the model is a variant, this is the ID of the parent. The parent might be virtural (in which case there's no model) or it might be a real model.
  - `variant_of`: only filled when we're sure of the parent (i.e., when it's not virtual)
  - `is_remake`: because OPDB doesn't point to what it's a remake of
  - `production_year`: always include
  - `production_month`: exclude all Januaries where the day is Jan 1 because we can't determine whether it's padding or not
- `manufacturers`: OPDB's manufacturers. Note that OPDB does NOT represent Corporate Entities, just Manufacturers (brand-level mfrs, trade names like Bally). Flipcommons manufacturers DO carry `opdb_manufacturer_id` so there's a way for Flippatch to join, but since it's not at the CE level it's more of a sanity check than anything else.
- `model_images`: OPDB's image URLs, one row per image URL per model
- `model_tags`: one row per model per Flipcommons tag, filled in from potentially multiple views acting like [`ipdb.model_specialties`](#ipdbmodel_specialties). This is where we'd map `Pro edition`, `Premium edition`, `Vault edition` -- signals to consider creating those in Flipcommons (this is exactly how `ipdb_ref.specialty` works)
- `model_relationships`: one row per model per Flipcommons `ModelRelationship`, filled in from potentially multiple views acting like [`ipdb.model_specialties`](#ipdbmodel_specialties)
- `model_gameplay_features`: one row per model per Flipcommons Gameplay Feature, filled in from potentially multiple views acting like [`ipdb.model_specialties`](#ipdbmodel_specialties)
- `model_themes`: one row per model per Flipcommons theme, sourced from `opdb_stg.model_keywords`
- Instead of the verbatim OPDB changelog, a view with something like:
  - `opdb_id`: any id OPDB has ever issued
  - `current_opdb_id`: what it is now, NULL if deleted
  - `status`: current / moved / deleted

Scalar FK in Flipcommons → column on `models` / `titles` / manufacturers. M2M → its own `model_*` / `title_*` etc view.

All OPDB values must be mapped, except for ones specifically mentioned above as unmapped. Not mapping a value must block the build. Yes, this means that pulling a new dump can block the build.

### `ipdb.model_specialties`

The IPDB mart translates IPDB Specialties and other info into Flipcommons vocabulary (see `ipdb_ref.specialty`), and the translation maps to multiple Flipcommons entities. For example:

- `Redemption Game`→ `reward-type`: `ticket-payout`
- `Head-to-Head Play` → `gameplay-feature`: `head-to-head`
- `Bagatelle` → `game-format`: `bagatelle`
- `Re-themed Game` → retheme edge
- `Widebody` → `tag`: `widebody`

We're going to need to do the same thing for OPDB. Two separate OPDB fields can create values in the same Flipcommons field. It would be easier for Flippatch to reason about if we converted completely to Flipcommons vocabulary, rather than expose something similar to `ipdb.model_specialties` in the mart, that sort of view would remain in staging.

If this works out well, we might go back to IPDB and make it speak Flipcommons more completely.

### When to translate values

Translate the values of small vocabs but not large ones:

**Don't translate vocabularies with aliases**. Flipcommons has an alias system for large vocabularies (themes, gameplay features…). It's not Pinexplore's responsibility to know what Flipcommons calls each item. Pinexplore just needs to move theme- and gameplay-feature-like items to the appropriate themes / gameplay-features bucket, and let Flippatch deal. Flippatch will deal by mapping via aliases, or marking specific OPDB items as permanently excluded, or not dealing with the bucket at all (like it might choose to do with themes).

**Do translate small vocabularies**. For small vocabularies (reward-type, cabinet, tag, series…), Pinexplore should translate to Flipcommons vocab (`em` → `electromechanical`).

**Some values cannot be directly translated to a Flipcommons concept**. We'll deal with those individually. Examples:

- For `keyword=licensed`: it's tag-like, so send it to tags. It's then in the right place for Flippatch to deal with. Flippatch won't create a tag, it'll use it as part of licensed relationships.
- For `keyword=eight` and `keyword=ball` those look like data entry snafus on the OPDB side, so make them die in Pinexplore so that Flippatch never sees them.

**Slugify all translated values**. Even the ones that don't match a slug in Flipcommmons. It's up to Flippatch to decide what to do with those values. For example, `feature=Payout Machine` → `reward_type=payout-machine`, which doesn't exist. If we in Flippatch decide `payout-machine` == `cash-payout`, we will update Pinexplore with that mapping.
