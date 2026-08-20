# Manufacturer docs in web cache

A manufacturer's first party documents are the absolute best, most important source materials for citations. We always cite them in preference of other sources.

IPDB has a huge trove of documents: thousands of manufacturer manuals, handbooks, parts lists, field bulletins. In includes not just manufacturer docs but also 3rd party game strategy guides, patents, and trade press articles.

You can explore the IPDB doc trove in Pinexplore's DuckDB analytics database. Read `sql/01_reference.sql` (class vocabulary, parent edges, detection patterns) and `sql/12_documents.sql` (`ipdb_documents`, `ipdb_patents`, `ipdb_trade_articles`) as a working prototype of the document schema — four independent axes (class, container, publisher, subject), a shallow is-a hierarchy where one document legitimately holds several classes, and patents and trade articles as separate source kinds — but treat its classification as coverage-measured and precision-unmeasured, and read `~/.claude/plans/pinexplore-document-classification-remaining-work.md` for what's known broken.

We want to expand Pinexplore's web cache to include this trove. Flippatch AI sessions researching data patches should be able to do one single full text search and get both the existing cached docs and this trove of docs, even before they are cached -- the search would search the names/titles/metadata of documents we haven't acquired yet. Just being able to search the metadata would be a a vast improvement over the current system, where a huge chunk of the AI session is spent simply hunting for what documentation might exist. Knowing a doc exists is half the battle.

As part of doing this, web cache should contain more structure and metadata around the docs than IPDB does; see [document classification](#document-classification) below.

As we acquire new docs NOT identified by IPDB, we'd apply the same structure and metadata. For example, IPDB provides zero Gottlieb docs. I would imagine that the existing web cache `fetch` would somehow add the extra metadata/classification info.

## Document library

We want Flipcommons to publicly host all these docs at some point. See [DocumentLibrary.md](~/dev/flipcommons/docs/plans/DocumentLibrary.md).

Pinexplore's web cache would be a prototype of how to [structure](#document-classification) that library. The documents acquired in web cache would be placed directly into Flipcommons; we wouldn't go re-acquire those docs from the internet.

## Relating web cache docs to Flipcommons records

We want the web cache documents to be associated with the related records in Flipcommons, such as:

- Relating each doc to correct manufacturer / model / system records
- Relating each doc to the correct citation source. We already seeded the manufacturer citation source roots.

We do NOT want to relate the docs to the Pindata data in the Pinexplore DuckDB; that is an obsolete static snapshot. We want to relate it to the Flipcommons dev database, which lives at `~/dev/flipcommons/backend/db.sqlite3`. It would be good to keep enough info about the IPDB source from whence it came that we CAN relate it back to the original IPDB document dump in Pinexplore DuckDB. It has its own analytics DB; see `~/dev/flipcommons/scripts/analysis/README.md`.

One challenge is that the cache syncs across machines via R2, but Flipcommons' `db.sqlite3` is a local dev DB. Storing its primary keys would be brittle. That pushes us towards storing slugs instead of the database's native PKs. The challenge is that slugs can and do change. And there's only one single developer using this right now. It's more important that the relationship survives a slug change than works across multiple developers. Can we have a system that accomplishes both? When moving to a new system, rebuilds the FKs? We don't even have to build that part immediately; we can simply store PK now and do the slug + PK thing later. I don't want to store the slug now because slugs DO change. We add slugs when we add developer #2.

### Attaching to system: deferred

This is trickier than it looks. Not for v1. All the information will be carried into web cache, we can make a better decision later.

## Relating web cache to Flippatch citations

Flippatch has an add-on analytics layer at `~/dev/flippatch/scripts/analysis/evidence.sql` that will probably want to relate itself to these docs in web cache.

## Citing these docs

Most of the content is cited as the new `document` citation source type created here: [DocumentCitations.md](~/dev/flipcommons/docs/plans/citations/DocumentCitations.md). Trade press articles are the exception; they're `periodical`.

## Prefer archive copies

We'd rather acquire and cite those docs from an archive site like archive.org and not just IPDB when possible, because we over-rely on IPDB and need to break the habit.

Once we have a non-web way of citing these docs, I guess we'd attach multiple URLs to the doc: archive.org, IPDB, Pinside, however many places a doc exists.

## IPDB deliberately blocks automated fetches

IPDB sits behind Cloudflare and refuses automated clients, including on `/files/`. Measured 2026-08-12:

| route                                                       | result                                                             |
| ----------------------------------------------------------- | ------------------------------------------------------------------ |
| `curl` + Chrome UA → `machine.cgi?id=6156`                  | 403                                                                |
| `curl` + Chrome UA → `/files/6156/TWDLEflyer.pdf`           | 403, `cf-mitigated: challenge`, `server: cloudflare`               |
| Playwright headless Chromium, pinexplore UA → `machine.cgi` | 403 interstitial                                                   |
| Playwright headless Chromium, default UA → `machine.cgi`    | 403 interstitial                                                   |
| Playwright headful Chromium → `machine.cgi`                 | 403, then an interactive "Verify you are human" Turnstile checkbox |
| Human-driven browser session → `machine.cgi` + in-page PDF  | 200, full page; PDF 17,546,301 bytes, `%PDF-1.3`                   |

What the matrix establishes:

- **Header spoofing does not work.** The UA-spoofed `curl` still 403s; Cloudflare fingerprints the JS and TLS handshake, and the `accept-ch` / `critical-ch` client-hint demands in the 403 confirm it wants a browser to answer.
- **A browser engine is not sufficient either.** Playwright-driven Chromium fails headless _and_ headful, with the stock UA and with ours. The last row's success came from a browser a human was sitting in front of — not from the engine.
- **The end state is an interactive human-verification challenge.** That is the decisive finding, and it is a deliberate access-control decision by a volunteer-run site, not a misconfiguration to route around. `/files/` in particular is exactly the bulk-download surface this plan wanted to automate.

**So automated bulk acquisition from IPDB is out of scope for this plan.** Not "hard", not "deferred pending a cleverer fetcher" — we are not building a pipeline whose function is to defeat a human-verification checkbox, and scale is what makes it that rather than an incidental workaround. Two corollaries worth stating so they don't get re-litigated: lifting Cloudflare's `cf_clearance` cookie out of a browser and replaying it from `httpx` is the same act with extra plumbing, and so is any fingerprint-patching stealth driver.

This does not touch the metadata half of the plan, which is the larger win and is fully unblocked: the [trove seed](#seed-and-backfill-sequence) reads `explore.duckdb`, never the network. Knowing a document exists remains most of the battle, and a seeded-but-unacquired document is a first-class row here by design.

## Each doc should exist exactly once

Each distinct, unique doc needs to exist exactly once in our system. When citing a document, we should be able to choose from among all the URLs that link to that doc. In the document library, Pokemon LE and Premium link to the same copy of `Pokemon_LE_Pre_web.pdf`.

We don't have a fully deterministic way of telling what's a distinct, unique document. Even after we fetch a URL, we have examples of different 404 pages returning the same content, same `content_sha`. So it sounds like what we need is something like:

| table             | grain         | carries                                          |
| ----------------- | ------------- | ------------------------------------------------ |
| 🆕 document       | the work      | title, class, publisher, subject, date           |
| 🆕 url            | an address    | role (reference/catalog/archive), sheet order    |
| capture (`pages`) | bytes we hold | `content_sha`, `text`, `ocr_text`, `http_status` |

- The documents table does not have a unique key.
- The URLs table holds URL a document owns, where a document can be found, whether or not they have been fetched.
- The existing per-URL `pages` table lives as it does today. This is a record of successful fetches / captures, and each row has a `content_sha`, which will somehow be used as inputs to de-duplicating / merging rows in the document table. Not deterministic; there will be some AI reasoning to decide whether it's truly mergeable.

General guidance: let's not over-merge on the initial metadata ingest. Later on, it will be easier to merge than undo over-merging, where the document silently ceases to exist and is never fetched.

### All types of documents go into the documents table

All types of documents -- web pages, images etc -- should go into the documents table.

All these types of documents should also this additional metadata, at least the metadata about the subject: specific manufacturer, model or system. We already sorta collect this as free text: `fetches.search_query` holds the intent behind nearly every fetch ("chicago gaming medieval madness merlin 2025").

This allows us to filter queries by manufacturer, model or system across the entire corpus. "What do we have about the Gorgar machine?"

### Editions and language variants

We won't group editions or languages into some higher-level container. Each of these its its own distinct document:

- Manual (February 1979, no schematics)
- Manual (March 1979, no schematics)
- Manual Amendment (WMA486-1, undated, …)
- Manual Amendment (490-2, undated, …)
- Manual Amendment (WMA490-1, undated, …)
- Solid State Flipper Maintenance Manual Supplement (August 1980)

## Search partition

Searching 4,000 metadata-only rows against < 1,000 text rows will swamp any common term; searching "manual" returns thousands of hits with no words in them. Let's partition: search returns held documents (text/ocr matches) first, then a separate capped "not acquired" block showing title, class, subject and the URL(s) to go get it.

The AI session then goes hunting and caches what it finds. This is already vastly better than what AI sessions have to do now, where it doesn't even know the doc exists. The hunt runs against reachable sources — archive copies, first-party manufacturer sites, the wider web. When the only known address is behind [human verification](#ipdb-deliberately-blocks-automated-fetches), the session does not try to get past it: it records the block and surfaces the document to the user, who can decide whether it is worth fetching by hand. A displayed "not acquired, blocked at ipdb.org" row is a useful result, not a failure — the session still learned the document exists and where it lives.

Let's have a way of recording failed hunts: record a list of places it _isn't_, and places it _is_ but it couldn't reach (auth, 403 etc). Both are dated records. We already have this in the `fetches` table. The search result shows this information, perhaps something like "not at xxxx @ date".

### Fetching un-acquired docs in bulk

Bulk acquisition targets **reachable sources, and IPDB is not one of them** (see [above](#ipdb-deliberately-blocks-automated-fetches)). The seed gives every document a `catalog`-role IPDB URL, but that URL's job is provenance and hand-fetching, not automated retrieval. A bulk run is the existing `web_fetch.py` over a work list — no new engine, no browser driver — and the work list is a query: documents whose `document_urls` have no matching `pages` row, ordered by role, `archive` and `reference` before `catalog`.

Which makes the interesting problem **finding the reachable copy**, not fetching it. A seeded document usually arrives knowing only its IPDB address, so the run has to discover an alternative before it has anything to fetch:

- **archive.org first.** It has a real API, wants to be fetched, and holds a great deal of IPDB's own material. Its Wayback CDX endpoint answers "is there a snapshot of this URL" directly, which turns many `catalog`-only documents into `archive` URLs mechanically.
- **First-party manufacturer sites next.** Stern, Jersey Jack, Chicago Gaming and friends publish current-era manuals themselves, mostly unprotected. This plan's opening line already calls first-party documents the best citation source, so this is the preferred copy on the merits, not a fallback.
- **Everything found gets written back as a `document_urls` row** with its role, so discovery accrues to the corpus instead of being repeated per run.

Two properties to keep whatever the source:

- **Failures are recorded, not just retried.** A blocked or missing document lands in the existing `fetches` / `document_hunts` records the [search partition](#search-partition) displays, so a run leaves behind dated evidence of where a document isn't — and a human-verification block is recorded once and then respected, not retried on a schedule.
- **Politeness is the fetcher's job.** Rate limiting and concurrency caps stay central, applied to every caller. Archive.org and small manufacturer sites deserve the same courtesy the `_rate_limit` path already gives everyone.

**Before building any of this, ask IPDB.** A catalog project with a clear purpose is a plausible candidate for a bulk dump, a rate-limit exemption, or simple permission — and that is the only outcome that makes the trove durably available rather than something we work around. One email is cheaper than every alternative on this page, and its answer changes what is worth building.

## What gets indexed

Include:

- the ~4,000 non-image documents
- classified images, such as flyer scans, instruction-card scans
  - Make sure we get whatever information comes with them, like captions
  - Some of these scans are actually separate sheets of a single doc, like the front and back of a flyer. We want to re-assemble that information. Its a single doc with multiple parts, somehow. Feels like we need sections or sheets, much like web cache already has sections for HTML and PDF and sheets/pages for PDFs.
- audio
- video

For every indexed source, make sure we get every single piece of information out of the IPDB dump. Pass any exceptions by me.

Exclude for v1

- Unclassified images
- ROM sets

We want all of it, eventually, to be the basis of the public document library.

## Document classification

We can improve on IPDB's classification, structure and metadata -- and have already started, in Pinexplore's DuckDB `sql/12_documents.sql`.

IPDB assigns a doc to the following categories:

| category           |  files | models | sample name                                                              |
| ------------------ | -----: | -----: | ------------------------------------------------------------------------ |
| `image`            | 80,374 |  5,535 | "Image # 25474: A-B-C Bowler Ad"                                         |
| `documentation`    |  2,251 |  1,241 | "Schematic Diagram (continuous, for serial numbers below 1640)"          |
| `file`             |  1,669 |    878 | "Hi-Score Replay Adjustments - Chart"                                    |
| `rom`              |  1,173 |    487 | "U15 L-1 Sound ROM, 4MB Chip Version"                                    |
| `rule_sheet`       |    188 |    154 | "The Addams Family Rulesheet Version 2.0 (Jan/27/1995), by Brian Dominy" |
| `service_bulletin` |    133 |     76 | "Customer Service Bulletin B-A004 (undated, adding posts to playfield)"  |
| `multimedia`       |     40 |     23 | "More Game Play At Night Movie"                                          |

The taxonomy **mixes axes**:

- `image`, `rom`, `multimedia` and `file` are **formats**
- `service_bulletin` and `rule_sheet` are **document classes**. `documentation` alone lumps 2,251 files — operations manuals, operators handbooks, parts lists and platform schematics all in one bucket. And `file` is a junk drawer holding adjustment charts, promotional photos and a "Differences between TAF and TAFG" comparison sheet alike.

A finer classification already exists, as free text in the IPDB document names: "Operations Manual (English, May 1996, Final)", "Operators Handbook (May 1996)", "Parts List", "Schematic Diagram (continuous, for serial numbers below 1640)". IPDB knows each document's class, language, date, revision and even the serial-number range a schematic applies to — theu just don't give it to us as structured fields.

The `0215-frontier-2026` campaign has hit cases IPDB's shape _cannot_ express:

- **Document attached to multiple Models**: `Pokemon_LE_Pre_web.pdf` covers LE and Premium.
- **Document attached to a System**: the `WPC-95 Schematic Manual` under `TOTAN` is the same document as under every other model that uses `/systems/wpc-95`.
  - We want to attach that document to the System, not the Models. A Model shows its System's docs along with ones directly attached. However, this is not a blanket thing: while "Williams WPC-95 Schematic Manual" is platform-level, "Schematic Diagram (continuous, for serial numbers below 1640)" is specific to one machine's serial range.
  - This is not an edge case; schematics are half the trove.

## Web cache must not depend on the analytics layer

On a day-to-day basis, the Pinexplore analytics references web cache, not the other way round. The vocuabulary layer in Pinexplore analytics was a _prototype_ of the real thing, which will live with web cache.

This is NOT to say that the initial data load into web cache cannot rely on Pinexplore; it must! We did all this great work hardening that culminated in `sql/12_documents.sql`, we must use it. So we're going to have to get that, as a one time thing, into web cache.

### We'll get classification wrong

We will get classification wrong at the beginning, and need to improve it. Make sure the classification is presented as a guess, not a verdict.

Let's use all the classifications, even ones with only 5 examples.

Let's make it easy to see how many docs are in each class.

## Retain all IPDB data in web cache

All the original IPDB data about the record needs to be in web cache so we don't have to re-ingest in order to make changes, like adjusting classification or attaching to system records. This includes things like the IPDB category, IPDB URL, IPDB name.

## Document titles

Every search result leads with the document's title, and the value we retrieve from IPDB won't do the job:

```text
294 × "Schematic Diagram (continuous)"
186 × "ROMs"
147 × "English Manual"
109 × "Game Flyer"
```

The IPDB name cannot be the displayed title. At read time let's synthesize a display title for these IPDB records. Something like `subject + name + date`, per DocumentCitations' convention — "Tales of the Arabian Nights Operations Manual (May 1996)", platform docs leading with the publisher instead.

## Design

Decisions settled in discussion, and the schema they produce. Everything here lives in the web cache's `cache.sqlite`, beside the existing `pages` / `fetches` tables, and rides the existing R2 sync.

### Settled decisions

- **One document row per IPDB `file_url` at seed. No merging in the seed.** The WPC-95 schematic arrives as one document per copy, sharing a basename. A wrong merge silently destroys a document forever; duplicates are cheap and visible. The shared-basename evidence (`machines_referencing` and friends) is stored on the rows as a merge _hint_. Merging is a later, deliberate act — same for consolidating identical `content_sha` captures and for assembling multi-sheet image scans (a flyer's front and back become one document only when someone decides they do; the parts structure that assembly needs is designed with that work, not ahead of it).
- **Every `pages` row gets a document row — the document is the universal top grain.** Document → url(s) → capture. The trove rows are simply documents none of whose URLs have a capture yet. The existing corpus is backfilled with minimal document rows (class and subject empty; `fetches.search_query` free text is raw material for a later subject-attachment pass), and every future fetch auto-creates its document row.
- **IPDB ID is the durable cross-system handle; the Flipcommons PK is an enrichment.** Subject rows carry the IPDB-side facts verbatim where the doc came from IPDB, plus a nullable resolved Flipcommons PK. Resolution is a seed-era script that reads the Flipcommons dev DB and joins on IPDB ID; day-to-day search never touches Flipcommons. If Flipcommons PKs ever shift, resolution re-runs off the IPDB IDs. Not all subjects have an IPDB ID — Flipcommons holds models IPDB doesn't, and much of the existing cache is about them — so a subject row must stand alone with only a Flipcommons PK; the IPDB columns are optional provenance. Flippatch and Flipcommons analytics join _into_ web cache on IPDB ID or URL; nothing in pinexplore executes against Flipcommons at query time.
- **Attachment is at the most granular true level.** Model rows when the doc is about model(s) — multiple models is native, one subject row each. A corporate-entity row when it's maker-wide: IPDB's manufacturer grain _is_ Flipcommons' CorporateEntity (its per-era makers carry year ranges), and the Manufacturer rollup derives via CorporateEntity's own FK. System attachment stays deferred; the info to decide later is all carried.
- **Registration is one library function, and the page→document invariant lives in the shared write path.** Document-row creation happens inside `upsert_page` itself, in the same transaction as the page write, so every writer — fetch, import, whatever comes later — upholds the invariant without knowing about it; the `init_schema` backfill self-heals rows written by code predating the tables. `web_docs.py` is the full metadata CLI surface (register, classify, attach subjects, merge, correct); `web_fetch.py` grows thin flags that call the same functions on the row it just wrote. Fetch never grows its own metadata path. The attachment function reconciles the two subject identity paths (a PK-carrying insert first looks for an existing row that resolves to the same identity), so one logical subject can never exist as an IPDB-only row and a PK-only row side by side.
- **Kind-specific fields are nullable columns on `documents`, not side tables.** Patent jurisdiction/number and trade-article publication/issue must survive the seed because they're the identity keys for the later merge pass, but at this scale a side table buys nothing.
- **Classification is stored as a guess.** Every class assignment carries its source (`ipdb_pattern` for the seed, `manual`, `ai`), and NULL/absent means not-yet-judged, never "not a manual". Per-class counts are one query away.
- **Naming: "machine" dies as soon as it leaves IPDB-specific code.** IPDB says machine; the catalog says model. The word survives only inside IPDB-specific names — the listings table's verbatim dump fields, `ipdb_`-prefixed provenance columns, comments describing IPDB's own grain, and `machinemodel` where it is Flipcommons' literal table name. Everywhere else — scope values, prose, function names, CLI surfaces, search output — the word is model. (IPDB likewise has no concept of a title; title-vocabulary appears only on the catalog side of a name.)

### Schema

`connect()` enables `PRAGMA foreign_keys=ON` — SQLite silently ignores every `REFERENCES` clause without it.

```sql
CREATE TABLE documents (
  id            INTEGER PRIMARY KEY,
  title         TEXT,     -- the work's own title where known (IPDB name, registration);
                          -- display titles are synthesized at read time, not stored
  publisher     TEXT,
  -- there is no source-kind column, and no kind vocabulary either. The
  -- prototype's document | patent | trade_article kinds existed to give the
  -- analytics projections different identity keys, and those keys are the
  -- kind-specific columns below — a non-NULL patent_number already says
  -- everything a kind label would. Citation routing is class-based prose
  -- (see "Citing these docs"): trade_article-classed docs cite as
  -- `periodical`, everything published cites as `document`, a bare page as
  -- `web` — matching DocumentCitations, which examined the third kind and
  -- deliberately collapsed it.
  -- date of the work: deliberately absent for now. IPDB's date text stays inside
  -- the listing names; parsed year/month/day columns land with the edition/merge
  -- work that consumes them ("extracted and unused" is the failure mode to avoid twice)

  -- merge hints, measured at seed time across shared basenames (NULL = no hint).
  -- The first counts IPDB's own listings; the other two count the catalog
  -- titles/systems those listings resolve to — catalog concepts, so no ipdb_ prefix
  ipdb_machines_referencing      INTEGER,
  catalog_titles_referencing     INTEGER,
  catalog_systems_referencing    INTEGER,

  -- kind-specific identity (merge keys), NULL off-kind
  patent_jurisdiction TEXT,     -- US | GB | ES ...
  patent_number       TEXT,     -- D prefix is part of the number
  article_publication TEXT,
  article_issue_date  TEXT,
  article_pages       TEXT,

  -- the Flipcommons citation source this document cites as, held as its cite
  -- ref ("williams:tales-of-the-arabian-nights-operations-manual-1996") — a
  -- ref, not a PK, the inverse trade from document_subjects.flipcommons_pk:
  -- catalog slugs change (so subjects hold PKs), citation slugs are frozen by
  -- flipcommons doctrine (patches replay against them, so they never rename).
  -- NULL until the enrichment pass resolves it by URL join, once the
  -- flipcommons document seed exists.
  citation_ref  TEXT,

  created_at  TEXT NOT NULL,  -- ISO8601 UTC; distinguishes seed-era rows from later registrations
  updated_at  TEXT NOT NULL   -- when metadata last changed
);

-- The original IPDB dump listings, verbatim, at the dump's own grain: one row
-- per (machine, file, category) listing. A URL can be listed under several
-- machines and under several categories, so scalars on the work-grain table
-- would have to discard facts or choose arbitrarily. This table IS the
-- "retain all IPDB data" requirement: every raw dump field lands here (the
-- columns below plus anything else the dump carries), so no change of mind
-- ever needs a re-ingest.
CREATE TABLE document_ipdb_listings (
  document_id          INTEGER NOT NULL REFERENCES documents(id),
  ipdb_id              INTEGER NOT NULL,  -- the machine page the file was listed under
  file_url             TEXT NOT NULL,     -- the seed-grain identity
  ipdb_category        TEXT NOT NULL,     -- image | documentation | file | rom | rule_sheet | ...
  ipdb_name            TEXT,              -- display name; holds the date/language/revision text
  container            TEXT,
  machine_name         TEXT,
  machine_manufacturer TEXT,
  ipdb_manufacturer_id INTEGER,           -- the dump's ManufacturerId; joins Flipcommons' CorporateEntity.ipdb_manufacturer_id
  machine_mpu          TEXT,
  PRIMARY KEY (ipdb_id, file_url, ipdb_category)
);

-- Join table: a document holds several classes legitimately (a Schematic Manual
-- is both). Each row is a judgment, stamped with who made it and when.
CREATE TABLE document_classes (
  document_id     INTEGER NOT NULL REFERENCES documents(id),
  document_class  TEXT NOT NULL REFERENCES document_class_vocab(document_class),
  source          TEXT NOT NULL,      -- ipdb_pattern | manual | ai — always a guess with provenance
  created_at      TEXT NOT NULL,
  PRIMARY KEY (document_id, document_class)
);

-- One row per address the work lives at, fetched or not. `url` is the primary
-- key: a capture belongs to exactly one document, which is what keeps
-- "acquired" well-defined — the seed never mints shared URLs (duplicate IPDB
-- listings collapse into one document), and a merge moves URLs to the survivor.
-- Roles are DocumentCitations' link-type vocabulary, so the two systems speak
-- the same words: `reference` is the document's own canonical address (the
-- publisher's copy, or a web page's own URL), `catalog` a third-party index
-- holding a copy (IPDB's /files/ URL — the bookshop, not the publisher),
-- `archive` a preserved snapshot (archive.org).
CREATE TABLE document_urls (
  url          TEXT PRIMARY KEY NOT NULL,  -- normalized; joins pages.url when captured.
                                           -- NOT NULL is not implied: in a rowid table only
                                           -- INTEGER PRIMARY KEY implies it
  document_id  INTEGER NOT NULL REFERENCES documents(id),
  role         TEXT,                       -- reference | catalog | archive
  created_at   TEXT NOT NULL
);
-- No sheet_order column: a URL row can't say whether it is another mirror of
-- the whole work or one ordered part of it (two mirrors of sheet 1 breaks
-- either reading). The parts structure gets designed with the deferred sheet
-- assembly work that consumes it — same discipline as the date columns.

-- Dated negative results: a hunt tried somewhere and concluded the document
-- is NOT there. Deliberately separate from document_urls, which asserts the
-- work DOES live at its URL — and whose primary key would let a wrong guess
-- permanently own an address against the document that actually lives there.
-- The other failure mode — a known address that can't be reached (403, auth)
-- — is a document_urls row plus its failed fetches, not a hunt.
CREATE TABLE document_hunts (
  document_id  INTEGER NOT NULL REFERENCES documents(id),
  tried        TEXT NOT NULL,   -- the URL or site searched
  note         TEXT,            -- what was searched, why concluded absent
  created_at   TEXT NOT NULL
);

-- Join table: one row per subject — a document about several models carries
-- several rows. A row stands alone with only a flipcommons_pk (Flipcommons
-- holds models IPDB doesn't); the ipdb_* columns are optional provenance.
-- The partial unique indexes below are what make the re-runnable attachment
-- and enrichment scripts idempotent (enrichment UPDATEs rows in place; these
-- guard the insert paths).
CREATE TABLE document_subjects (
  document_id           INTEGER NOT NULL REFERENCES documents(id),
  -- corporate_entity, not "manufacturer": IPDB's ManufacturerId is
  -- corporate-entity-grained (Flipcommons stores it on CorporateEntity, whose
  -- year_start/year_end mirror IPDB's per-era makers), and the Manufacturer
  -- rollup is always one derivable FK hop away (CorporateEntity.manufacturer_id
  -- is fully populated). Most granular true level, same as models.
  scope                 TEXT NOT NULL CHECK (scope IN ('model', 'corporate_entity')),
  flipcommons_pk        INTEGER,        -- resolved enrichment: machinemodel PK on model scope,
                                        -- corporateentity PK on corporate_entity scope;
                                        -- re-derivable from the scope-matching ipdb id below
  label                 TEXT,           -- local searchable name snapshot ("Yukon Yeti"), written at
                                        -- attachment and refreshed by enrichment — a PK-only subject
                                        -- has no IPDB name, and search never opens Flipcommons, so
                                        -- without this a Flipcommons-only model is unfindable by name
  ipdb_machine_id       INTEGER CHECK (ipdb_machine_id IS NULL OR scope = 'model'),
  ipdb_manufacturer_id  INTEGER CHECK (ipdb_manufacturer_id IS NULL OR scope = 'corporate_entity'),
  ipdb_machine_name     TEXT,
  ipdb_manufacturer     TEXT,
  created_at            TEXT NOT NULL,
  -- a subject must be identified by something
  CHECK (flipcommons_pk IS NOT NULL OR ipdb_machine_id IS NOT NULL
         OR ipdb_manufacturer_id IS NOT NULL),
  -- a PK-only subject's label is its only searchable name, so it is mandatory
  -- and must carry a non-whitespace character ("   " tokenizes to nothing)
  CHECK (ipdb_machine_id IS NOT NULL OR ipdb_manufacturer_id IS NOT NULL
         OR (label IS NOT NULL AND trim(label) <> ''))
);
CREATE UNIQUE INDEX document_subjects_by_pk
  ON document_subjects(document_id, scope, flipcommons_pk) WHERE flipcommons_pk IS NOT NULL;
CREATE UNIQUE INDEX document_subjects_by_ipdb_machine
  ON document_subjects(document_id, scope, ipdb_machine_id) WHERE ipdb_machine_id IS NOT NULL;
CREATE UNIQUE INDEX document_subjects_by_ipdb_manufacturer
  ON document_subjects(document_id, scope, ipdb_manufacturer_id) WHERE ipdb_manufacturer_id IS NOT NULL;

-- The class vocabulary migrates here from sql/01_reference.sql as data: the
-- classes and their parent edges. The prototype's source_kind column does NOT
-- migrate (see the documents-table comment). The detection *patterns* stay
-- in pinexplore — they read one source's naming habits and run only at seed.
-- Parent edges are one row per edge, preserving the prototype's deliberate
-- choice: a class may later carry two parents without a schema change.
CREATE TABLE document_class_vocab (
  document_class  TEXT PRIMARY KEY
);
CREATE TABLE document_class_parents (
  document_class  TEXT NOT NULL REFERENCES document_class_vocab(document_class),
  parent_class    TEXT NOT NULL REFERENCES document_class_vocab(document_class),
  PRIMARY KEY (document_class, parent_class)
);
```

An un-acquired document is a `documents` row whose `document_urls` have no matching `pages` row. The two flavors of failure are held apart: "it's there but blocked" is a `document_urls` row whose `fetches` show a non-200 status and a date; "we looked and it isn't there" is a `document_hunts` row. Search's "not acquired" block reads both — "blocked at ipdb.org @ 2026-08-12", "not at archive.org @ 2026-08-12".

### Search

Metadata gets its own FTS table (title, IPDB names, subject labels and IPDB subject names, classes) covering **every** document, acquired or not — a separate bm25 space, for the same reason `ocr_fts` is separate: metadata-only rows would swamp or be swamped inside the text index. Acquisition state is a display partition, not an index boundary: an acquired scan whose subject appears only in metadata must still match a subject search. Search output partitions as planned: held documents first (the existing text/ocr tiers, hits decorated with class/subject, metadata-only matches on held documents included), then a capped "not acquired" block showing synthesized title, class, subject, the URL(s) to go get it, and any failed-hunt history.

Two mechanics pinned here so they're decisions rather than accidents:

- **Held results aggregate by document, not URL.** After a merge, one work can own several captured URLs; it occupies one result slot, listing each matching capture with its URL intact — citation and verification still address the capture.
- **The metadata FTS is maintained by the shared library, not triggers.** Its text derives from four tables (documents, listings, classes, subjects), and every mutation already flows through the registration functions, so they rebuild the affected document's FTS row; a full-rebuild command exists for repair. Multi-table triggers would be the fragile version of the same promise.

### Seed and backfill sequence

1. **Vocabulary** — copy classes and parent edges from `01_reference.sql` into `document_class_vocab` / `document_class_parents` (the prototype's source kinds deliberately don't come along).
2. **Trove seed** — one-time script reading `explore.duckdb` (`ipdb_documents`, `ipdb_patents`, `ipdb_trade_articles` plus the raw fields): one document per `file_url` (a URL's several IPDB listings collapse into one document, each listing kept verbatim in `document_ipdb_listings`), class rows marked `ipdb_pattern`, subject rows with IPDB facts, URL rows with role `catalog`. **The v1 subset is defined by exclusion from seeding, not by hiding**: non-image rows minus ROM sets, plus image rows carrying at least one class match (the "classified images"). The ROM predicate is both conditions — excluded when `ipdb_category = 'rom'` **or** the classes include `rom_set` — because the two disagree in both directions (ROM-category rows the pattern missed, and EPROM/bin files filed under `file`); the recorded cost is a couple of "ROM Revision History" rows, real documents _about_ ROMs, which wait for the subset to widen. **Subject rule: the seed asserts only what IPDB asserts** — one model-scope subject per distinct machine a listing appears under, and never a corporate-entity subject, since IPDB attaches files only to machine pages; corporate-entity subjects come from later judgments or non-IPDB registrations. A shared platform document is simply a document with several model subjects plus merge hints, pending the deferred system-attachment decision. Excluded rows are simply not seeded — the dump stays in pinexplore, so widening the subset later is a re-run, and tens of thousands of caption-less image rows never enter the metadata index the search partition exists to protect. This scopes the retention requirement: "retain all IPDB data" applies to every _seeded_ record (its listings land verbatim, so reclassification and re-attachment never need a re-ingest); rows outside the v1 subset are retained by the dump itself until the subset widens.
3. **Corpus backfill** — a minimal document row per existing `pages` row, URL role `reference`: at backfill time the document _is_ the page at that URL, so the URL is definitionally its canonical address. The role becomes a judgment only later, when someone declares a cached copy to be some other publisher's work — and re-judging the role is part of that same deliberate act. **Collision rule, both directions: one URL, one document — whichever script arrives second attaches to the existing document instead of minting.** The backfill actually lands first (it ships with the schema migration, so the every-page-has-a-document invariant holds from the start); the trove seed then finds any already-cached IPDB URLs owned by backfill documents and enriches those documents in place with listings, classes and subjects.
4. **Enrichment** — resolve `document_subjects.flipcommons_pk` by joining IPDB IDs against the Flipcommons dev DB; resolve `documents.citation_ref` by URL join once the flipcommons document-citation seed exists. Both re-runnable.
5. **Later, separately** — subject attachment for the backfilled corpus (from `search_query` and titles, resolving to Flipcommons PKs directly), merges, sheet assembly, date parsing with the edition model.

### Deferred, restated

Date columns (until the edition/merge work consumes them), language axis (post-acquisition detection beats filename parsing), system attachment, slugs on subject rows (PK now, slug+PK when developer #2 arrives), ROM sets and unclassified images, and the document-parts structure for sheet assembly (designed with the assembly work, not ahead of it). Also deferred: materializing the document tables into the DuckDB analytics build (`03_raw_web.sql`) — nothing there consumes them, the plan deliberately relates docs to Flipcommons rather than the DuckDB snapshot, and flippatch's `evidence.sql` can attach `cache.sqlite` directly; materialize when a DuckDB consumer actually exists.
