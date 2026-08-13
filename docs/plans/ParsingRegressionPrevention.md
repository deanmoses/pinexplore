# Preventing regressions when changing parsing

The PDF text parsing has grown quite complicated, to the point that changes are now introducing regressions in the actual readable text that Flippatch relies on for quotes.

While we're at it, we should also prevent regressions to HTML page parsing, which is also gotten quite complicated.

We need better quality control on any change to PDF text generation.

## Options

### ❌ Synthetic PDFs

I don't like this because it's the formatting we DON'T think of that bites us.

### ❌ Check PDFs into git

Create standard gold file tests over them that verify that parsing produces known good output. I don't want to do this because those are huge files to put into git, and adding someone else's PDFs to a public repo is not good form.

### PDFs in DB

Verify against the actual files in the database, using actual quotes from Flippatch.

- Flippatch hands over its citations and Pinexplore checks them into git, where they can be diffed. Every cite Flippatch has ever shipped becomes coverage, which beats hand-picked gold documents.
- We can create contracts separately from Flippatch too. Necessary for right now where we're testing feature matrices and Flippatch hasn't quoted any yet.
- The contract errors / test fails without the blob.
- Use it for all content types, not just PDFs. Our text extraction has gotten quite complicated.
- I would imagine the contract includes the following information:
  - `content_sha` ⬅️ because a refetched document can fail because the content changed
  - `locator`: "page 2"
  - `quote`: "some verbatim string with [...] ellipses just like citations have"
  - `source`: "flippatch patch 0142" ⬅️ will probably help with reconciling what quotes we've already acquired from flippatch?
