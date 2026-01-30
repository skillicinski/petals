# Design Document

## Principles

**Data outlives pipelines.** Extraction logic will be refactored, replaced, or deprecated while the data remains valuable. Infrastructure lifecycle should not dictate data lifecycle.

**Pipelines are standalone.** Each pipeline is deployed independently without impacting others. Isolated blast radius, simple reasoning.

**Pipelines are idempotent.** Every process that involves data should produce predictable outputs. Change detection is key to produce efficient writes of new data without altering closed historical information.

---

## Architecture

Key architectural choices and rationale:

**Build pipelines as a single Docker Image** - Pipeline code gets bundled and containerised using Docker in order to reproduce most functionality locally for development and testing purposes.

**Batch over Lambda** - Full extractions take ~15 minutes; Batch with Fargate Spot handles longer runs and provides significant cost savings.

**Step Functions over direct EventBridge → Batch** - Enables state tracking (last_run_time), error handling with failure recording, and visible execution history.

**S3 Tables over regular S3 + Glue Catalog** - Native Iceberg support with automatic compaction, integrated Lake Formation permissions, no catalog management overhead.

**Shared infrastructure stack** - DynamoDB state table and S3 Tables bucket are shared across pipelines; lifecycle decoupled from individual pipeline stacks.

See [ARCHITECTURE.md](ARCHITECTURE.md) for system diagram.

---

## Tables

### reference.tickers

**Purpose:** Master list of tradeable instruments from Massive (formerly Polygon.io) API.

**Primary Key:** `(ticker, market)` composite - same symbol can exist in different markets (e.g., BITW in both `stocks` and `otc`).

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place, new records inserted. Each new pipeline run retieves records with a `last_updated_utc` more recent than the previous run.

**Design Notes:**
- Considered SCD Type 2 (valid_from/valid_to) but `list_date` requires detail endpoint (1 call per ticker, 146+ hours for backfill at rate limit)
- `delisted_utc` from bulk API provides delisting date when available
- `active` boolean indicates current trading status

### reference.ticker_details

**Purpose:** Enriched ticker details from Massive API, focused on pharma/biotech/medical companies for entity matching with clinical trial sponsors.

**Primary Key:** `(ticker, market)` composite - matches the tickers table.

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place. Incremental runs filter source tickers by `last_updated_utc`.

**Design Notes:**
- Filtered to pharma-like ticker names at extraction time to reduce API calls (~3,600 vs 68,000)
- Key fields: `sic_code`, `sic_description` for industry classification
- `description`, `homepage_url` useful for LLM-based entity matching verification
- ADRs (foreign companies) have no SIC code - handled separately in matching logic
- Initial backfill takes ~12 hours due to API rate limiting (5 calls/min)

### clinical.trials

**Purpose:** Completed clinical trials from ClinicalTrials.gov API, filtered to INDUSTRY sponsors (pharma/biotech companies).

**Primary Key:** `nct_id` - unique NCT identifier assigned to each registered study.

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place, new records inserted. Each run fetches studies with `last_update_date` more recent than the previous run.

**Design Notes:**
- Filtered to `COMPLETED` status and `INDUSTRY` sponsor class at extraction time
- `conditions` and `interventions` stored as JSON arrays for Iceberg compatibility
- `has_results` boolean indicates whether study results have been posted
- `completion_date` and `primary_completion_date` are returned in mixed formats in API responses: full dates (`2024-08-06`) or year-month only (`2011-01`). We normalize year-month values to first-of-month (`2011-01` → `2011-01-01`) for consistent date handling and downstream stock price correlation analysis.

---

## Entity Matching: Sponsors to Tickers

**Goal:** Link clinical trial sponsors (`clinical.trials.sponsor_name`) to public company tickers (`reference.tickers`) to enable cross-domain analysis (e.g., trial completion → stock price correlation).

### Problem Space

Trial sponsors and ticker names don't match exactly:
- Name variations: "Pfizer Inc" vs "Pfizer" vs "PFIZER INC."
- Legal suffixes: Inc., Corp., Ltd., PLC, AG, A/S, GmbH, etc.
- Abbreviations: "GlaxoSmithKline" vs "GSK", "Johnson & Johnson" vs "J&J"
- ADR naming: European pharma trades under different names as ADRs
- Private companies: Some major sponsors (e.g., Boehringer Ingelheim) aren't publicly traded

### Exploration Findings (2026-01-30)

Data scale:
- ~64k unique ticker names, ~10k unique sponsor names
- ~84k completed INDUSTRY-sponsored trials
- Only 175 exact matches after basic normalization

Matching pipeline tested:
1. **Normalization** - lowercase, strip suffixes, handle `&` vs `and`
2. **Alias lookup** - manual mappings for known variations (GSK, Roche, Bayer)
3. **Fuzzy matching** - rapidfuzz token_sort_ratio + token_set_ratio
4. **Confidence tiering** - high (≥90), medium (75-89), low (60-74), none (<60)

Results (1000 sponsor sample):
- High confidence: 26.7%
- Medium confidence: 42.7%
- Low/None: 30.6%
- **Actionable (high + medium): 69.4%**

The ~43% in medium tier contains both true positives and false positives - this is where additional verification adds value.

### Open Questions

*Decision pending on approach for alias generation and match verification:*

- **LLM-assisted aliasing** - generate company aliases programmatically vs. manual curation
- **LLM verification** - validate medium-confidence matches using company context
- **Ticker enrichment** - pull additional metadata (SIC codes, industry, description) from Massive API to improve matching signals
- **Output table design** - schema for the sponsor→ticker mapping table
