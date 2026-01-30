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

### clinical.trials

**Purpose:** Completed clinical trials from ClinicalTrials.gov API, filtered to INDUSTRY sponsors (pharma/biotech companies).

**Primary Key:** `nct_id` - unique NCT identifier assigned to each registered study.

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place, new records inserted. Each run fetches studies with `last_update_date` more recent than the previous run.

**Design Notes:**
- Filtered to `COMPLETED` status and `INDUSTRY` sponsor class at extraction time
- `conditions` and `interventions` stored as JSON arrays for Iceberg compatibility
- `has_results` boolean indicates whether study results have been posted
- `completion_date` and `primary_completion_date` are returned in mixed formats in API responses: full dates (`2024-08-06`) or year-month only (`2011-01`). We normalize year-month values to first-of-month (`2011-01` → `2011-01-01`) for consistent date handling and downstream stock price correlation analysis.
