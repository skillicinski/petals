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

**Stale lock detection for long-running pipelines** - Pipelines like `ticker_details` that run for many hours use a DynamoDB-based lock to prevent concurrent executions. A Lambda function checks lock age before skipping: if a lock is older than the job timeout (e.g., 7 days for a 6-day job), it's considered stale and the new execution proceeds. This handles cases where a job is killed externally (timeout, spot interruption) without releasing its lock.

See [ARCHITECTURE.md](ARCHITECTURE.md) for system diagram.

---

## Tables

### market.tickers

**Purpose:** Master list of tradeable instruments from Massive (formerly Polygon.io) API.

**Primary Key:** `(ticker, market)` composite - same symbol can exist in different markets (e.g., BITW in both `stocks` and `otc`).

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place, new records inserted. Each new pipeline run retieves records with a `last_updated_utc` more recent than the previous run.

**Design Notes:**
- Considered SCD Type 2 (valid_from/valid_to) but `list_date` requires detail endpoint (1 call per ticker, 146+ hours for backfill at rate limit)
- `delisted_utc` from bulk API provides delisting date when available
- `active` boolean indicates current trading status

### market.ticker_details

**Purpose:** Enriched ticker details from Massive API for US stock tickers. SIC codes enable downstream industry filtering (e.g., pharma/biotech for entity matching with clinical trial sponsors).

**Primary Key:** `(ticker, market)` composite - matches the tickers table.

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place. Incremental runs filter source tickers by `last_updated_utc`.

**Design Notes:**
- Filtered to US stock tickers (`market='stocks'`, `locale='us'`) at extraction time (~15,000 tickers)
- Industry filtering (pharma/biotech) done downstream using `sic_code` field
- Key SIC codes: 2833 (Medicinal Chemicals), 2834 (Pharmaceuticals), 2835 (Diagnostics), 2836 (Biologicals)
- `description`, `homepage_url` useful for LLM-based entity matching verification
- ADRs (foreign companies) have no SIC code - handled separately in matching logic
- Initial backfill takes ~54 hours due to API rate limiting (5 calls/min)

### market.ticker_prices

**Purpose:** Daily OHLCV (Open, High, Low, Close, Volume) price data for tickers using `yfinance`. Historical data backfilled manually using files from https://stooq.com/db/h/.

**Primary Key:** `(ticker, date)` composite - unique daily price record per ticker.

**Change Strategy:** Fact table using upsert to handle corrections or backfills. New records are inserted; existing (ticker, date) combinations are updated in place.

**Design Notes:**
- Uses monthly partitioning on the `date` column (`TruncateTransform(7)` truncates YYYY-MM-DD string to YYYY-MM) to optimize date range queries (YTD, quarterly reports) and time-series analysis. Hidden partitions like `date_month=2024-01` are automatically created. For new tables, the partition spec is applied at creation; for existing unpartitioned tables, adding the spec only partitions future data while existing data remains unpartitioned (requires a full rewrite to partition historical data).

### clinical.trials

**Purpose:** Completed clinical trials from ClinicalTrials.gov API, filtered to INDUSTRY sponsors (pharma/biotech companies).

**Primary Key:** `nct_id` - unique NCT identifier assigned to each registered study.

**Change Strategy:** SCD Type 1 (upsert) - existing records updated in place, new records inserted. Each run fetches studies with `last_update_date` more recent than the previous run.

**Design Notes:**
- Filtered to `COMPLETED` status and `INDUSTRY` sponsor class at extraction time
- `conditions` and `interventions` stored as JSON arrays for Iceberg compatibility
- `has_results` boolean indicates whether study results have been posted
- `completion_date` and `primary_completion_date` are returned in mixed formats in API responses: full dates (`2024-08-06`) or year-month only (`2011-01`). We normalize year-month values to first-of-month (`2011-01` → `2011-01-01`) for consistent date handling and downstream stock price correlation analysis.

## Namespaces

### market
Contains reference data and market prices for financial instruments (tickers, ticker details, daily prices).

### clinical
Contains clinical trial data from ClinicalTrials.gov.

### relation
Reserved for relational/mapping tables that link entities across datasets (e.g., sponsor-to-ticker mappings). Currently empty.
