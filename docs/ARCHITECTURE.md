# Architecture

## Overview

Petals is a data platform for market data, built on AWS with infrastructure-as-code (CDK). The architecture prioritizes cost efficiency and operational simplicity.

## System Diagram

```mermaid
flowchart TB
    subgraph External["External Services"]
        API[Massive API]
        CTGOV[ClinicalTrials.gov API]
        SM[Secrets Manager]
    end

    subgraph TickersPipeline["Tickers Pipeline (petals-tickers-pipeline)"]
        EB1[EventBridge<br/>Daily 6 AM UTC]
        SFN1[Step Functions]
        BATCH1[AWS Batch<br/>Fargate Spot]
    end

    subgraph TickerDetailsPipeline["Ticker Details Pipeline (petals-ticker-details-pipeline)"]
        EB3[EventBridge<br/>Daily 7 AM UTC]
        SFN3[Step Functions]
        BATCH3[AWS Batch<br/>Fargate Spot]
    end

    subgraph TrialsPipeline["Trials Pipeline (petals-trials-pipeline)"]
        EB2[EventBridge<br/>Daily 7 AM UTC]
        SFN2[Step Functions]
        BATCH2[AWS Batch<br/>Fargate Spot]
    end

    subgraph EntityMatchPipeline["Entity Match Pipeline (petals-entity-match-pipeline)"]
        SFN4[Step Functions<br/>On-demand]
        BATCH4[AWS Batch<br/>Fargate Spot]
    end

    subgraph Shared["Shared Stack (petals-shared)"]
        ECR[ECR<br/>petals-pipelines]
        DDB[(DynamoDB<br/>Pipeline State)]
        subgraph S3Tables["S3 Tables (Iceberg)"]
            NS_MARKET[market.*]
            NS_CLINICAL[clinical.*]
            NS_RELATION[relation.*]
        end
    end

    subgraph Query["Query Layer"]
        ATH[Athena]
        USER[Analyst / dbt]
    end

    %% Tickers pipeline flow
    API --> BATCH1
    SM --> BATCH1
    EB1 --> SFN1
    SFN1 <-->|state| DDB
    SFN1 --> BATCH1
    ECR --> BATCH1
    BATCH1 -->|upsert| NS_MARKET

    %% Ticker details pipeline flow
    NS_MARKET -->|read US stock tickers| BATCH3
    API --> BATCH3
    SM --> BATCH3
    EB3 --> SFN3
    SFN3 <-->|state| DDB
    SFN3 --> BATCH3
    ECR --> BATCH3
    BATCH3 -->|upsert| NS_MARKET

    %% Trials pipeline flow
    CTGOV --> BATCH2
    EB2 --> SFN2
    SFN2 <-->|state| DDB
    SFN2 --> BATCH2
    ECR --> BATCH2
    BATCH2 -->|upsert| NS_CLINICAL

    %% Entity match pipeline flow
    NS_MARKET -->|read tickers| BATCH4
    NS_CLINICAL -->|read sponsors| BATCH4
    SFN4 --> BATCH4
    ECR --> BATCH4
    BATCH4 -->|write matches| NS_RELATION

    %% Query flow
    USER --> ATH
    ATH --> S3Tables
```

## Data Flow

### Tickers Pipeline (daily, 6 AM UTC)
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → container fetches from Massive API (incremental) → upserts to S3 Tables `market.tickers` → records new timestamp

### Ticker Details Pipeline (daily, 7 AM UTC)
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → reads US stock tickers from `market.tickers` (`market='stocks'`, `locale='us'`) → fetches detailed info from Massive API (SIC codes, descriptions, etc.) → upserts to S3 Tables `market.ticker_details` → records new timestamp

*Note: Initial backfill takes ~54 hours due to API rate limiting (5 calls/min). Incremental runs are much faster. Industry filtering (pharma/biotech via SIC codes) is done downstream.*

### Trials Pipeline (daily, 7 AM UTC)
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → container fetches COMPLETED studies from ClinicalTrials.gov (filtered to INDUSTRY sponsors) → upserts to S3 Tables `clinical.trials` → records new timestamp

### Entity Match Pipeline (on-demand)
Manual trigger → Step Functions submits Batch job → reads sponsors from `clinical.trials` and tickers from `market.ticker_details` → computes sentence-transformer embeddings → scores candidate pairs via cosine similarity → applies greedy 1:1 matching → writes matched pairs to `relation.sponsor_ticker_candidates`

*Note: Uses CPU-only embedding model (`all-MiniLM-L6-v2`), no GPU required. Full run completes in ~21 seconds.*

### Query
Athena queries S3 Tables via federated catalog (`s3tablescatalog`)

## Namespaces

| Namespace | Table | Description |
|-----------|-------|-------------|
| `market` | `tickers` | Stock/ETF ticker reference data from Massive API |
| `market` | `ticker_details` | Enriched ticker details (SIC codes, descriptions) for US stock tickers |
| `market` | `ticker_prices` | Daily OHLCV price data for tickers (yfinance + historical data) |
| `clinical` | `trials` | Completed clinical trials from ClinicalTrials.gov (INDUSTRY sponsors) |
| `relation` | `sponsor_ticker_candidates` | Sponsor↔ticker matches with confidence scores (embedding similarity) |

