#!/usr/bin/env python3
import os

import aws_cdk as cdk

# Analytics layer - derives insights from pipeline data
from stacks.analytics.entity_resolution import EntityResolutionStack

# ETL pipelines - extract, transform, load raw data
from stacks.pipelines.ticker_details import TickerDetailsPipelineStack
from stacks.pipelines.ticker_prices import TickerPricesPipelineStack
from stacks.pipelines.tickers import TickersPipelineStack
from stacks.pipelines.trials import TrialsPipelineStack
from stacks.shared import SharedStack

app = cdk.App()

# Environment for stacks that need account/region context (e.g., VPC lookup)
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

# Shared infrastructure
SharedStack(app, "petals-shared")

# ETL pipelines (extract, transform, load)
TickersPipelineStack(app, "petals-tickers-pipeline", env=env)
TickerPricesPipelineStack(app, "petals-ticker-prices-pipeline", env=env)
TrialsPipelineStack(app, "petals-trials-pipeline", env=env)
TickerDetailsPipelineStack(app, "petals-ticker-details-pipeline", env=env)

# Analytics workloads (entity resolution, features, ML)
EntityResolutionStack(app, "petals-entity-resolution-pipeline", env=env)

app.synth()
