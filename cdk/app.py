#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.pipelines.ticker_details import TickerDetailsPipelineStack
from stacks.pipelines.tickers import TickersPipelineStack
from stacks.pipelines.trials import TrialsPipelineStack
from stacks.shared import SharedStack

app = cdk.App()

# Environment for stacks that need account/region context (e.g., VPC lookup)
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

SharedStack(app, "petals-shared")
TickersPipelineStack(app, "petals-tickers-pipeline", env=env)
TrialsPipelineStack(app, "petals-trials-pipeline", env=env)
TickerDetailsPipelineStack(app, "petals-ticker-details-pipeline", env=env)

app.synth()
