#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.shared import SharedStack

app = cdk.App()

SharedStack(app, "petals-shared")

app.synth()
