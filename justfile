# Petals operational recipes
# Usage: just health | just logs tickers | just trigger tickers

set shell := ["bash", "-uc"]

profile := "personal"
region := "us-east-1"
state_table := "petals-pipeline-state"
pipelines := "tickers trials entity_match ticker_details"

# Show pipeline health from DynamoDB state
health:
    @echo "=== Pipeline Health ==="
    @aws dynamodb scan \
        --table-name {{state_table}} \
        --profile {{profile}} \
        --region {{region}} \
        --query 'Items[].{pipeline: pipeline_id.S, status: last_status.S, last_run: last_run_time.S, error: error.S}' \
        --output table

# Check recent Step Functions executions
executions pipeline="all":
    #!/usr/bin/env bash
    if [ "{{pipeline}}" = "all" ]; then
        plist="{{pipelines}}"
    else
        plist="{{pipeline}}"
    fi
    for p in $plist; do
        sm_name="petals-${p//_/-}-pipeline"
        echo "=== $sm_name ==="
        aws stepfunctions list-executions \
            --state-machine-arn "arn:aws:states:{{region}}:$(aws sts get-caller-identity --profile {{profile}} --query Account --output text):stateMachine:$sm_name" \
            --profile {{profile}} \
            --region {{region}} \
            --max-results 3 \
            --query 'executions[].{status: status, start: startDate, stop: stopDate}' \
            --output table 2>/dev/null || echo "(not deployed)"
    done

# Last N log events for a pipeline (chronological order)
logs pipeline="tickers" n="50":
    @aws logs filter-log-events \
        --log-group-name /petals/pipelines/{{pipeline}} \
        --profile {{profile}} \
        --region {{region}} \
        --start-time $(($(date +%s) * 1000 - 7 * 24 * 60 * 60 * 1000)) \
        --output json \
    | jq -r '[.events[] | {ts: .timestamp, msg: "\(.timestamp / 1000 | strftime("%Y-%m-%d %H:%M:%S")) \(.message)"}] | sort_by(.ts) | .[-{{n}}:] | .[].msg'

# Trigger a pipeline (dry-run by default)
trigger pipeline dry="true":
    #!/usr/bin/env bash
    p="{{pipeline}}"
    sm_arn="arn:aws:states:{{region}}:$(aws sts get-caller-identity --profile {{profile}} --query Account --output text):stateMachine:petals-${p//_/-}-pipeline"
    if [ "{{dry}}" = "true" ]; then
        echo "DRY RUN: would start $sm_arn"
        echo "Run with: just trigger {{pipeline}} false"
    else
        aws stepfunctions start-execution \
            --state-machine-arn "$sm_arn" \
            --input '{"force_full": false}' \
            --profile {{profile}} \
            --region {{region}}
    fi

# Full health check (state + recent executions)
check: health executions
