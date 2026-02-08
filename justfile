# Petals operational recipes
# Usage: just health | just logs tickers | just trigger tickers

set shell := ["bash", "-uc"]
set dotenv-load

profile := env_var_or_default("AWS_PROFILE", "personal")
region := env_var_or_default("AWS_REGION", "us-east-1")
account_id := env_var("AWS_ACCOUNT_ID")
state_table := "petals-pipeline-state"
table_bucket := "arn:aws:s3tables:" + region + ":" + account_id + ":bucket/petals-tables-" + account_id

# Show pipeline health dashboard
health:
    #!/usr/bin/env bash
    set -euo pipefail
    
    # Colors & symbols
    GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
    BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'
    OK="${GREEN}✓${NC}"; WARN="${YELLOW}◆${NC}"; ERR="${RED}✗${NC}"
    
    # Staleness threshold (seconds) - 2 days
    STALE_THRESHOLD=$((2 * 24 * 60 * 60))
    NOW=$(date +%s)
    
    echo -e "\n${BOLD}Pipeline Health Dashboard${NC}"
    echo -e "${DIM}$(date '+%Y-%m-%d %H:%M:%S')${NC}\n"
    
    # Pipeline definitions: name|namespace|table|daily (1=check staleness)
    pipelines=(
        "tickers|market|tickers|1"
        "ticker_details|market|ticker_details|1"
        "ticker_prices|market|ticker_prices|1"
        "trials|clinical|trials|1"
    )
    
    for entry in "${pipelines[@]}"; do
        IFS='|' read -r name ns tbl daily <<< "$entry"
        
        name_upper=$(echo "$name" | tr '[:lower:]' '[:upper:]')
        echo -e "${BOLD}${name_upper}${NC}"
        
        # State machine name (underscores become hyphens)
        sm_name="petals-${name//_/-}-pipeline"
        sm_arn="arn:aws:states:{{region}}:{{account_id}}:stateMachine:$sm_name"
        
        # Check for running Step Functions executions
        running_count=$(aws stepfunctions list-executions \
            --state-machine-arn "$sm_arn" \
            --status-filter RUNNING \
            --profile {{profile}} --region {{region}} \
            --query 'length(executions)' \
            --output text 2>/dev/null || echo "0")
        
        if [ "$running_count" != "0" ] && [ "$running_count" != "None" ]; then
            # Currently running - show that status
            sym="$WARN"; color="$YELLOW"
            status_text="running"
            last_run_display="now"
            error_msg=""
        else
            # Not running - check DynamoDB for last state
            state=$(aws dynamodb get-item \
                --table-name {{state_table}} \
                --key "{\"pipeline_id\": {\"S\": \"$name\"}}" \
                --profile {{profile}} --region {{region}} \
                --output json 2>/dev/null || echo '{}')
            
            status_raw=$(echo "$state" | jq -r '.Item.last_status.S // "no-state"')
            status=$(echo "$status_raw" | tr '[:upper:]' '[:lower:]')
            last_run=$(echo "$state" | jq -r '.Item.last_run_time.S // ""')
            last_run_display="${last_run:0:19}"
            [ -z "$last_run_display" ] && last_run_display="-"
            error_msg=$(echo "$state" | jq -r '.Item.error.S // empty')
            
            # Check staleness for daily pipelines
            stale=""
            if [ "$daily" = "1" ] && [ -n "$last_run" ] && [ "$status" = "success" ]; then
                # Parse ISO timestamp to epoch (handles both Z and +00:00 formats)
                last_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${last_run:0:19}" +%s 2>/dev/null || echo "0")
                age=$((NOW - last_epoch))
                if [ "$age" -ge "$STALE_THRESHOLD" ]; then
                    stale="stale"
                    days_ago=$((age / 86400))
                fi
            fi
            
            # Determine symbol and color
            if [ -n "$stale" ]; then
                sym="$WARN"; color="$YELLOW"
                status_text="success ${YELLOW}(${days_ago}d stale)${NC}"
            else
                case "$status" in
                    success) sym="$OK"; color="$GREEN"; status_text="$status" ;;
                    failed|error) sym="$ERR"; color="$RED"; status_text="$status" ;;
                    no-state) sym="$WARN"; color="$YELLOW"; status_text="$status" ;;
                    *) sym="$WARN"; color="$YELLOW"; status_text="$status" ;;
                esac
            fi
        fi
        
        echo -e "  State: $sym ${color}${status_text}${NC} ${DIM}(${last_run_display})${NC}"
        [ -n "$error_msg" ] && echo -e "         ${RED}└─ ${error_msg}${NC}"
        
        # S3 Tables check
        ns_check=$(aws s3tables list-namespaces \
            --table-bucket-arn "{{table_bucket}}" \
            --profile {{profile}} --region {{region}} \
            --query "namespaces[?namespace[0]=='$ns']" \
            --output json 2>/dev/null | jq 'length > 0' 2>/dev/null || echo "false")
        
        tbl_check="false"
        if [ "$ns_check" = "true" ]; then
            tbl_check=$(aws s3tables list-tables \
                --table-bucket-arn "{{table_bucket}}" \
                --namespace "$ns" \
                --profile {{profile}} --region {{region}} \
                --query "tables[?name=='$tbl']" \
                --output json 2>/dev/null | jq 'length > 0' 2>/dev/null || echo "false")
        fi
        
        if [ "$tbl_check" = "true" ]; then
            echo -e "  Table: $OK ${ns}.${tbl}"
        elif [ "$ns_check" = "true" ]; then
            echo -e "  Table: $ERR ${ns}.${tbl} ${DIM}(table missing)${NC}"
        else
            echo -e "  Table: $ERR ${ns}.${tbl} ${DIM}(namespace missing)${NC}"
        fi
        echo ""
    done

# =============================================================================
# Testing
# =============================================================================

# Run all unit tests
test *args:
    uv run pytest tests/ cdk/tests/ -v {{args}}

# =============================================================================
# Deploy
# =============================================================================

ecr_repo := "petals-pipelines"
ecr_base_repo := "petals-base"

# Rebuild base image (rare - only when upgrading torch/transformers)
build-base:
    #!/usr/bin/env bash
    set -euo pipefail
    ECR_URI="{{account_id}}.dkr.ecr.{{region}}.amazonaws.com"
    
    echo "Building base image with ML dependencies..."
    docker build -f Dockerfile.base -t petals-base .
    
    echo "Logging into ECR..."
    aws ecr get-login-password --profile {{profile}} --region {{region}} \
        | docker login --username AWS --password-stdin "$ECR_URI" >/dev/null 2>&1
    
    echo "Pushing to ECR..."
    docker tag petals-base:latest "$ECR_URI/{{ecr_base_repo}}:latest"
    docker push "$ECR_URI/{{ecr_base_repo}}:latest"
    
    echo "Cleaning up old base images..."
    # Remove dangling images from this build
    docker image prune -f >/dev/null 2>&1 || true
    
    # Show any old petals-base images (user can manually prune if needed)
    OLD_IMAGES=$(docker images petals-base --format "{{{{.ID}}}} {{{{.CreatedAt}}}}" | tail -n +2 || echo "")
    if [ -n "$OLD_IMAGES" ]; then
        echo "Note: Found old petals-base images (run 'docker image prune -a' to clean):"
        echo "$OLD_IMAGES" | head -3
    fi
    
    echo "✓ Base image updated"

# Full deploy: sync → format → test → build → push → cdk deploy
# Skip tests with: just deploy true
deploy skip_tests="false":
    #!/usr/bin/env bash
    set -euo pipefail
    
    GREEN='\033[0;32m'; YELLOW='\033[1;33m'; DIM='\033[2m'; NC='\033[0m'
    OK="${GREEN}✓${NC}"; SKIP="${YELLOW}↷${NC}"
    
    ECR_URI="{{account_id}}.dkr.ecr.{{region}}.amazonaws.com"
    IMAGE_TAG=$(git rev-parse --short HEAD 2>/dev/null || echo "latest")
    
    # -------------------------------------------------------------------------
    # 1. uv sync
    # -------------------------------------------------------------------------
    echo -e "\n${DIM}[1/6]${NC} Syncing dependencies..."
    if uv sync --all-extras 2>&1 | grep -q "Resolved\|Installed"; then
        echo -e "  $OK Dependencies updated"
    else
        echo -e "  $SKIP Already synced"
    fi
    
    # -------------------------------------------------------------------------
    # 2. Format with ruff
    # -------------------------------------------------------------------------
    echo -e "\n${DIM}[2/6]${NC} Formatting code..."
    formatted=$(uv run ruff format src/ tests/ cdk/ 2>&1)
    if echo "$formatted" | grep -q "file"; then
        echo -e "  $OK Formatted files"
    else
        echo -e "  $SKIP No formatting needed"
    fi
    
    # -------------------------------------------------------------------------
    # 3. Run tests
    # -------------------------------------------------------------------------
    echo -e "\n${DIM}[3/6]${NC} Running tests..."
    if [ "{{skip_tests}}" = "true" ]; then
        echo -e "  $SKIP Skipped (skip_tests=true)"
    else
        uv run pytest tests/ cdk/tests/ -v --tb=short
        echo -e "  $OK All tests passed"
    fi
    
    # -------------------------------------------------------------------------
    # 4. ECR login
    # -------------------------------------------------------------------------
    echo -e "\n${DIM}[4/6]${NC} ECR login..."
    aws ecr get-login-password --profile {{profile}} --region {{region}} \
        | docker login --username AWS --password-stdin "$ECR_URI" >/dev/null 2>&1
    echo -e "  $OK Authenticated"
    
    # -------------------------------------------------------------------------
    # 5. Docker build & push
    # -------------------------------------------------------------------------
    echo -e "\n${DIM}[5/6]${NC} Docker build & push..."
    
    # Compute content hash for skip detection
    CONTENT_HASH=$(cat Dockerfile pyproject.toml uv.lock $(find src -type f -name '*.py') | sha256sum | cut -c1-12)
    
    # Check if this exact image already exists in ECR
    EXISTING=$(aws ecr describe-images \
        --repository-name {{ecr_repo}} \
        --profile {{profile}} --region {{region}} \
        --query "imageDetails[?contains(imageTags, '$CONTENT_HASH')].imageTags" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$EXISTING" ]; then
        echo -e "  $SKIP Image already up-to-date (hash: $CONTENT_HASH)"
    else
        docker build --platform linux/amd64 \
            -t {{ecr_repo}}:$IMAGE_TAG \
            -t {{ecr_repo}}:$CONTENT_HASH \
            -t {{ecr_repo}}:latest . \
            --quiet
        
        docker tag {{ecr_repo}}:$IMAGE_TAG "$ECR_URI/{{ecr_repo}}:$IMAGE_TAG"
        docker tag {{ecr_repo}}:$CONTENT_HASH "$ECR_URI/{{ecr_repo}}:$CONTENT_HASH"
        docker tag {{ecr_repo}}:latest "$ECR_URI/{{ecr_repo}}:latest"
        
        docker push "$ECR_URI/{{ecr_repo}}:$IMAGE_TAG" --quiet
        docker push "$ECR_URI/{{ecr_repo}}:$CONTENT_HASH" --quiet
        docker push "$ECR_URI/{{ecr_repo}}:latest" --quiet
        
        echo -e "  $OK Pushed $IMAGE_TAG (hash: $CONTENT_HASH)"
    fi
    
    # -------------------------------------------------------------------------
    # 6. CDK deploy
    # -------------------------------------------------------------------------
    echo -e "\n${DIM}[6/6]${NC} CDK deploy..."
    cd cdk
    
    # Check for changes - look for "Number of stacks with differences: N"
    DIFF_OUTPUT=$(uv run cdk diff --all --profile {{profile}} 2>&1 || true)
    
    # Extract the number of stacks with differences (0 = no changes)
    STACKS_WITH_DIFF=$(echo "$DIFF_OUTPUT" | grep -o 'Number of stacks with differences: [0-9]*' | grep -o '[0-9]*$' || echo "")
    
    if [ -z "$STACKS_WITH_DIFF" ]; then
        # Couldn't parse diff output - deploy to be safe
        uv run cdk deploy --all --require-approval never --profile {{profile}}
        echo -e "  $OK Deployed all stacks"
    elif [ "$STACKS_WITH_DIFF" = "0" ]; then
        echo -e "  $SKIP No infrastructure changes"
    else
        uv run cdk deploy --all --require-approval never --profile {{profile}}
        echo -e "  $OK Deployed $STACKS_WITH_DIFF stack(s)"
    fi
    
    echo -e "\n${GREEN}✓ Deployment complete${NC}"

# =============================================================================
# Operations
# =============================================================================

# Last N log events for a pipeline (timestamp in descending order)
logs pipeline="tickers" n="50":
    #!/usr/bin/env bash
    # Get recent log streams and fetch events from them
    streams=$(aws logs describe-log-streams \
        --log-group-name /petals/pipelines/{{pipeline}} \
        --profile {{profile}} \
        --region {{region}} \
        --order-by LastEventTime \
        --descending \
        --max-items 5 \
        --output json | jq -r '.logStreams[].logStreamName')
    
    # Fetch events from recent streams
    for stream in $streams; do
        aws logs get-log-events \
            --log-group-name /petals/pipelines/{{pipeline}} \
            --log-stream-name "$stream" \
            --profile {{profile}} \
            --region {{region}} \
            --output json | jq -r '.events[] | {ts: .timestamp, msg: "\(.timestamp / 1000 | strftime("%Y-%m-%d %H:%M:%S")) \(.message)"}'
    done | jq -sr 'sort_by(.ts) | .[-{{n}}:] | reverse | .[].msg'

# Trigger a pipeline (dry-run by default)
trigger pipeline dry="true":
    #!/usr/bin/env bash
    p="{{pipeline}}"
    sm_arn="arn:aws:states:{{region}}:{{account_id}}:stateMachine:petals-${p//_/-}-pipeline"
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
