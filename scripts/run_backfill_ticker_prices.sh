#!/usr/bin/env bash
# Backfill market and locale columns in ticker_prices table
#
# This script runs the SQL backfill for historical data before 2026-01-06

set -euo pipefail

PROFILE="${AWS_PROFILE:-personal}"
WORKGROUP="petals"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/backfill_ticker_prices_market_locale.sql"

echo "🔄 Starting backfill for ticker_prices market and locale columns..."
echo "   Profile: $PROFILE"
echo "   Workgroup: $WORKGROUP"
echo ""

# Start query execution
QUERY_ID=$(aws athena start-query-execution \
  --query-string "$(cat "$SQL_FILE")" \
  --work-group "$WORKGROUP" \
  --profile "$PROFILE" \
  --query 'QueryExecutionId' \
  --output text)

echo "✓ Query started: $QUERY_ID"
echo "⏳ Waiting for completion..."

# Poll for completion
while true; do
  STATUS=$(aws athena get-query-execution \
    --query-execution-id "$QUERY_ID" \
    --profile "$PROFILE" \
    --query 'QueryExecution.Status.State' \
    --output text)
  
  if [ "$STATUS" = "SUCCEEDED" ]; then
    echo "✅ Backfill completed successfully!"
    
    # Get row count if available
    aws athena get-query-execution \
      --query-execution-id "$QUERY_ID" \
      --profile "$PROFILE" \
      --query 'QueryExecution.Statistics' \
      --output json
    
    break
  elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
    echo "❌ Query $STATUS"
    aws athena get-query-execution \
      --query-execution-id "$QUERY_ID" \
      --profile "$PROFILE" \
      --query 'QueryExecution.Status.StateChangeReason' \
      --output text
    exit 1
  else
    echo "   Status: $STATUS..."
    sleep 5
  fi
done

echo ""
echo "To verify the backfill, run:"
echo "  aws athena start-query-execution --query-string \"SELECT COUNT(*) FROM market_data.ticker_prices WHERE market='stocks' AND locale='us'\" --work-group petals --profile personal"
