"""
Exploratory analysis: Do completed clinical trials correlate with stock price movement?

Usage:
    export MASSIVE_API_KEY=your_key
    uv run python analysis/trial_stock_impact.py
"""

import json
import os
import time
from datetime import datetime, timedelta

import boto3
import httpx
import polars as pl

# Config
AWS_PROFILE = "personal"
S3_TABLES_CATALOG = "s3tablescatalog/petals-tables-620117234001"
ATHENA_OUTPUT = "s3://petals-artifacts-620117234001/athena-results/"
MASSIVE_BASE_URL = "https://api.massive.com"
EVENT_WINDOW_DAYS = 30


def get_athena_client():
    """Get Athena client with personal profile."""
    session = boto3.Session(profile_name=AWS_PROFILE)
    return session.client("athena", region_name="us-east-1")


def run_athena_query(query: str) -> pl.DataFrame:
    """Execute Athena query and return as Polars DataFrame."""
    client = get_athena_client()

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Catalog": S3_TABLES_CATALOG},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    execution_id = response["QueryExecutionId"]
    print(f"[athena] Query started: {execution_id}")

    while True:
        status = client.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Query {state}: {reason}")

    results = client.get_query_results(QueryExecutionId=execution_id)

    columns = [col["Name"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:  # skip header
        rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])

    return pl.DataFrame(rows, schema=columns, orient="row")


def get_single_trial_ticker(sponsor_filter: str | None = None) -> dict:
    """Get one ticker with its most recently completed trial.

    Args:
        sponsor_filter: Optional sponsor name to filter by (case-insensitive LIKE match)
    """
    sponsor_clause = ""
    if sponsor_filter:
        # Escape single quotes and add LIKE filter
        safe_sponsor = sponsor_filter.replace("'", "''")
        sponsor_clause = f"AND LOWER(sponsor_name) LIKE LOWER('%{safe_sponsor}%')"

    query = f"""
    WITH completed_trials AS (
        SELECT
            sponsor_name,
            nct_id,
            title,
            primary_completion_date
        FROM "clinical"."trials"
        WHERE UPPER(overall_status) = 'COMPLETED'
          AND primary_completion_date IS NOT NULL
          AND primary_completion_date < date_format(current_date, '%Y-%m-%d')
          {sponsor_clause}
    ),
    
    matchings AS (
        SELECT sponsor_name, ticker, market 
        FROM "matching"."sponsor_ticker_candidates"
        WHERE confidence >= 0.75
    ),
    
    tickers AS (
        SELECT ticker, market, name
        FROM "reference"."tickers"
        WHERE market = 'stocks'
    )
    
    SELECT 
        ti.ticker,
        ti.name AS company_name,
        ct.nct_id,
        ct.title,
        ct.primary_completion_date AS completion_date
    FROM completed_trials ct
    INNER JOIN matchings ma ON ct.sponsor_name = ma.sponsor_name
    INNER JOIN tickers ti ON ma.ticker = ti.ticker AND ma.market = ti.market
    ORDER BY ct.primary_completion_date DESC
    LIMIT 1
    """
    df = run_athena_query(query)
    if df.is_empty():
        raise ValueError("No matching trial/ticker found")

    row = df.row(0, named=True)
    print(f"[athena] Found: {row['ticker']} - {row['company_name']}")
    print(f"[athena] Trial: {row['nct_id']} completed {row['completion_date']}")
    return {
        "ticker": row["ticker"],
        "company_name": row["company_name"],
        "nct_id": row["nct_id"],
        "brief_title": row["title"],
        "completion_date": row["completion_date"],
    }


def fetch_trial_details(nct_id: str) -> dict:
    """Fetch trial metadata from ClinicalTrials.gov API."""
    url = f"https://clinicaltrials.gov/api/v2/studies/{nct_id}"
    print(f"[ctgov] Fetching trial details for {nct_id}")

    with httpx.Client() as client:
        resp = client.get(url, timeout=30)
        if resp.status_code != 200:
            print(f"[ctgov] Warning: Could not fetch trial details ({resp.status_code})")
            return {}
        data = resp.json()

    protocol = data.get("protocolSection", {})
    identification = protocol.get("identificationModule", {})
    status = protocol.get("statusModule", {})
    design = protocol.get("designModule", {})
    conditions = protocol.get("conditionsModule", {})

    # Extract phases (can be multiple like ["PHASE2", "PHASE3"])
    phases = design.get("designInfo", {}).get("phases", design.get("phases", []))
    phase_str = ", ".join(phases) if phases else "N/A"

    # Extract enrollment
    enrollment = design.get("enrollmentInfo", {})

    details = {
        "nct_id": nct_id,
        "phase": phase_str,
        "enrollment_count": enrollment.get("count"),
        "enrollment_type": enrollment.get("type"),
        "study_type": design.get("studyType"),
        "conditions": conditions.get("conditions", []),
        "has_results": data.get("hasResults", False),
        "brief_title": identification.get("briefTitle"),
        "official_title": identification.get("officialTitle"),
        "sponsor": identification.get("organization", {}).get("fullName"),
        "primary_completion_date": status.get("primaryCompletionDateStruct", {}).get("date"),
        "completion_date": status.get("completionDateStruct", {}).get("date"),
    }

    print(
        f"[ctgov] Phase: {phase_str}, Enrollment: {enrollment.get('count')}, Has Results: {data.get('hasResults')}"
    )
    return details


def fetch_ohlc(ticker: str, from_date: str, to_date: str) -> pl.DataFrame:
    """Fetch daily OHLC from Massive API."""
    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise ValueError("MASSIVE_API_KEY environment variable required")

    url = f"{MASSIVE_BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    params = {"adjusted": "true", "sort": "asc", "limit": 5000}
    headers = {"Authorization": f"Bearer {api_key}"}

    print(f"[massive] Fetching {ticker} OHLC: {from_date} to {to_date}")

    with httpx.Client() as client:
        resp = client.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

    if data.get("resultsCount", 0) == 0:
        raise ValueError(f"No price data for {ticker} in date range")

    results = data["results"]
    print(f"[massive] Got {len(results)} bars")

    # Convert to DataFrame
    df = pl.DataFrame(results)
    # Timestamp is Unix ms -> convert to date
    df = df.with_columns(pl.from_epoch("t", time_unit="ms").alias("date"))
    return df.select(["date", "o", "h", "l", "c", "v", "vw"])


def calculate_returns(df: pl.DataFrame, event_date: str) -> pl.DataFrame:
    """Calculate returns relative to event date."""
    event_dt = datetime.strptime(event_date, "%Y-%m-%d").date()

    df = df.with_columns(
        [
            ((pl.col("c") - pl.col("c").shift(1)) / pl.col("c").shift(1) * 100).alias(
                "daily_return_pct"
            ),
            (pl.col("date").cast(pl.Date) - pl.lit(event_dt))
            .dt.total_days()
            .alias("days_from_event"),
        ]
    )

    # Find the close on event date (or nearest prior)
    pre_event = df.filter(pl.col("days_from_event") <= 0).sort("days_from_event", descending=True)
    if pre_event.is_empty():
        base_price = df["c"][0]
    else:
        base_price = pre_event["c"][0]

    df = df.with_columns(
        ((pl.col("c") - base_price) / base_price * 100).alias("cumulative_return_pct")
    )

    return df


def main(sponsor_filter: str | None = None):
    """Run the analysis.

    Args:
        sponsor_filter: Optional sponsor name to filter by (case-insensitive LIKE match)
    """
    # Step 1: Get trial/ticker from Iceberg tables
    trial = get_single_trial_ticker(sponsor_filter)
    ticker = trial["ticker"]
    completion_date = trial["completion_date"]
    nct_id = trial["nct_id"]

    # Step 2: Fetch trial details from ClinicalTrials.gov
    trial_details = fetch_trial_details(nct_id)

    # Step 3: Calculate date window
    event_dt = datetime.strptime(completion_date, "%Y-%m-%d")
    from_date = (event_dt - timedelta(days=EVENT_WINDOW_DAYS)).strftime("%Y-%m-%d")
    to_date = (event_dt + timedelta(days=EVENT_WINDOW_DAYS)).strftime("%Y-%m-%d")

    # Step 4: Fetch OHLC
    ohlc = fetch_ohlc(ticker, from_date, to_date)

    # Step 5: Calculate returns
    returns = calculate_returns(ohlc, completion_date)

    # Step 6: Output summary
    phase = trial_details.get("phase", "N/A")
    enrollment = trial_details.get("enrollment_count", "N/A")
    conditions = ", ".join(trial_details.get("conditions", [])[:2]) or "N/A"

    print("\n" + "=" * 60)
    print(f"TRIAL COMPLETION EVENT STUDY: {ticker}")
    print(f"Trial: {nct_id} - {trial['brief_title'][:50]}...")
    print(f"Phase: {phase} | Enrollment: {enrollment} | Conditions: {conditions}")
    print(f"Completion Date: {completion_date}")
    print("=" * 60)

    # Pre-event vs post-event summary
    pre = returns.filter(pl.col("days_from_event") < 0)
    post = returns.filter(pl.col("days_from_event") >= 0)

    if not pre.is_empty() and not post.is_empty():
        pre_return = pre["cumulative_return_pct"][-1]
        post_return = post["cumulative_return_pct"][-1] if not post.is_empty() else 0

        print(f"\nPre-event return (T-{EVENT_WINDOW_DAYS} to T-1): {pre_return:.2f}%")
        print(f"Post-event return (T to T+{EVENT_WINDOW_DAYS}): {post_return:.2f}%")
        print(
            f"Event day close: ${ohlc.filter(pl.col('date').cast(pl.Date) >= datetime.strptime(completion_date, '%Y-%m-%d').date()).head(1)['c'][0]:.2f}"
        )

    # Save outputs
    os.makedirs("analysis/output", exist_ok=True)
    base_path = f"analysis/output/{ticker}_{completion_date}_event_study"

    # Save price data
    returns.write_parquet(f"{base_path}.parquet")

    # Save metadata
    metadata = {
        "ticker": ticker,
        "company_name": trial["company_name"],
        "event_date": completion_date,
        "event_window_days": EVENT_WINDOW_DAYS,
        "trial": trial_details,
    }
    with open(f"{base_path}_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\n[output] Saved to {base_path}.parquet")
    print(f"[output] Saved to {base_path}_metadata.json")

    return returns, metadata


if __name__ == "__main__":
    df, meta = main()
    print("\n--- Sample Data ---")
    print(df.select(["date", "c", "daily_return_pct", "days_from_event", "cumulative_return_pct"]))
