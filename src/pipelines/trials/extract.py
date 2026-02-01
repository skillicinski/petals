"""
Clinical trials extraction from ClinicalTrials.gov API v2.

Fetches completed studies and yields them as flattened dicts.
Optionally filters to INDUSTRY sponsors only (pharma/biotech companies).

API Docs: https://clinicaltrials.gov/data-api/api
"""

import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterator

BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


def _normalize_date(date_str: str | None) -> str | None:
    """
    Normalize dates from ClinicalTrials.gov API.

    The API returns dates in mixed formats:
    - Full dates: "2024-08-06"
    - Year-month only: "2011-01"

    We normalize year-month to first-of-month for consistent downstream analysis.
    """
    if not date_str:
        return None
    # Year-month format (YYYY-MM) → normalize to first of month
    if len(date_str) == 7 and date_str[4] == "-":
        return f"{date_str}-01"
    return date_str


def _extract_study_fields(study: dict) -> dict:
    """
    Flatten a study response into a clean record for storage.

    Extracts key fields from the nested protocolSection structure.
    Arrays (conditions, interventions) are JSON-serialized for Iceberg compatibility.
    """
    protocol = study.get("protocolSection", {})

    # Identification
    id_module = protocol.get("identificationModule", {})
    org = id_module.get("organization", {})

    # Status and dates
    status_module = protocol.get("statusModule", {})
    completion = status_module.get("completionDateStruct", {})
    primary_completion = status_module.get("primaryCompletionDateStruct", {})

    # Sponsor
    sponsor_module = protocol.get("sponsorCollaboratorsModule", {})
    lead_sponsor = sponsor_module.get("leadSponsor", {})

    # Design
    design_module = protocol.get("designModule", {})
    enrollment = design_module.get("enrollmentInfo", {})
    phases = design_module.get("phases", [])

    # Conditions and interventions
    conditions_module = protocol.get("conditionsModule", {})
    conditions = conditions_module.get("conditions", [])

    arms_module = protocol.get("armsInterventionsModule", {})
    interventions = arms_module.get("interventions", [])

    # Extract intervention names/types
    intervention_list = [{"type": i.get("type"), "name": i.get("name")} for i in interventions]

    return {
        "nct_id": id_module.get("nctId"),
        "title": id_module.get("briefTitle"),
        "official_title": id_module.get("officialTitle"),
        "sponsor_name": lead_sponsor.get("name"),
        "sponsor_class": lead_sponsor.get("class"),
        "organization_name": org.get("fullName"),
        "organization_class": org.get("class"),
        "overall_status": status_module.get("overallStatus"),
        "completion_date": _normalize_date(completion.get("date")),
        "completion_date_type": completion.get("type"),
        "primary_completion_date": _normalize_date(primary_completion.get("date")),
        "study_type": design_module.get("studyType"),
        "phases": json.dumps(phases) if phases else None,
        "enrollment_count": enrollment.get("count"),
        "enrollment_type": enrollment.get("type"),
        "has_results": study.get("hasResults", False),
        "conditions": json.dumps(conditions) if conditions else None,
        "interventions": json.dumps(intervention_list) if intervention_list else None,
        "last_update_date": status_module.get("lastUpdateSubmitDate"),
        "study_first_submit_date": status_module.get("studyFirstSubmitDate"),
    }


def fetch_studies(
    status: str = "COMPLETED",
    sponsor_class: str | None = "INDUSTRY",
    page_size: int = 100,
    rate_limit_delay: float = 0.5,
    max_retries: int = 5,
    updated_since: datetime | None = None,
) -> Iterator[dict]:
    """
    Fetch studies from ClinicalTrials.gov API.

    Args:
        status: Overall status filter (default: COMPLETED)
        sponsor_class: Filter to specific sponsor class (INDUSTRY, OTHER, etc.)
                       Set to None to fetch all sponsors.
        page_size: Results per page (max 1000, default 100)
        rate_limit_delay: Seconds between requests
        max_retries: Max retries on rate limit (429)
        updated_since: Only fetch studies updated after this date (ISO format)

    Yields:
        Flattened study dicts ready for loading
    """
    params = [f"pageSize={page_size}", f"filter.overallStatus={status}"]

    if updated_since:
        # API uses query.term with AREA/RANGE syntax for date filtering
        date_str = updated_since.strftime("%Y-%m-%d")
        params.append(f"query.term=AREA[LastUpdatePostDate]RANGE[{date_str},MAX]")
        print(f"Incremental mode: fetching studies updated since {date_str}")
    else:
        print(f"Full extraction mode: fetching all {status} studies")

    if sponsor_class:
        print(f"Filtering to sponsor class: {sponsor_class}")

    url = f"{BASE_URL}?{'&'.join(params)}"
    page = 0
    total_yielded = 0
    total_skipped = 0

    while url:
        retries = 0
        data = None

        while retries < max_retries:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/json")

            try:
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    retries += 1
                    wait_time = rate_limit_delay * (2**retries)
                    msg = f"Rate limited ({retries}/{max_retries}), waiting {wait_time:.1f}s..."
                    print(msg)
                    time.sleep(wait_time)
                else:
                    raise

        if data is None:
            raise RuntimeError(f"Max retries ({max_retries}) exceeded on page {page}")

        studies = data.get("studies", [])
        page_yielded = 0
        page_skipped = 0

        for study in studies:
            record = _extract_study_fields(study)

            # Filter by sponsor class if specified
            if sponsor_class and record.get("sponsor_class") != sponsor_class:
                page_skipped += 1
                total_skipped += 1
                continue

            # Skip records without nct_id (shouldn't happen but be safe)
            if not record.get("nct_id"):
                page_skipped += 1
                total_skipped += 1
                continue

            yield record
            page_yielded += 1
            total_yielded += 1

        page += 1
        print(f"Page {page}: +{page_yielded} -{page_skipped} (total: {total_yielded})")

        # Pagination via nextPageToken
        next_token = data.get("nextPageToken")
        if next_token:
            url = f"{BASE_URL}?{'&'.join(params)}&pageToken={next_token}"
            time.sleep(rate_limit_delay)
        else:
            url = None

    print(f"Fetch complete: {total_yielded} studies ({total_skipped} skipped)")


if __name__ == "__main__":
    # Quick test - fetch first page only
    count = 0
    for study in fetch_studies(page_size=10):
        print(f"{study['nct_id']}: {study['sponsor_name']} - {study['title'][:60]}...")
        count += 1
        if count >= 5:
            break
