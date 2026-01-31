"""Configuration for entity matching pipeline.

Defines thresholds, table names, and blocking rules.
Currently specific to sponsor↔ticker matching.

Future generalization note:
    To make this a generic entity matcher, parameterize:
    - Source tables and columns
    - Entity columns to match on
    - Output table name
"""

# Confidence thresholds
CONFIDENCE_AUTO_APPROVE = 0.90  # Machine auto-approves above this
CONFIDENCE_AUTO_REJECT = 0.50  # Machine auto-rejects below this
CONFIDENCE_REVIEW_LOW = 0.50  # Lower bound for human review
CONFIDENCE_REVIEW_HIGH = 0.90  # Upper bound for human review

# Status values
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"

# Output table
OUTPUT_TABLE = "matching.sponsor_ticker_candidates"

# Blocking configuration
MIN_TOKEN_LENGTH = 2  # Ignore tokens shorter than this
COMMON_TOKENS = {
    # Legal suffixes to ignore in blocking (but keep for scoring)
    "inc",
    "corp",
    "corporation",
    "ltd",
    "limited",
    "llc",
    "plc",
    "ag",
    "sa",
    "nv",
    "bv",
    "gmbh",
    "co",
    "company",
    "companies",
    "pharmaceutical",
    "pharmaceuticals",
    "pharma",
    "biotech",
    "therapeutics",
    "biosciences",
    "laboratories",
    "lab",
    "labs",
    "healthcare",
    "health",
    "medical",
    "sciences",
    "science",
    "international",
    "global",
    "group",
    "holdings",
    "the",
}
