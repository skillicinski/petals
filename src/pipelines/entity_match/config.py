"""Configuration for entity matching pipeline.

Defines thresholds, table names, and blocking rules.
Currently specific to sponsor↔ticker matching.

Matching approach: sentence-transformer embeddings with cosine similarity.
Thresholds calibrated for all-MiniLM-L6-v2 model.
"""

# Confidence thresholds (for embedding cosine similarity)
# These are lower than traditional string matching because embeddings
# capture semantic relatedness, not just lexical similarity
CONFIDENCE_AUTO_APPROVE = 0.85  # High confidence - auto-approve
CONFIDENCE_AUTO_REJECT = 0.65  # Low confidence - auto-reject
# Range 0.65-0.85 = pending human review

# Status values
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"

# Output table
OUTPUT_TABLE = "matching.sponsor_ticker_candidates"

# Blocking configuration (token-based pre-filter)
MIN_TOKEN_LENGTH = 2  # Ignore tokens shorter than this
COMMON_TOKENS = {
    # Legal suffixes to ignore in blocking
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
    # Industry terms (too common in this domain)
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
    # Generic business terms
    "international",
    "global",
    "group",
    "holdings",
    "the",
}
