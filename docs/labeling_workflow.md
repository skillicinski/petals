# Ground Truth Labeling Workflow

## Goal
Create 50-100 labeled examples to scientifically measure entity resolution quality.

## How You Can Help

### Option 1: Label from Pipeline Output (Recommended)

**Step 1: Generate candidates**
```bash
# Run pipeline with moderate limits to get diverse examples
AWS_PROFILE=personal LIMIT_SPONSORS=200 LIMIT_TICKERS=500 \
  OUTPUT_PATH=data/candidates_for_labeling.parquet \
  uv run python -m src.analytics.entity_resolution.main
```

**Step 2: Review candidates**
```bash
# View candidates in order of confidence
python << 'EOF'
import polars as pl

df = pl.read_parquet('data/candidates_for_labeling.parquet')

# Show diverse examples (high, medium, low confidence)
high = df.filter(pl.col('confidence') >= 0.90).head(20)
medium = df.filter((pl.col('confidence') >= 0.70) & (pl.col('confidence') < 0.90)).head(20)
low = df.filter((pl.col('confidence') >= 0.60) & (pl.col('confidence') < 0.70)).head(20)

print("HIGH CONFIDENCE (likely correct):")
print(high.select(['sponsor_name', 'ticker', 'ticker_name', 'confidence']))

print("\nMEDIUM CONFIDENCE (uncertain):")
print(medium.select(['sponsor_name', 'ticker', 'ticker_name', 'confidence']))

print("\nLOW CONFIDENCE (likely incorrect):")
print(low.select(['sponsor_name', 'ticker', 'ticker_name', 'confidence']))
EOF
```

**Step 3: Research and label**

For each match, determine if sponsor → ticker is correct:

1. **Google the sponsor name**
   - Is it a real company?
   - Is it publicly traded?
   - Was it acquired?

2. **Check the ticker**
   - Go to finance.yahoo.com/quote/{TICKER}
   - Does the company name match?
   - Check "Profile" tab for details

3. **Label**
   - `correct`: Same company or direct subsidiary
   - `incorrect`: Different companies
   - `unknown`: Can't determine (need more research)

4. **Add notes**
   - Why did you make this decision?
   - Any tricky aspects?

**Step 4: Add to CSV**
```bash
# Edit data/ground_truth/sponsor_ticker_labels.csv
# Add new rows with your labels
```

---

### Option 2: Strategic Sampling

Sample specific categories to ensure diversity:

**Category 1: Exact matches (easy - should be correct)**
- "Pfizer Inc" → PFE
- "Moderna, Inc." → MRNA

**Category 2: Subsidiaries (medium - may be tricky)**
- "Genentech" → RHHBY (owned by Roche)
- "Instagram" → META (owned by Meta)

**Category 3: Abbreviations (medium)**
- "3M" → MMM
- "J&J" → JNJ

**Category 4: Similar names (hard - likely incorrect)**
- "Novartis Pharma" → PFE (wrong, should be NVS)
- "Pfizer Research" → MRNA (wrong, should be PFE)

**Category 5: Complex cases (hard)**
- Warrants vs stocks (LBPWQ vs LBPS)
- OTC tickers vs main exchanges
- Merged/acquired companies

---

### Option 3: Semi-Automated Labeling

I can help you generate candidate labels that you review:

```python
# Script to auto-label obvious cases
import polars as pl

df = pl.read_parquet('data/candidates_for_labeling.parquet')

# Auto-label high confidence exact matches
def fuzzy_match(sponsor, ticker_name):
    """Simple heuristic: if ticker name contains sponsor, likely correct."""
    sponsor_clean = sponsor.lower().replace(',', '').replace('.', '')
    ticker_clean = ticker_name.lower().replace(',', '').replace('.', '')

    # Check if main word from sponsor is in ticker name
    sponsor_words = [w for w in sponsor_clean.split() if len(w) > 3]
    ticker_words = set(ticker_clean.split())

    matches = sum(1 for w in sponsor_words if w in ticker_words)
    return matches >= 1

# Generate suggested labels
df = df.with_columns([
    pl.when(pl.col('confidence') >= 0.95)
      .then(pl.lit('likely_correct'))
      .when(pl.col('confidence') < 0.60)
      .then(pl.lit('likely_incorrect'))
      .otherwise(pl.lit('needs_review'))
      .alias('suggested_label')
])

# You review and confirm/correct
print(df.select(['sponsor_name', 'ticker', 'ticker_name', 'confidence', 'suggested_label']))
```

---

## Labeling Guidelines

### When to label "correct"

✅ **Exact name match**
- "Pfizer Inc" → PFE "Pfizer Inc."

✅ **Minor variations**
- "Pfizer" → PFE "Pfizer Inc."
- "Pfizer, Inc." → PFE "Pfizer Inc."

✅ **Subsidiary of parent company**
- "Genentech" → RHHBY "Roche Holding AG" (acquired 2009)
- "Instagram" → META "Meta Platforms Inc."

✅ **Common abbreviations**
- "3M" → MMM "3M Company"
- "J&J" → JNJ "Johnson & Johnson"

✅ **Acquired companies (use acquirer ticker)**
- "WhatsApp" → META (owned by Meta since 2014)

### When to label "incorrect"

❌ **Different companies entirely**
- "Pfizer Inc" → MSFT "Microsoft Corporation"

❌ **Similar names but unrelated**
- "Novartis Pharma" → PFE (should be NVS)

❌ **Wrong security type**
- "4D Pharma" → LBPWQ "4D PHARMA PLC 2026 WTS" (warrant, not stock)

❌ **Divisions/departments (if no clear ownership)**
- "Pfizer Global R&D" → MRNA (wrong company)

❌ **Competitors**
- "Moderna" → PFE (both pharma but different)

### When to label "unknown"

❓ **Complex ownership structures**
- Unclear if division vs parent vs subsidiary

❓ **Recently merged/acquired**
- Ownership structure still changing

❓ **Foreign subsidiaries**
- "3SBIO AU PTY LTD" → ? (Australian subsidiary, unclear)

❓ **Insufficient information**
- Can't find company information online
- Ticker no longer exists

---

## Quality Checks

Before committing new labels:

```bash
# 1. Validate schema
python -c "
from src.analytics.entity_resolution.evaluation import load_ground_truth
df = load_ground_truth('data/ground_truth/sponsor_ticker_labels.csv')
print(f'✓ Loaded {len(df)} valid labels')
print(f'  - Correct: {(df[\"label\"] == \"correct\").sum()}')
print(f'  - Incorrect: {(df[\"label\"] == \"incorrect\").sum()}')
print(f'  - Unknown: {(df[\"label\"] == \"unknown\").sum()}')
"

# 2. Check for duplicates
python -c "
import polars as pl
df = pl.read_csv('data/ground_truth/sponsor_ticker_labels.csv')
dupes = df.group_by(['sponsor_name', 'ticker']).count().filter(pl.col('count') > 1)
if len(dupes) > 0:
    print('⚠️  Found duplicates:')
    print(dupes)
else:
    print('✓ No duplicates found')
"

# 3. Balance check (aim for ~60-70% correct, ~30-40% incorrect)
python -c "
import polars as pl
df = pl.read_csv('data/ground_truth/sponsor_ticker_labels.csv')
correct_pct = (df['label'] == 'correct').sum() / len(df) * 100
print(f'Dataset balance: {correct_pct:.1f}% correct, {100-correct_pct:.1f}% incorrect/unknown')
if correct_pct < 50 or correct_pct > 80:
    print('⚠️  Consider adding more examples from the other category')
"
```

---

## Target Distribution

Aim for diverse examples:

| Category | Count | Notes |
|----------|-------|-------|
| High confidence (≥0.90) | 20-30 | Likely correct, good for precision |
| Medium confidence (0.70-0.90) | 15-25 | Uncertain, informative cases |
| Low confidence (0.60-0.70) | 5-10 | Likely incorrect, helps with recall |
| Edge cases | 10-15 | Subsidiaries, abbreviations, etc. |
| **Total** | **50-80** | Enough for reliable metrics |

---

## Let's Start!

Ready to label together? Here's the plan:

1. **I'll generate candidates** (run pipeline with LIMIT_SPONSORS=200)
2. **You review and label** 10-20 examples
3. **I'll add 10-20 examples** to complement yours
4. **We iterate** until we have 50+ diverse labels
5. **Re-run evaluation** to see real baseline metrics!

Sound good?
