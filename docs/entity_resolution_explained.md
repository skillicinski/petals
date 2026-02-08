# Entity Resolution Pipeline - Detailed Explanation

## How It Works: Step-by-Step

### **Step 1: Extract** (extract.py)

**What happens:**
- Fetch distinct sponsor names from `clinical.trials` table
- Fetch company info from `reference.ticker_details` table

**Example data:**
```
Left side (sponsors):
- "Pfizer Inc"
- "Moderna, Inc."
- "Genentech, Inc."

Right side (tickers):
- ticker: "PFE", name: "Pfizer Inc."
- ticker: "MRNA", name: "Moderna, Inc."
- ticker: "RHHBY", name: "Roche Holding AG"
```

**Code location:** `src/analytics/entity_resolution/extract.py:34-73`

---

### **Step 2: Blocking** (blocking.py)

**Problem:** Comparing every sponsor to every ticker is expensive
- 1,000 sponsors × 10,000 tickers = 10 million comparisons
- Would take hours and cost $$$ for embeddings

**Solution: Token-based pre-filtering**

**How it works:**
1. **Tokenize** both sponsor and ticker names
   - Split on non-alphanumeric characters
   - Lowercase everything
   - Filter out "common tokens" (inc, ltd, pharma, etc.)

2. **Build inverted index** for tickers
   ```python
   # Example inverted index:
   {
       "pfizer": ["PFE"],
       "moderna": ["MRNA"],
       "roche": ["RHHBY"],
       "genentech": []  # Not in index (will miss this!)
   }
   ```

3. **For each sponsor, find candidates with shared tokens**
   ```python
   Sponsor: "Pfizer Inc"
   → Tokens: {"pfizer"}  # "inc" filtered out
   → Candidates: ["PFE"]  # Shares "pfizer" token

   Sponsor: "Genentech, Inc."
   → Tokens: {"genentech"}
   → Candidates: []  # No ticker has "genentech" token!
   ```

**Result:** Reduced from 10M to ~100K comparisons (99% reduction)

**Trade-off:** May miss matches if no shared tokens
- Example: "Genentech" → "RHHBY" (owned by Roche Group)
- No shared tokens, so it's filtered out!

**Code location:** `src/analytics/entity_resolution/blocking.py:12-131`

**Configuration:**
- `MIN_TOKEN_LENGTH = 2` (ignore 1-char tokens)
- `COMMON_TOKENS` = {"inc", "corp", "pharma", "ltd", ...}

---

### **Step 3: Embedding Computation** (main.py:71-90)

**What are embeddings?**
- Vector representations that capture semantic meaning
- Similar meanings → similar vectors
- Uses neural network model: `all-MiniLM-L6-v2`

**Example:**
```python
"Pfizer Inc"           → [0.23, -0.45, 0.12, ..., 0.67]  # 384 dimensions
"Pfizer Incorporated"  → [0.24, -0.44, 0.13, ..., 0.68]  # Very similar!
"Apple Computer"       → [0.89, 0.12, -0.56, ..., -0.23] # Very different!
```

**Process:**
1. Collect unique sponsors from candidate pairs: `["Pfizer Inc", "Moderna Inc", ...]`
2. Collect unique tickers from candidate pairs: `["PFE", "MRNA", ...]`
3. Pass through sentence-transformer model
4. Get 384-dimensional vectors (L2-normalized)

**Why L2-normalized?**
- Makes cosine similarity = dot product
- Faster computation: `similarity = np.dot(v1, v2)`

**Model details:**
- Name: `all-MiniLM-L6-v2`
- Size: ~80MB
- Speed: ~14k sentences/sec on CPU
- Dimension: 384

**Code location:** `src/analytics/entity_resolution/main.py:54-90`

---

### **Step 4: Scoring** (main.py:197-282)

**How it works:**
For each candidate pair, compute cosine similarity between embeddings.

**Formula:**
```python
similarity = dot(sponsor_embedding, ticker_embedding)
# Result: 0.0 (completely different) to 1.0 (identical)
```

**Example scores:**
```
"Pfizer Inc" → "Pfizer Inc."     = 0.994  (almost identical)
"Moderna Inc" → "Moderna, Inc."  = 0.972  (very similar)
"3M" → "3M Company"              = 0.781  (somewhat similar)
"Pfizer Inc" → "Apple Inc."      = 0.134  (very different)
```

**Thresholds:**
- `≥ 0.85` → Auto-approve (high confidence)
- `0.65 - 0.85` → Pending review (uncertain)
- `< 0.65` → Auto-reject (low confidence)

**Why these thresholds?**
- Set empirically based on observation
- **Not scientifically calibrated** (Phase 1 will help with this!)
- Should be tuned with ground truth data

**Output:** DataFrame with all candidate pairs + confidence scores

**Code location:** `src/analytics/entity_resolution/main.py:197-282`

---

### **Step 5: 1:1 Matching** (main.py:156-194)

**Problem:** One sponsor might match multiple tickers (and vice versa)

**Example conflict:**
```
"Pfizer Inc" → "PFE"   (0.994)
"Pfizer Inc" → "PFE"   (0.850)  # Somehow appears twice
"Moderna Inc" → "PFE"  (0.200)  # Wrong match
```

**Current solution: Greedy assignment**
1. Sort all pairs by confidence (descending)
2. Iterate from highest to lowest
3. For each pair:
   - If sponsor not matched AND ticker not matched → assign
   - Otherwise → skip

**Example:**
```
Input pairs (sorted):
1. "Pfizer Inc" → "PFE" (0.994)   ✓ Assign
2. "Moderna Inc" → "MRNA" (0.972) ✓ Assign
3. "Pfizer Inc" → "PFE" (0.850)   ✗ Skip (both already matched)

Output: 2 unique pairs
```

**Potential limitations:**
- When we care about minimum match quality
- When matches have different importance weights
- When we want to maximize coverage, not total score

**Phase 2 will implement Hungarian algorithm** (optimal bipartite matching)

**Code location:** `src/analytics/entity_resolution/main.py:156-194`

---

### **Step 6: Output** (main.py:385-395 + load.py)

**What gets saved:**

1. **Local Parquet file** (for inspection)
   ```
   data/candidates.parquet
   ```

2. **Iceberg table** (if SAVE_ICEBERG=1)
   ```
   matching.sponsor_ticker_candidates
   ```
   Columns:
   - sponsor_name
   - ticker
   - ticker_name
   - market
   - confidence
   - status (approved/pending/rejected)
   - match_reason
   - run_id
   - created_at

**Example output:**
```csv
sponsor_name,ticker,confidence,status
"Pfizer Inc",PFE,0.994,approved
"Moderna Inc",MRNA,0.972,approved
"Genentech Inc",RHHBY,0.848,pending
"Unknown Sponsor",XXXX,0.234,rejected
```

---

## **Full Pipeline Visualization**

```
┌─────────────────────┐
│  1. EXTRACT         │  Fetch sponsors + tickers from database
│  50 sponsors        │
│  100 tickers        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  2. BLOCKING        │  Token-based pre-filter
│  5,000 naive pairs  │  → Filter to candidates with shared tokens
│  → 108 candidates   │  → 97.8% reduction!
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  3. EMBEDDINGS      │  Compute semantic vectors
│  16 unique sponsors │  → all-MiniLM-L6-v2 model
│  35 unique tickers  │  → 384-dimensional vectors
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  4. SCORING         │  Cosine similarity
│  108 candidate pairs│  → confidence scores (0.0 - 1.0)
│  Score each pair    │  → classify: approved/pending/rejected
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  5. 1:1 MATCHING    │  Greedy assignment
│  108 scored pairs   │  → Select best match per sponsor
│  → 6 unique pairs   │  → No duplicates
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  6. OUTPUT          │  Save results
│  6 unique matches   │  → Parquet + Iceberg
│  (3 approved, 3 pending) │
└─────────────────────┘
```

---

## **Key Files**

| File | Purpose | Lines |
|------|---------|-------|
| `main.py` | Orchestrates entire pipeline | 438 |
| `extract.py` | Fetch data from Iceberg tables | 74 |
| `blocking.py` | Token-based candidate generation | 147 |
| `config.py` | Thresholds, constants, table names | 66 |
| `load.py` | Save results to storage | 192 |
| `evaluation.py` | **NEW** - Measure quality | 450 |

---

## **Current Limitations**

1. **Blocking can miss matches**
   - If no shared tokens → filtered out
   - Example: "Genentech" won't match "Roche"

2. **Greedy matching may be suboptimal**
   - Doesn't guarantee maximum total score
   - Doesn't consider global constraints

3. **Thresholds not calibrated**
   - 0.85/0.65 cutoffs chosen arbitrarily
   - Should be tuned with ground truth data

4. **No learning from feedback**
   - Human corrections aren't used to improve
   - Each run is independent

5. **Single embedding model**
   - all-MiniLM-L6-v2 is fast but relatively simple
   - Larger models might be more accurate

6. **Security type confusion**
   - Matches to warrants, not stocks (saw this in FP)
   - Embeddings don't distinguish security types

---

## **Strengths**

1. **Semantic understanding**
   - Handles typos, abbreviations, variations
   - "3M" correctly matches "3M Company"

2. **Efficient**
   - Blocking reduces 10M → 100K comparisons
   - Runs in ~20 seconds for full dataset

3. **Production-ready**
   - CloudWatch logging
   - DynamoDB state tracking
   - Iceberg table output

4. **Human-in-the-loop**
   - Three-tier classification
   - Pending cases for review

---

## **What Phase 2 Will Improve**

- Replace greedy with **Hungarian algorithm** (optimal matching)
- Measure improvement with our evaluation framework
- Learn about bipartite matching and graph algorithms
