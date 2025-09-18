# calc_rank_or

## Location
src/backend/utils/adt/tsrank.c: 283 - 356

## Overview
Calculates text search ranking for OR operations between query terms, computing relevance scores based on individual term weights and occurrence positions.

## Definition
```c
static float calc_rank_or(const float *w, TSVector t, TSQuery q)
```

## Detailed Description
This function implements the OR-based ranking algorithm for PostgreSQL's text search functionality. It calculates a relevance score by analyzing each query term independently and combining their individual contributions. The algorithm uses a mathematical approach based on the convergent series sum(1/i^2) = π²/6 ≈ 1.64493406685.

For each matching term, the function computes a weighted score considering both the term's weight and its positional information. The scoring formula applies decreasing weights to multiple occurrences of the same term: resj = sum(wi/i^2) where wi represents the weight of the i-th occurrence. The final result is normalized by dividing by the number of unique query terms.

The algorithm includes a noted optimization where instead of sorting weights in descending order (as mathematically ideal), it simply uses the maximum weight for efficiency, as indicated by the TODO comment from Oleg Bartunov.

## Parameters / Member Variables
- `w`: Array of floating-point weights for different term positions/frequencies
- `t`: TSVector containing the document's lexemes and their positions
- `q`: TSQuery containing the search query terms and operators

## Dependencies
- Functions called/Symbols referenced:
  - SortAndUniqItems
  - find_wordentry
  - POSDATALEN
  - POSDATAPTR
  - wpos
- Called from (representative examples):
  - calc_rank
  - calc_rank_and
  - DEF_NORM_METHOD

## Notes and Other Information
- Uses the mathematical constant π²/6 ≈ 1.64493406685 for score normalization
- Contains a known optimization where maximum weight is used instead of proper sorting
- Handles both positional and non-positional text search vectors using dummy positions when needed
- Final score is averaged across all unique query terms
- More efficient than AND-based ranking as it doesn't require proximity calculations