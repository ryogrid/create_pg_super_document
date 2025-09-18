# calc_rank

## Location
src/backend/utils/adt/tsrank.c: 357 - 399

## Overview
Main ranking function that determines the appropriate ranking algorithm (AND or OR) and applies various normalization methods to compute the final text search relevance score.

## Definition
```c
static float calc_rank(const float *w, TSVector t, TSQuery q, int32 method)
```

## Detailed Description
This function serves as the central dispatcher for PostgreSQL's text search ranking system. It analyzes the query structure to determine whether to use AND-based or OR-based ranking algorithms, then applies various normalization methods based on the specified method flags.

The function first checks if the query contains AND or PHRASE operators at the root level, directing it to use calc_rank_and for proximity-based scoring. Otherwise, it uses calc_rank_or for independent term scoring. After obtaining the base score, it applies multiple normalization techniques:

- RANK_NORM_LOGLENGTH: Normalizes by logarithmic document length
- RANK_NORM_LENGTH: Normalizes by actual document length  
- RANK_NORM_UNIQ: Normalizes by number of unique terms
- RANK_NORM_LOGUNIQ: Normalizes by logarithmic unique term count
- RANK_NORM_RDIVRPLUS1: Applies res/(res+1) normalization

The function ensures a minimum positive score (1e-20) to avoid negative or zero results.

## Parameters / Member Variables
- `w`: Array of floating-point weights for different term positions/frequencies
- `t`: TSVector containing the document's lexemes and their positions
- `q`: TSQuery containing the search query terms and operators
- `method`: Bitmask specifying which normalization methods to apply

## Dependencies
- Functions called/Symbols referenced:
  - GETQUERY
  - calc_rank_and
  - calc_rank_or
  - cnt_length
  - QI_OPR
  - OP_AND
  - OP_PHRASE
  - RANK_NORM_* constants
- Called from (representative examples):
  - ts_rank_wttf
  - ts_rank_wtt
  - ts_rank_ttf
  - ts_rank_tt

## Notes and Other Information
- Returns 0.0 immediately if either TSVector or TSQuery is empty
- Does not handle NOT operators (marked as TODO with XXX comment)
- RANK_NORM_EXTDIST normalization method is explicitly noted as not applicable
- Ensures minimum score of 1e-20 to prevent mathematical issues with zero/negative scores
- Multiple normalization methods can be combined using bitwise OR in the method parameter