# gin_tsquery_triconsistent

## Location
src/backend/utils/adt/tsginidx.c: 263 - 303

## Overview
The gin_tsquery_triconsistent function implements the triconsistent function for GIN text search indexes, providing ternary logic evaluation (TRUE/FALSE/MAYBE) for TSQuery matching without requiring heap-level rechecks.

## Definition
Datum gin_tsquery_triconsistent(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the triconsistent callback for GIN indexes on tsvector columns, introduced as an optimization over the traditional consistent function. Unlike gin_tsquery_consistent, this function directly returns ternary values (GIN_TRUE, GIN_FALSE, GIN_MAYBE) without needing a separate recheck parameter.

The function evaluates a TSQuery against the available index information using ternary logic. It processes the query terms through TS_execute_ternary, which can definitively determine matches, non-matches, or uncertain cases. This allows the GIN index to make more informed decisions about which tuples need heap-level verification.

## Parameters / Member Variables  
- : Pointer to GinTernaryValue array indicating the ternary state of each query operand
- : Strategy number (unused in this implementation, commented out)
- : The TSQuery object containing the search query
- : Number of keys (unused, commented out)
- : Pointer array containing additional data, specifically the operand mapping

## Dependencies
- Functions called/Symbols referenced:
  - GETQUERY (macro to extract query items from TSQuery)
  - TS_execute_ternary (executes TSQuery with ternary logic)
  - checkcondition_gin (callback function for term evaluation)
  - PG_RETURN_GIN_TERNARY_VALUE (macro to return ternary result)
- Called from (representative examples):
  - No direct callers found (called by GIN index infrastructure)

## Notes and Other Information
- Returns GIN_FALSE for empty queries (query->size == 0)
- More efficient than gin_tsquery_consistent as it eliminates unnecessary heap checks
- Uses the same GinChkVal structure and checkcondition_gin callback as the consistent function
- Part of PostgreSQL's optimized GIN index support for full-text search
- The extra_data[0] contains the mapping from query items to operands
- Located in src/backend/utils/adt/tsginidx.c:263-303