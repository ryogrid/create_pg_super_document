# gin_tsquery_consistent

## Location
[src/backend/utils/adt/tsginidx.c:214-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L214-L262)

## Overview
The gin_tsquery_consistent function implements the consistent function for GIN (Generalized Inverted Index) text search, determining whether a given TSQuery can be satisfied by the indexed terms.

## Definition
Datum gin_tsquery_consistent(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the consistent callback for GIN indexes on tsvector columns. It evaluates whether a TSQuery can be satisfied given the presence/absence information of individual query terms in the index. The function processes the query using ternary logic (YES/NO/MAYBE) and determines if a recheck is needed at the heap level.

The function extracts a check array indicating the presence of query operands, executes the TSQuery using ternary evaluation, and returns both a boolean result and a recheck flag. When the result is uncertain (TS_MAYBE), it sets the recheck flag to true, indicating that the tuple must be examined at the heap level for final verification.

## Parameters / Member Variables
- : Pointer to boolean array indicating presence/absence of each query operand in the index
- : Strategy number (unused in this implementation, commented out)  
- : The TSQuery object containing the search query
- : Number of keys (unused, commented out)
- : Pointer array containing additional data, specifically the operand mapping
- : Output parameter indicating whether heap-level recheck is required

## Dependencies
- Functions called/Symbols referenced:
  - GETQUERY (macro to extract query items from TSQuery)
  - [TS_execute_ternary](../T/TS_execute_ternary.md) (executes TSQuery with ternary logic)
  - [checkcondition_gin](../c/checkcondition_gin.md) (callback function for term evaluation)
- Called from (representative examples):
  - [gin_tsquery_consistent_6args](gin_tsquery_consistent_6args.md)
  - [gin_tsquery_consistent_oldsig](gin_tsquery_consistent_oldsig.md)

## Notes and Other Information
- Initially assumes no recheck is required (*recheck = false)
- Uses GinChkVal structure to pass context to the checkcondition_gin callback
- Handles empty queries (query->size == 0) by returning false
- The extra_data[0] contains the mapping from query items to operands
- Part of PostgreSQL's full-text search GIN index support infrastructure
- Located in src/backend/utils/adt/tsginidx.c:214-262