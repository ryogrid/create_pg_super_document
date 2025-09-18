# tsquery_requires_match

## Location
[src/backend/utils/adt/tsvector_op.c:2156-2205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2156-L2205)

## Overview
Detects whether a tsquery boolean expression requires any positive matches to values shown in the tsquery, used to optimize GIN index searches.

## Definition


## Detailed Description
This function analyzes a text search query tree to determine if the query requires at least one positive match to terms present in the query. This information is crucial for GIN index optimization, as it determines whether a full index scan is necessary or if the search can be limited to specific index entries.

The function recursively traverses the query tree and applies boolean logic rules: for AND operations (including PHRASE), only one side needs to require a match; for OR operations, both sides must require a match; NOT operations are assumed to not require matches to avoid complex nested analysis.

## Parameters / Member Variables
- : Pointer to the current QueryItem being analyzed in the query tree

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [tsquery_requires_match](tsquery_requires_match.md) (recursive calls)
- Called from (representative examples):
  - [gin_extract_tsquery](../g/gin_extract_tsquery.md)
  - [tsquery_requires_match](tsquery_requires_match.md) (recursive calls)

## Notes and Other Information
- Includes stack overflow protection for deep recursion
- Treats OP_PHRASE identically to OP_AND for analysis purposes
- Conservative approach: assumes NOT operations don't require matches, even though some nested NOT cases might
- Critical for GIN index performance: helps determine if full index scan is needed
- Example: 'x & \!y' requires match of x (can scan x entries), but 'x | \!y' might match rows with neither x nor y (requires full scan)