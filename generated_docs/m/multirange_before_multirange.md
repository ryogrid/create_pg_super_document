# multirange_before_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2352-2364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2352-L2364)

## Overview
Determines if one multirange is strictly positioned before (to the left of) another multirange, meaning the first multirange's rightmost upper bound is less than the second multirange's leftmost lower bound.

## Definition

```c
Datum
multirange_before_multirange(PG_FUNCTION_ARGS)
```
## Detailed Description
This PostgreSQL function implements the "strictly left of" operator (<<) between two multiranges. It checks whether the first multirange is entirely positioned before the second multirange with no overlap or adjacency. The function serves as a SQL-callable wrapper around the internal  function.

The implementation compares the upper bound of the rightmost range in the first multirange against the lower bound of the leftmost range in the second multirange. If the first multirange's rightmost point is less than the second multirange's leftmost point, then the first multirange is strictly before the second one. Empty multiranges are handled specially, returning false as they are not considered to have strict positional relationships.

## Parameters / Member Variables
- : PostgreSQL function call information containing the two multirange arguments
  - Argument 0:  - The first multirange to compare
  - Argument 1:  - The second multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract multirange arguments from function call
  -  - Get type cache entry for multirange operations
  -  - Get OID of multirange type
  -  - Internal implementation of the comparison logic
- Called from (representative examples):
  - SQL queries using the << operator between two multirange types

## Notes and Other Information
- This function implements the << (strictly left of) operator for multirange-multirange comparisons
- Returns false for empty multiranges, following PostgreSQL's convention for spatial operations
- The internal logic compares the rightmost upper bound of the first multirange with the leftmost lower bound of the second multirange
- Uses  to extract bounds from specific range positions within each multirange
- Part of PostgreSQL's comprehensive multirange type system that supports spatial relationship operations
- Enables efficient spatial queries and indexing operations on collections of ranges