# multirange_adjacent_range

## Location
[src/backend/utils/adt/multirangetypes.c:2519-2533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2519-L2533)

## Overview
Determines if a multirange is adjacent to a range by checking if any part of the multirange is adjacent to the range.

## Definition

```c
Datum
multirange_adjacent_range(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL adjacency operator () for checking if a multirange is adjacent to a range. A multirange is considered adjacent to a range if any range within the multirange shares a boundary point with the range without overlapping. The function serves as a PostgreSQL function wrapper that handles argument extraction, performs early empty checks, retrieves the appropriate type cache, and delegates the actual logic to .

The adjacency check works by:
1. Extracting the multirange and range arguments from the function call
2. Performing an early return if either operand is empty (empty ranges/multiranges are never adjacent)
3. Getting the type cache for the multirange's element type
4. Calling the same internal implementation used by  (arguments are symmetric for adjacency)

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0:  - the multirange to test for adjacency
  - Argument 1:  - the range to test against

## Dependencies
- Functions called/Symbols referenced:
  -  - extract multirange argument
  -  - extract range argument
  -  - check if range is empty
  -  - check if multirange is empty
  -  - get type cache for multirange operations
  -  - get OID of multirange type
  -  - perform actual adjacency check
- Called from (representative examples):
  - SQL queries using the  operator between multiranges and ranges
  - PostgreSQL operator system via function catalog entries

## Notes and Other Information
- Returns a boolean result wrapped as a Datum using 
- Includes an early empty check optimization that immediately returns false for empty operands
- Reuses the same internal implementation as  since adjacency is symmetric
- Part of PostgreSQL's range and multirange type system for advanced range operations
- The function arguments are swapped when calling the internal function to maintain consistent parameter order
- File location: 