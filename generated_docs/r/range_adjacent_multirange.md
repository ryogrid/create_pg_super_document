# range_adjacent_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2507-2518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2507-L2518)

## Overview
Determines if a range is adjacent to a multirange by checking if the range is adjacent to any part of the multirange.

## Definition

```c
Datum
range_adjacent_multirange(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL adjacency operator () for checking if a range is adjacent to a multirange. A range is considered adjacent to a multirange if it shares a boundary point with any range in the multirange without overlapping. The function serves as a PostgreSQL function wrapper that extracts the arguments, retrieves the appropriate type cache, and delegates the actual logic to .

The adjacency check works by:
1. Extracting the range and multirange arguments from the function call
2. Getting the type cache for the multirange's element type
3. Calling the internal implementation to perform the actual adjacency test
4. The internal function checks if the range is adjacent to either the first or last range in the multirange

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0:  - the range to test for adjacency
  - Argument 1:  - the multirange to test against

## Dependencies
- Functions called/Symbols referenced:
  -  - extract range argument
  -  - extract multirange argument  
  -  - get type cache for multirange operations
  -  - get OID of multirange type
  -  - perform actual adjacency check
- Called from (representative examples):
  - SQL queries using the  operator between ranges and multiranges
  - PostgreSQL operator system via function catalog entries

## Notes and Other Information
- Returns a boolean result wrapped as a Datum using 
- Part of PostgreSQL's range and multirange type system introduced for advanced range operations
- The actual adjacency logic is implemented in the internal function which checks bounds of the first and potentially last ranges in the multirange
- Empty ranges or multiranges are never considered adjacent to anything
- File location: 