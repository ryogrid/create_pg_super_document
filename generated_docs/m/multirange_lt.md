# multirange_lt

## Location
[src/backend/utils/adt/multirangetypes.c:2640-2647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2640-L2647)

## Overview
Implements the less-than operator () for multirange types by using the multirange comparison function.

## Definition

```c
Datum
multirange_lt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL less-than operator () for multirange types. It serves as a simple wrapper around the  function, returning true if the first multirange is lexicographically less than the second multirange. 

The function works by:
1. Calling  with the same function arguments to get a comparison result (-1, 0, or 1)
2. Returning true if the comparison result is negative (indicating the first multirange is less than the second)
3. Returning false otherwise

This operator enables multirange types to be used in SQL queries with less-than comparisons and supports sorting operations where multiranges need to be ordered.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0:  - the first multirange for comparison
  - Argument 1:  - the second multirange for comparison

## Dependencies
- Functions called/Symbols referenced:
  -  - performs the actual comparison logic
- Called from (representative examples):
  - SQL queries using the  operator between multirange values
  - PostgreSQL operator system via function catalog entries
  - Sorting operations that need to order multirange values

## Notes and Other Information
- Returns a boolean result wrapped as a Datum using 
- Very simple implementation that delegates all comparison logic to 
- Part of the family of inequality operators (lt, le, ge, gt) that all use 
- Essential for enabling full comparison semantics for multirange types in SQL
- Part of PostgreSQL's range and multirange type system for advanced range operations
- File location: 