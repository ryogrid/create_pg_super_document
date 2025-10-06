# range_before_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2328-2339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2328-L2339)

## Overview
Determines if a range is strictly positioned before (to the left of) a multirange, meaning the range's upper bound is less than the multirange's lower bound.

## Definition

```c
Datum
range_before_multirange(PG_FUNCTION_ARGS)
```
## Detailed Description
This PostgreSQL function implements the "strictly left of" operator (<<) between a range and a multirange. It checks whether the given range is entirely positioned before the multirange with no overlap or adjacency. The function serves as a SQL-callable wrapper around the internal  function, handling PostgreSQL's function calling convention and type management.

The function extracts the range and multirange arguments from the function call info, retrieves the appropriate type cache entry for the multirange's base type, and delegates the actual comparison logic to the internal implementation. It returns false if either operand is empty, as empty ranges/multiranges are not considered to be strictly positioned relative to anything.

## Parameters / Member Variables
- : PostgreSQL function call information containing the range and multirange arguments
  - Argument 0:  - The range to compare
  - Argument 1:  - The multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract range argument from function call
  -  - Extract multirange argument from function call
  -  - Get type cache entry for multirange operations
  -  - Get OID of multirange type
  -  - Internal implementation of the comparison logic
- Called from (representative examples):
  - SQL queries using the << operator between range and multirange types

## Notes and Other Information
- This function implements the << (strictly left of) operator for range-multirange comparisons
- Returns false for empty ranges or multiranges, following PostgreSQL's convention for spatial operations
- The actual comparison logic is delegated to  which compares bounds using 
- Part of PostgreSQL's multirange type system introduced to handle collections of non-overlapping ranges

## Simplified Source

```c
Datum
range_before_multirange(PG_FUNCTION_ARGS)
{
    // Extract arguments: range and multirange
    RangeType *range = PG_GETARG_RANGE_P(0);
    MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(1);

    // Get type information for the multirange
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(multirange));

    // Delegate to internal function for actual comparison logic
    PG_RETURN_BOOL(range_before_multirange_internal(typcache->rngtype, range, multirange));
}
```