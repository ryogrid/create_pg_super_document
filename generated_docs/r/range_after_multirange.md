# range_after_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2365-2376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2365-L2376)

## Overview
Determines if a range is strictly positioned after (to the right of) a multirange, meaning the range's lower bound is greater than the multirange's upper bound.

## Definition

```c
Datum
range_after_multirange(PG_FUNCTION_ARGS)
```
## Detailed Description
This PostgreSQL function implements the "strictly right of" operator (>>) between a range and a multirange. It checks whether the given range is entirely positioned after the multirange with no overlap or adjacency. The function serves as a SQL-callable wrapper around the internal  function, handling PostgreSQL's function calling convention and type management.

The function extracts the range and multirange arguments from the function call info, retrieves the appropriate type cache entry for the multirange's base type, and delegates the actual comparison logic to the internal implementation. The internal logic compares the range's lower bound against the multirange's rightmost (last) range's upper bound to determine if the range is strictly positioned after the entire multirange.

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
  - SQL queries using the >> operator between range and multirange types
  - Used internally by  function (symmetric relationship)

## Notes and Other Information
- This function implements the >> (strictly right of) operator for range-multirange comparisons
- Returns false for empty ranges or multiranges, following PostgreSQL's convention for spatial operations
- The internal implementation compares the range's lower bound with the multirange's rightmost range's upper bound
- Used symmetrically by the  function, demonstrating efficient code reuse
- Part of PostgreSQL's multirange type system that provides comprehensive spatial relationship operators
- Enables spatial indexing and query optimization for range-based data

## Simplified Source

```c
Datum
range_after_multirange(PG_FUNCTION_ARGS)
{
    // Extract arguments: range and multirange
    RangeType *range = PG_GETARG_RANGE_P(0);
    MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(1);

    // Get type information for the multirange
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(multirange));

    // Delegate to internal function for actual comparison logic
    PG_RETURN_BOOL(range_after_multirange_internal(typcache->rngtype, range, multirange));
}
```