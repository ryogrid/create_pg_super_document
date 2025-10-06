# multirange_overlaps_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:1960-1975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1960-L1975)

## Overview
SQL operator function that checks if two multiranges overlap with each other, returning true if they have any common values.

## Definition
```c
Datum multirange_overlaps_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL operator `&&` (overlaps) for multirange types. It serves as a PostgreSQL function interface that extracts two multirange arguments from the function call context, retrieves the appropriate type cache information, and delegates the actual overlap checking logic to the internal function `multirange_overlaps_multirange_internal`.

The function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS` and returns a boolean result using `PG_RETURN_BOOL`.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro to access arguments:

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange from function arguments
  - [multirange_get_typcache](multirange_get_typcache.md) - Get type cache entry for multirange operations
  - `MultirangeTypeGetOid` - Get the OID of the multirange type
  - [multirange_overlaps_multirange_internal](multirange_overlaps_multirange_internal.md) - Internal overlap checking implementation
  - `PG_RETURN_BOOL` - Return boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using the `&&` operator on multirange types
  - Indirectly called through PostgreSQL's function manager system

## Notes and Other Information
- This is a wrapper function that provides the PostgreSQL function interface for multirange overlap operations
- The actual overlap logic is implemented in `multirange_overlaps_multirange_internal`
- Located in `src/backend/utils/adt/multirangetypes.c` at lines 1960-1975
- Part of PostgreSQL's multirange type system introduced for handling collections of ranges

## Simplified Source

```c
Datum multirange_overlaps_multirange(PG_FUNCTION_ARGS)
{
    // Extract two multirange arguments
    MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);

    // Get type cache for range operations
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    // Delegate to internal overlap function and return result
    PG_RETURN_BOOL(multirange_overlaps_multirange_internal(typcache->rngtype, mr1, mr2));
}
```