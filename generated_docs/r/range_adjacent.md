# range_adjacent

## Location
[src/backend/utils/adt/rangetypes.c:828-840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L828-L840)

## Overview
PostgreSQL SQL function that determines if two ranges are adjacent (touching but not overlapping).

## Definition

```c
Datum
range_adjacent(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the SQL-callable wrapper for the range adjacency operator "-|-". It extracts two range arguments from the PostgreSQL function call context, validates they are of the same type, and delegates the actual adjacency testing to .

Two ranges are considered adjacent if they touch at exactly one boundary point without overlapping or having a gap between them. This is useful for range operations where you need to detect ranges that can be merged or are positioned next to each other.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  -  (argument 0): First RangeType to test
  -  (argument 1): Second RangeType to test

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](range_get_typcache.md)
  - RangeTypeGetOid
  - [range_adjacent_internal](range_adjacent_internal.md)
- Called from (representative examples):
  - No direct callers found (SQL operator function)

## Notes and Other Information
- This function implements the PostgreSQL "-|-" (adjacent) range operator
- Returns a boolean datum indicating whether the ranges are adjacent
- The function is registered in the PostgreSQL system catalogs as the implementation for range adjacency operations
- Relies on range_adjacent_internal for the core adjacency logic
- Located in src/backend/utils/adt/rangetypes.c:828-840

## Simplified Source

```c
Datum range_adjacent(PG_FUNCTION_ARGS) {
    RangeType *r1 = PG_GETARG_RANGE_P(0);
    RangeType *r2 = PG_GETARG_RANGE_P(1);
    TypeCacheEntry *typcache;

    // Get type cache for range operations
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    // Delegate to internal function and return result
    PG_RETURN_BOOL(range_adjacent_internal(typcache, r1, r2));
}
```