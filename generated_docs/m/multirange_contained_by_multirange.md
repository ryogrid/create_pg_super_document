# multirange_contained_by_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2251-2265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2251-L2265)

## Overview
Tests whether one multirange is completely contained by another multirange by reversing the argument order and calling the internal containment function.

## Definition
```c
Datum multirange_contained_by_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
The `multirange_contained_by_multirange` function implements the "contained by" operator (<@) for checking containment relationships between two multiranges. It serves as a PostgreSQL function wrapper that determines if the first multirange is completely contained within the second multirange.

The function cleverly implements the "contained by" logic by reversing the argument order and calling `multirange_contains_multirange_internal(typcache->rngtype, mr2, mr1)`, effectively asking "does mr2 contain mr1?" to answer "is mr1 contained by mr2?".

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: `MultirangeType *mr1` - The multirange to test for being contained
  - Argument 1: `MultirangeType *mr2` - The potentially containing multirange

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange from function arguments
  - [multirange_get_typcache](multirange_get_typcache.md) - Get type cache for range type
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - [multirange_contains_multirange_internal](multirange_contains_multirange_internal.md) - Internal containment logic (with reversed arguments)
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This is the logical inverse of `multirange_contains_multirange`
- Implements the operation by swapping arguments to the internal containment function
- This function supports the <@ operator in SQL queries between multirange types
- Located in src/backend/utils/adt/multirangetypes.c:2251-2265
- Includes a comment indicating it implements the "contained by?" operation

## Simplified Source

```c
Datum multirange_contained_by_multirange(PG_FUNCTION_ARGS) {
    // Extract both multirange arguments
    MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);

    // Get type cache for multirange
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    // Check if mr1 is contained by mr2 by asking if mr2 contains mr1
    PG_RETURN_BOOL(multirange_contains_multirange_internal(typcache->rngtype, mr2, mr1));
}
```