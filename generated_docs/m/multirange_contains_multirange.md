# multirange_contains_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 2238 - 2250

## Overview
Tests whether one multirange completely contains another multirange by delegating to the internal containment checking function.

## Definition
```c
Datum multirange_contains_multirange(PG_FUNCTION_ARGS)
```

## Detailed Description
The `multirange_contains_multirange` function implements the "contains" operator (@>) for checking containment relationships between two multiranges. It serves as a PostgreSQL function wrapper that extracts the function arguments and delegates the actual containment logic to `multirange_contains_multirange_internal`.

This function is part of PostgreSQL's multirange type system and enables SQL queries using the @> operator to test if one multirange completely contains all ranges within another multirange.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: `MultirangeType *mr1` - The potentially containing multirange
  - Argument 1: `MultirangeType *mr2` - The multirange to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange from function arguments
  - `[multirange_get_typcache](multirange_get_typcache.md)` - Get type cache for range type
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - `[multirange_contains_multirange_internal](multirange_contains_multirange_internal.md)` - Internal containment logic
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This is a thin wrapper around the internal containment function
- The actual containment logic is implemented in `multirange_contains_multirange_internal`
- This function supports the @> operator in SQL queries between multirange types
- Located in src/backend/utils/adt/multirangetypes.c:2238-2250
- Includes a comment indicating it implements the "contains?" operation