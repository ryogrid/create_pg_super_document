# multirange_contained_by_range

## Location
src/backend/utils/adt/multirangetypes.c: 1758 - 1773

## Overview
A PostgreSQL function that checks if a multirange is completely contained within a single range.

## Definition
```c
Datum multirange_contained_by_range(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "contained by" operation for multiranges and ranges (multirange <@ range). It takes a multirange and a range as input and returns a boolean indicating whether the entire multirange is contained within the given range. The function serves as a wrapper that retrieves the necessary type cache information and delegates the actual containment check to `range_contains_multirange_internal`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `MultirangeType *mr` - The multirange to check for containment
  - Argument 1: `RangeType *r` - The range that may contain the multirange

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange from function arguments
  - `PG_GETARG_RANGE_P` - Extract range from function arguments  
  - [multirange_get_typcache](multirange_get_typcache.md) - Get type cache entry for the multirange type
  - `MultirangeTypeGetOid` - Get the OID of the multirange type
  - [range_contains_multirange_internal](../r/range_contains_multirange_internal.md) - Perform the actual containment check
- Called from (representative examples):
  - No direct references found (likely called via SQL operator framework)

## Notes and Other Information
- This function is part of PostgreSQL's multirange type system introduced to handle collections of ranges
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Returns the result via PG_RETURN_BOOL macro
- The actual containment logic is delegated to `range_contains_multirange_internal` for code reuse