# int8range_canonical

## Location
src/backend/utils/adt/rangetypes.c: 1511 - 1557

## Overview
Converts an int8range (64-bit integer range) to its canonical form by normalizing bounds to use consistent inclusivity/exclusivity conventions.

## Definition


## Detailed Description
This function standardizes int8range values to a canonical representation where lower bounds are inclusive and upper bounds are exclusive. Similar to int4range_canonical but operating on 64-bit integers, this function converts exclusive lower bounds to inclusive by incrementing the value, and converts inclusive upper bounds to exclusive by incrementing the value. It includes overflow checking to prevent integer overflow when incrementing boundary values near PG_INT64_MAX.

The canonical form ensures that ranges like [1,5) and (0,5) are represented consistently, which is essential for range operations like equality comparisons, indexing, and hash operations on bigint ranges.

## Parameters / Member Variables
- : The input int8range value to be canonicalized (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - [DatumGetInt64](../D/DatumGetInt64.md)
  - [Int64GetDatum](../I/Int64GetDatum.md)
  - [range_serialize](../r/range_serialize.md)
  - PG_RETURN_RANGE_P
  - ereturn (for error handling)
- Called from (representative examples):
  - No direct references found (likely called via function catalog for range operations)

## Notes and Other Information
- Part of the canonical functions for built-in range types, specifically for bigint ranges
- Handles integer overflow by checking for PG_INT64_MAX before incrementing
- Empty ranges are returned unchanged
- The canonical form uses inclusive lower bounds and exclusive upper bounds
- Error context is preserved for proper error reporting in nested function calls
- This canonicalization is essential for consistent range comparisons and hash operations on 64-bit integer ranges
- Nearly identical to int4range_canonical but operates on 64-bit integers instead of 32-bit