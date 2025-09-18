# int4range_canonical

## Location
src/backend/utils/adt/rangetypes.c: 1464 - 1510

## Overview
Converts an int4range (32-bit integer range) to its canonical form by normalizing bounds to use consistent inclusivity/exclusivity conventions.

## Definition


## Detailed Description
This function standardizes int4range values to a canonical representation where lower bounds are inclusive and upper bounds are exclusive. For discrete types like integers, this canonical form provides a unique representation for equivalent ranges. The function converts exclusive lower bounds to inclusive by incrementing the value, and converts inclusive upper bounds to exclusive by incrementing the value. It includes overflow checking to prevent integer overflow when incrementing boundary values.

The canonical form ensures that ranges like [1,5) and (0,5) are represented consistently, which is important for range operations like equality comparisons and indexing.

## Parameters / Member Variables
- : The input int4range value to be canonicalized (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - range_get_typcache
  - RangeTypeGetOid
  - range_deserialize
  - DatumGetInt32
  - Int32GetDatum
  - range_serialize
  - PG_RETURN_RANGE_P
  - ereturn (for error handling)
- Called from (representative examples):
  - No direct references found (likely called via function catalog for range operations)

## Notes and Other Information
- Part of the canonical functions for built-in range types
- Handles integer overflow by checking for PG_INT32_MAX before incrementing
- Empty ranges are returned unchanged
- The canonical form uses inclusive lower bounds and exclusive upper bounds
- Error context is preserved for proper error reporting in nested function calls
- This canonicalization is essential for consistent range comparisons and hash operations