# range_overright_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 2179 - 2190

## Overview
Checks if a range does not extend to the left of a multirange (PostgreSQL "&>" operator for range-multirange comparison).

## Definition


## Detailed Description
This function implements the "overright" or "does not extend to left of" operator (&>) between a range type and a multirange type. It determines whether the given range does not extend to the left of the given multirange by comparing their bounds. The function serves as a PostgreSQL function wrapper that extracts arguments, retrieves type information, and delegates the actual comparison logic to `range_overright_multirange_internal`.

The overright operator returns true if the range's lower bound is greater than or equal to the multirange's lower bound, meaning the range does not extend beyond the leftmost point of the multirange.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access:
  - Argument 0: `RangeType *r` - The range to compare
  - Argument 1: `MultirangeType *mr` - The multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - Extract range argument
  - `PG_GETARG_MULTIRANGE_P` - Extract multirange argument
  - [multirange_get_typcache](../m/multirange_get_typcache.md) - Get type cache information
  - `MultirangeTypeGetOid` - Get OID of multirange type
  - [range_overright_multirange_internal](range_overright_multirange_internal.md) - Perform actual comparison logic
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called using the &> operator in SQL
- Returns false if either the range or multirange is empty (handled by the internal function)
- The actual comparison logic is implemented in `range_overright_multirange_internal`
- Complementary to `range_overleft_multirange` - while overleft checks if a range doesn't extend to the right, overright checks if a range doesn't extend to the left
- Part of PostgreSQL's range and multirange type system for spatial and temporal data operations
- Located in `src/backend/utils/adt/multirangetypes.c:2179-2190`