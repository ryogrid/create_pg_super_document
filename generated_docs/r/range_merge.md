# range_merge

## Location
src/backend/utils/adt/rangetypes.c: 1114 - 1126

## Overview
The range_merge function computes a range that spans from the minimum bound to the maximum bound of two input ranges, allowing non-adjacent ranges and potentially creating gaps.

## Definition
Datum range_merge(PG_FUNCTION_ARGS)

## Detailed Description
The range_merge function implements a relaxed version of range union that does not require input ranges to be adjacent or overlapping. Unlike range_union, this function will merge any two ranges by creating a new range that spans from the lowest lower bound to the highest upper bound, potentially including gaps between the original ranges. This is useful for operations that need to find the overall span covered by multiple ranges rather than their strict mathematical union.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro to access arguments:
  - r1: First range argument obtained via PG_GETARG_RANGE_P(0)
  - r2: Second range argument obtained via PG_GETARG_RANGE_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - range_get_typcache
  - RangeTypeGetOid
  - range_union_internal
  - PG_RETURN_RANGE_P
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Uses range_union_internal with strict=false, allowing non-adjacent ranges
- Will create a range that may include values not present in either input range
- Useful for finding the overall span or envelope of multiple ranges
- Does not throw errors for non-contiguous input ranges unlike range_union
- Part of PostgreSQL's extended range functionality for span operations
- Commonly used when you need the bounding range rather than strict set union
- The result always represents the minimal range that contains both input ranges