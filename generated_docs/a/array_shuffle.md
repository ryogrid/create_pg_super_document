# array_shuffle

## Location
src/backend/utils/adt/array_userfuncs.c: 1626 - 1659

## Overview
A PostgreSQL user function that returns an array with the same dimensions as input, but with first-dimension elements randomly shuffled.

## Definition


## Detailed Description
 is a PostgreSQL function that randomly reorders the elements along the first dimension of a multi-dimensional array while preserving all other dimensions and the overall structure. It provides a complete shuffle of all items in the first dimension, unlike  which selects only a subset.

The function serves as a wrapper around the  helper function, passing the full size of the first dimension to ensure all elements are included in the shuffle. It maintains type cache information across multiple calls for performance optimization, storing TypeCacheEntry in the function's local context.

For optimization, the function performs early exit checks: arrays with fewer than 2 items in the first dimension are returned unchanged since shuffling would have no effect. Empty arrays are also returned as-is.

## Parameters / Member Variables
- : Function call information structure containing:
  - : The input array to shuffle

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM, ARR_DIMS, ARR_ELEMTYPE
  - lookup_type_cache
  - array_shuffle_n
  - PG_RETURN_ARRAYTYPE_P
- Called from (representative examples):
  - SQL function  (through function catalog)

## Notes and Other Information
- Returns the input array unchanged if it has fewer than 2 items in first dimension
- Preserves all dimensions and bounds of the original array
- Uses Fisher-Yates algorithm internally (via array_shuffle_n)
- Maintains TypeCacheEntry cache in fn_extra for performance across multiple calls
- Shuffles complete first dimension (unlike array_sample which takes a subset)
- Preserves original lower bound of first dimension (keep_lb=true)
- Memory efficient: delegates actual work to array_shuffle_n helper function
- Part of PostgreSQL's array utility functions for randomization
- Located in src/backend/utils/adt/array_userfuncs.c:1626-1659