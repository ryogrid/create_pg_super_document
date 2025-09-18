# get_distance

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 918 - 965

## Overview
Measures the distance between two range bounds using the range type's subdiff function, returning a float8 value representing the magnitude of separation between the bounds.

## Definition
```c
static float8 get_distance(TypeCacheEntry *typcache, const RangeBound *bound1, const RangeBound *bound2)
```

## Detailed Description
This function calculates the distance between two range bounds, which is essential for selectivity estimation in range containment and overlap operations. The distance measurement depends on the availability of the range type's subdiff function and the nature of the bounds (finite vs infinite).

The function handles several scenarios:
1. **Both bounds finite**: Uses the `rng_subdiff_finfo` function to compute the actual distance; returns 1.0 as fallback if subdiff is unavailable or returns invalid results (NaN, negative)
2. **Both bounds infinite**: Returns 0.0 if they're the same infinite bound (both +∞ or both -∞), otherwise returns positive infinity
3. **One bound infinite**: Always returns positive infinity representing unbounded distance

The distance calculation is fundamental to estimating how much of a range histogram bin is covered by containment or overlap predicates.

## Parameters / Member Variables
- `typcache`: Type cache entry containing subdiff function and collation information for the range type
- `bound1`: First range bound for distance measurement
- `bound2`: Second range bound for distance measurement

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - isnan
  - get_float8_infinity
  - RangeBound
- Called from (representative examples):
  - [calc_hist_selectivity_contained](../c/calc_hist_selectivity_contained.md)
  - [calc_hist_selectivity_contains](../c/calc_hist_selectivity_contains.md)

## Notes and Other Information
- Returns 1.0 as a reasonable default when subdiff function is unavailable or produces invalid results
- Uses `get_float8_infinity()` to represent unbounded distances when dealing with infinite bounds
- The subdiff function must handle the specific semantics of the range type (numeric, temporal, etc.)
- Critical for accurate selectivity estimation in PostgreSQL's query planner for range containment operations
- Defensive against NaN and negative results from subdiff functions to maintain estimation stability