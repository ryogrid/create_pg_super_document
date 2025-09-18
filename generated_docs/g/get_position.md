# get_position

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 794 - 872

## Overview
Calculates the relative position of a range bound value within a histogram bin, returning a normalized position in the range [0,1] for interpolation during selectivity estimation.

## Definition
```c
static float8 get_position(TypeCacheEntry *typcache, const RangeBound *value, const RangeBound *hist1, const RangeBound *hist2)
```

## Detailed Description
This function determines where a specific range bound value lies within a histogram bin bounded by `hist1` and `hist2`. The result is a normalized position between 0.0 and 1.0, where 0.0 indicates the value is at the lower bound and 1.0 indicates it's at the upper bound. This is essential for accurate interpolation during selectivity estimation.

The function handles several cases:
1. **Finite bounds**: Uses the range type's subdiff function to calculate precise relative position
2. **Infinite lower bound**: Returns 0.0 for -infinite values, 1.0 for finite values  
3. **Infinite upper bound**: Returns 1.0 for +infinite values, 0.0 for finite values
4. **Both bounds infinite**: Returns 0.5 (middle position)

When both bounds are finite, the function uses the `rng_subdiff_finfo` function to compute differences, enabling accurate interpolation for numeric and date/time types.

## Parameters / Member Variables
- `typcache`: Type cache entry containing subdiff function and collation information for the range type
- `value`: Target range bound whose position within the bin needs to be determined
- `hist1`: Lower boundary of the histogram bin
- `hist2`: Upper boundary of the histogram bin

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall2Coll
  - DatumGetFloat8
  - isnan
  - RangeBound
- Called from (representative examples):
  - calc_hist_selectivity_scalar
  - calc_hist_selectivity_contained
  - calc_hist_selectivity_contains

## Notes and Other Information
- Returns 0.5 as a fallback when subdiff function is unavailable or returns NaN/invalid results
- Ensures the returned position is always clamped to [0,1] range using Max/Min operations
- Handles infinite bounds gracefully by using logical positioning rather than mathematical calculation
- Critical for accurate selectivity estimation in PostgreSQL's query planner for range predicates
- The subdiff function must be available for precise interpolation with finite numeric bounds