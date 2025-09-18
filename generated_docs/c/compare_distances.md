# compare_distances

## Location
src/backend/access/brin/brin_minmax_multi.c: 1305 - 1328

## Overview
Comparison function for DistanceValue structures that sorts distances in descending order for use with qsort algorithms.

## Definition
static int compare_distances(const void *a, const void *b)

## Detailed Description
This is a standard qsort-compatible comparison function designed specifically for DistanceValue structures. It implements descending order sorting, meaning larger distance values will appear first in the sorted array. This ordering is intentional and serves the algorithm's need to identify and process the largest gaps between ranges first.

The function follows the standard C comparison function contract:
- Returns positive value if a > b
- Returns negative value if a < b  
- Returns 0 if a == b

However, the return values are inverted to achieve descending order (larger distances first).

## Parameters / Member Variables
- : Pointer to first DistanceValue structure for comparison
- : Pointer to second DistanceValue structure for comparison

## Dependencies
- Functions called/Symbols referenced:
  - DistanceValue

- Called from (representative examples):
  - build_distances

## Notes and Other Information
- Specifically designed for descending order sorting (largest gaps first)
- Used in BRIN minmax_multi range optimization algorithms where identifying the largest gaps between ranges is crucial
- The descending order allows algorithms to prioritize eliminating the largest gaps first when reducing the number of ranges
- Simple double value comparison with no special handling for NaN or infinity values
- The function is static and used internally within the BRIN minmax_multi implementation
- Part of the range consolidation strategy where largest inter-range distances are candidates for elimination