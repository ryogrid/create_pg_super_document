# reduce_expanded_ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:1476-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1476-L1557)

## Overview
Reduces the number of expanded ranges by merging adjacent ranges with the smallest gaps until the total number of boundary values falls below a specified threshold, optimizing storage efficiency in BRIN minmax-multi indexes.

## Definition

```c
static int
reduce_expanded_ranges(ExpandedRange *eranges, int neranges,
					   DistanceValue *distances, int max_values,
					   FmgrInfo *cmp, Oid colloid)
```
## Detailed Description
This function implements a range consolidation algorithm that merges adjacent ranges to reduce storage requirements while preserving as much selectivity as possible. The algorithm works by identifying the smallest gaps between consecutive ranges (using pre-computed distances) and merging ranges across those gaps. It starts with global minimum and maximum values, then adds boundary values for the largest gaps that should be preserved.

The algorithm aims to keep the most significant gaps (largest distances) intact while merging ranges separated by smaller gaps. This approach maintains good index selectivity for queries while meeting storage constraints. The function uses a greedy strategy, selecting gaps to preserve based purely on distance, though the code comments note that this may not always be optimal for cases with many equal-length gaps.

## Parameters / Member Variables
- : Array of expanded ranges to reduce (modified in-place)
- : Number of ranges in the input array
- : Pre-computed distances between consecutive ranges, sorted by size
- : Maximum number of boundary values allowed in the result
- : Comparison function for the data type
- : Collation identifier for proper value comparison

## Dependencies
- Functions called/Symbols referenced:
  - [compare_values](../c/compare_values.md)
  - qsort_arg
  - [palloc](../p/palloc.md)
- Types referenced:
  - [ExpandedRange](../E/ExpandedRange.md)
  - [DistanceValue](../D/DistanceValue.md)
  - [compare_context](../c/compare_context.md)
- Called from:
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [compactify_ranges](../c/compactify_ranges.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- Returns the number of ranges in the reduced result
- Modifies the input eranges array in-place to contain the merged ranges
- Uses a greedy algorithm that may not be optimal for all data distributions
- The algorithm preserves (max_values / 2 - 1) of the largest gaps
- Collapsed ranges (single-point ranges) are properly identified in the result
- Comments indicate potential future improvements like considering range lengths or adding randomization
- Part of the BRIN index space management system for handling storage constraints
- The function includes extensive comments discussing algorithmic trade-offs and potential improvements