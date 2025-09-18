# ExpandedRange

## Location
[src/backend/access/brin/brin_minmax_multi.c:237-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L237-L242)

## Overview
ExpandedRange is a helper structure used internally by BRIN minmax-multi operations to represent individual ranges in an expanded format that simplifies merging, sorting, and combining operations.

## Definition


## Detailed Description
ExpandedRange serves as an intermediate representation for ranges during BRIN minmax-multi processing operations. Each instance represents a single interval with explicit minimum and maximum boundary values. The structure includes a collapsed flag to efficiently handle single-point ranges (where min equals max) without requiring expensive comparison function calls for by-reference data types. This representation is primarily used during compaction operations where the complex hybrid storage format of Ranges needs to be temporarily expanded into individual intervals for sorting, merging overlapping ranges, and other algorithmic processing.

## Parameters / Member Variables
- `minval`: Datum representing the lower boundary value of the range interval
- `maxval`: Datum representing the upper boundary value of the range interval  
- `collapsed`: Boolean flag indicating whether this represents a single-point range (minval == maxval); used to optimize comparisons for by-reference data types

## Dependencies
- Used extensively by functions:
  - [AssertCheckExpandedRanges](../A/AssertCheckExpandedRanges.md)
  - [compare_expanded_ranges](../c/compare_expanded_ranges.md)
  - [fill_expanded_ranges](../f/fill_expanded_ranges.md)
  - [sort_expanded_ranges](../s/sort_expanded_ranges.md)
  - [merge_overlapping_ranges](../m/merge_overlapping_ranges.md)
  - [build_distances](../b/build_distances.md)
  - [build_expanded_ranges](../b/build_expanded_ranges.md)
  - [count_values](../c/count_values.md)
  - [reduce_expanded_ranges](../r/reduce_expanded_ranges.md)
  - [store_expanded_ranges](../s/store_expanded_ranges.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [compactify_ranges](../c/compactify_ranges.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- Temporary structure used during complex range processing operations
- The collapsed flag avoids expensive datum comparisons for by-reference data types
- Primarily used during compaction when the Ranges structure is expanded for algorithmic processing
- Enables efficient sorting and merging algorithms on range data
- Not used for persistent storage - only for intermediate processing steps
- Critical for the range consolidation algorithms that maintain the maxvalues constraint
- Part of the internal machinery that keeps BRIN minmax-multi indexes efficient and compact