# ExpandedRange

## Location
src/backend/access/brin/brin_minmax_multi.c: 237 - 242

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
  - AssertCheckExpandedRanges
  - compare_expanded_ranges
  - fill_expanded_ranges
  - sort_expanded_ranges
  - merge_overlapping_ranges
  - build_distances
  - build_expanded_ranges
  - count_values
  - reduce_expanded_ranges
  - store_expanded_ranges
  - ensure_free_space_in_buffer
  - compactify_ranges
  - brin_minmax_multi_union

## Notes and Other Information
- Temporary structure used during complex range processing operations
- The collapsed flag avoids expensive datum comparisons for by-reference data types
- Primarily used during compaction when the Ranges structure is expanded for algorithmic processing
- Enables efficient sorting and merging algorithms on range data
- Not used for persistent storage - only for intermediate processing steps
- Critical for the range consolidation algorithms that maintain the maxvalues constraint
- Part of the internal machinery that keeps BRIN minmax-multi indexes efficient and compact