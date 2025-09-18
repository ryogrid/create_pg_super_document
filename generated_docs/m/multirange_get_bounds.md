# multirange_get_bounds

## Location
src/backend/utils/adt/multirangetypes.c: 744 - 801

## Overview
This function efficiently extracts the lower and upper bounds from the i-th range within a multirange, providing a performance-optimized alternative to deserializing a complete range object.

## Definition


## Detailed Description
The function provides a streamlined way to access range bounds without the overhead of constructing a complete RangeType structure. It directly extracts bounds data from the multirange's compressed format, handling type-specific considerations like alignment and value fetching. The function properly handles both finite and infinite bounds, as well as inclusive/exclusive boundaries.

This is an optimized shortcut that performs the equivalent of calling multirange_get_range() followed by range_deserialize(), but with significantly fewer operations and memory allocations. The function ensures that empty ranges are never encountered (as multiranges cannot contain empty ranges) and correctly interprets the range flags to populate the RangeBound structures.

## Parameters / Member Variables
- : TypeCacheEntry containing metadata about the range element type
- : Pointer to the source MultirangeType structure
- : Zero-based index of the range to extract bounds from
- : Output parameter for the lower bound information
- : Output parameter for the upper bound information

## Dependencies
- Functions called/Symbols referenced:
  - multirange_get_bounds_offset
  - MultirangeGetFlagsPtr
  - MultirangeGetBoundariesPtr
  - RANGE_EMPTY
  - RANGE_HAS_LBOUND
  - RANGE_HAS_UBOUND
  - fetch_att
  - att_addlength_pointer
  - att_align_pointer
  - RANGE_LB_INF, RANGE_LB_INC
  - RANGE_UB_INF, RANGE_UB_INC
- Called from (representative examples):
  - multirange_get_union_range
  - multirange_bsearch_match
  - multirange_lower
  - multirange_upper
  - multirange_overlaps_multirange_internal
  - range_contains_multirange_internal

## Notes and Other Information
- The function validates the index with an Assert to ensure it's within bounds
- Includes an assertion that multiranges cannot contain empty ranges
- Efficiently handles type alignment without unnecessary alignment operations for lower bounds
- Sets all RangeBound fields including val, infinite, inclusive, and lower flags
- Used extensively throughout multirange comparison and operation functions
- Provides significant performance benefits over full range deserialization when only bounds are needed