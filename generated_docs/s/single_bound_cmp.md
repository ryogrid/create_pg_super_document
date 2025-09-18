# single_bound_cmp

## Location
src/backend/utils/adt/rangetypes_gist.c: 1731 - 1743

## Overview
A comparison function used for sorting SingleBoundSortItem structures during GiST range index splitting operations.

## Definition


## Detailed Description
This function serves as a comparator for qsort operations when sorting range bounds during GiST index node splitting. It compares two SingleBoundSortItem structures by comparing their underlying range bounds using the appropriate type-specific comparison logic.

The function follows the standard qsort comparator interface, taking two void pointers to the items being compared and an additional argument (the type cache entry) that provides the necessary type information for bound comparison.

## Parameters / Member Variables
- : Pointer to the first SingleBoundSortItem to compare
- : Pointer to the second SingleBoundSortItem to compare  
- : Pointer to TypeCacheEntry containing type-specific comparison information

## Dependencies
- Functions called/Symbols referenced:
  - SingleBoundSortItem (struct type)
  - TypeCacheEntry (struct type)
  - range_cmp_bounds
- Called from:
  - rangeCopy (src/backend/utils/adt/rangetypes_gist.c:181)
  - range_gist_single_sorting_split (src/backend/utils/adt/rangetypes_gist.c:1267)

## Notes and Other Information
- This is a static function used internally within the range types GiST implementation
- Designed specifically for use with qsort or similar sorting algorithms
- The comparison logic is delegated to range_cmp_bounds which handles the type-specific comparison details
- Part of the GiST index splitting strategy for range types to maintain balanced tree structure