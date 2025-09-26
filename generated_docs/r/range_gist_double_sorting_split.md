# range_gist_double_sorting_split

## Location
[src/backend/utils/adt/rangetypes_gist.c:1318-1620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1318-L1620)

## Overview
An advanced GiST splitting algorithm that uses double sorting to minimize overlap between left and right groups by analyzing both lower and upper bounds of ranges.

## Definition
static void range_gist_double_sorting_split(TypeCacheEntry *typcache, GistEntryVector *entryvec, GIST_SPLITVEC *v)

## Detailed Description
This function implements the most sophisticated splitting strategy for GiST range indexes, based on the "double sorting-based node splitting algorithm for R-tree" by A. Korotkov. The algorithm aims to minimize overlap between the resulting left and right groups while maintaining acceptable distribution ratios.

The algorithm works in several phases:
1. **Preparation**: Creates two sorted arrays of range bounds - one sorted by lower bounds, another by upper bounds
2. **Split candidate evaluation**: Considers multiple split points by iterating through possible boundaries and evaluating each with 
3. **Entry distribution**: Distributes entries into three categories: definitely left, definitely right, and "common" entries that could go to either side
4. **Common entry resolution**: Uses delta calculations to optimally distribute common entries based on their proximity to group boundaries

The algorithm considers splits where the left group has an upper bound and the right group has a lower bound, trying to minimize overlap while ensuring both groups contain a reasonable number of entries. Common entries are distributed using subtype difference calculations when available, falling back to equal distribution otherwise.

## Parameters / Member Variables
- : Type cache entry containing range type information and comparison functions
- : Vector containing all the index entries to be split
- : Output structure that will contain the split result, including left/right entries and union ranges

## Dependencies
- Functions called/Symbols referenced:
  - [range_deserialize](range_deserialize.md)
  - qsort_arg
  - [interval_cmp_lower](../i/interval_cmp_lower.md)
  - [interval_cmp_upper](../i/interval_cmp_upper.md)
  - [range_cmp_bounds](range_cmp_bounds.md)
  - [range_gist_consider_split](range_gist_consider_split.md)
  - [range_gist_fallback_split](range_gist_fallback_split.md)
  - [call_subtype_diff](../c/call_subtype_diff.md)
  - [common_entry_cmp](../c/common_entry_cmp.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - PLACE_LEFT (macro)
  - PLACE_RIGHT (macro)
  - FirstOffsetNumber (constant)
  - OffsetNumberNext
  - [ConsiderSplitContext](../C/ConsiderSplitContext.md) (struct)
  - [NonEmptyRange](../N/NonEmptyRange.md) (struct)
  - CommonEntry (struct)
  - RangeBound (struct)
- Called from (representative examples):
  - [range_gist_picksplit](range_gist_picksplit.md)

## Notes and Other Information
- This is a static function, only accessible within the rangetypes_gist.c file
- Most sophisticated of the range splitting algorithms, providing optimal spatial organization
- Falls back to range_gist_fallback_split if no acceptable split is found
- Uses subtype difference functions when available for more precise delta calculations
- Implements the LIMIT_RATIO constraint to ensure balanced splits
- The algorithm is based on academic research and provides near-optimal index organization
- Handles degenerate cases by falling back to simpler splitting methods
- Common entries are sorted by delta values to distribute the most ambiguous entries first
- The two-phase iteration (lower bounds first, then upper bounds) ensures all possible optimal splits are considered
- Memory allocation for temporary arrays (by_lower, by_upper, common_entries) is done upfront for efficiency