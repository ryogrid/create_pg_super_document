# range_gist_single_sorting_split

## Location
[src/backend/utils/adt/rangetypes_gist.c:1229-1317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1229-L1317)

## Overview
A GiST splitting function that sorts range entries by either their lower or upper bounds and splits them into two equal halves to optimize spatial ordering.

## Definition
static void range_gist_single_sorting_split(TypeCacheEntry *typcache, GistEntryVector *entryvec, GIST_SPLITVEC *v, bool use_upper_bound)

## Detailed Description
This function implements a sorting-based splitting strategy for GiST index operations on range types. It sorts all entries by a single bound (either lower or upper, depending on the use_upper_bound parameter) and then divides them into two equal groups. The first half of the sorted entries go to the left page, and the second half go to the right page.

The algorithm works by first extracting the appropriate bound from each range entry, sorting these bounds using a comparison function, and then placing entries in order. This approach tends to produce better spatial locality than random splitting, as ranges with similar bounds end up on the same page, which can improve query performance for range-based queries.

The function uses an auxiliary array of SingleBoundSortItem structures to maintain the association between bounds and their original entry indices during the sorting process.

## Parameters / Member Variables
- : Type cache entry containing range type information and comparison functions
- : Vector containing all the index entries to be split
- : Output structure that will contain the split result, including left/right entries and union ranges
- : Boolean flag indicating whether to sort by upper bounds (true) or lower bounds (false)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetRangeTypeP
  - [range_deserialize](range_deserialize.md)
  - qsort_arg
  - [single_bound_cmp](../s/single_bound_cmp.md)
  - PLACE_LEFT (macro)
  - PLACE_RIGHT (macro)
  - RangeTypePGetDatum
  - FirstOffsetNumber (constant)
  - OffsetNumberNext
  - SingleBoundSortItem (struct)
  - RangeBound (struct)
- Called from (representative examples):
  - [range_gist_picksplit](range_gist_picksplit.md)

## Notes and Other Information
- This is a static function, only accessible within the rangetypes_gist.c file
- Always produces a balanced split in terms of number of entries (splits exactly in half)
- The sorting approach can produce better spatial locality than simple positional splitting
- Uses qsort_arg to allow passing the type cache as context to the comparison function
- The Assert(!empty) ensures that empty ranges are not processed by this function
- Creates better organized index pages by grouping ranges with similar bounds together
- More sophisticated than fallback splitting but less complex than double sorting split
- The auxiliary array must be allocated and filled before sorting can occur