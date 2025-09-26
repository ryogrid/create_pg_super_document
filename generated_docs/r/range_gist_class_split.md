# range_gist_class_split

## Location
[src/backend/utils/adt/rangetypes_gist.c:1186-1228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1186-L1228)

## Overview
A GiST splitting function for range types that classifies ranges into semantic categories and splits entries based on these classifications to optimize spatial locality.

## Definition
static void range_gist_class_split(TypeCacheEntry *typcache, GistEntryVector *entryvec, GIST_SPLITVEC *v, SplitLR *classes_groups)

## Detailed Description
This function implements a class-based splitting strategy for GiST index operations on range types. It classifies each range entry using  and then assigns entries to left or right pages based on the classification and a predefined grouping strategy specified in the  array.

The classification system recognizes different types of ranges (such as empty ranges, point ranges, ranges with different bound combinations) and groups similar range types together to improve query performance. This approach is more sophisticated than simple positional splitting as it considers the semantic properties of the ranges.

Each entry is processed by determining its class, looking up the destination (left or right) from the classes_groups array, and placing it accordingly. The function also maintains union ranges for both pages to create appropriate parent index entries.

## Parameters / Member Variables
- : Type cache entry containing range type information
- : Vector containing all the index entries to be split
- : Output structure that will contain the split result, including left/right entries and union ranges  
- : Array of length CLS_COUNT specifying which side (SPLIT_LEFT or SPLIT_RIGHT) each range class should be assigned to

## Dependencies
- Functions called/Symbols referenced:
  - [get_gist_range_class](../g/get_gist_range_class.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - PLACE_LEFT (macro)
  - PLACE_RIGHT (macro)
  - [RangeTypePGetDatum](../R/RangeTypePGetDatum.md)
  - FirstOffsetNumber (constant)
  - OffsetNumberNext
  - SPLIT_LEFT (constant)
  - SPLIT_RIGHT (constant)
- Called from (representative examples):
  - [range_gist_picksplit](range_gist_picksplit.md)

## Notes and Other Information
- This is a static function, only accessible within the rangetypes_gist.c file
- Uses semantic classification of ranges rather than simple positional splitting
- The classes_groups parameter allows different splitting strategies to be applied using the same classification system
- More sophisticated than fallback splitting but may not always produce optimal splits for all data distributions
- The Assert statement ensures that each class is assigned to exactly one side of the split
- Relies on the get_gist_range_class function to categorize ranges into meaningful semantic groups