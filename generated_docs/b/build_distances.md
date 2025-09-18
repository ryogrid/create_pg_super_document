# build_distances

## Location
src/backend/access/brin/brin_minmax_multi.c: 1329 - 1385

## Overview
Computes the distances (gaps) between consecutive expanded ranges in a BRIN minmax-multi index structure to identify which ranges can be efficiently merged.

## Definition


## Detailed Description
This function analyzes an array of expanded ranges and calculates the size of gaps between each consecutive pair of ranges. For n ranges, it computes (n-1) gap distances. The function uses a provided distance function to calculate the difference between the maximum value of one range and the minimum value of the next range. These distance calculations are used later by the range merging logic to determine which ranges should be combined to optimize storage efficiency in BRIN indexes.

The computed distances are sorted in descending order so that the largest gaps appear first, allowing the calling code to prioritize merging ranges with smaller gaps while preserving larger gaps that provide better selectivity.

## Parameters / Member Variables
- : Function pointer to the distance calculation function for the specific data type
- : Collation identifier for proper comparison of values  
- : Array of expanded ranges to analyze
- : Number of ranges in the array

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall2Coll
  - DatumGetFloat8
  - qsort
  - compare_distances
  - palloc0
- Types referenced:
  - ExpandedRange
  - DistanceValue
- Called from:
  - ensure_free_space_in_buffer
  - compactify_ranges
  - brin_minmax_multi_union

## Notes and Other Information
- Returns NULL if only a single range is provided (no gaps to calculate)
- The function is static and used internally within the BRIN minmax-multi access method
- Distance calculations may be expensive depending on the data type, so they are performed once and cached
- The resulting distances array is sorted to optimize the range merging process
- Part of PostgreSQL's BRIN (Block Range INdex) implementation for handling multiple values per range