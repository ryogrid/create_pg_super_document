# build_expanded_ranges

## Location
src/backend/access/brin/brin_minmax_multi.c: 1386 - 1414

## Overview
Constructs a unified array of expanded ranges from separate collections of ranges and single-point values, providing a normalized representation for easier processing in BRIN minmax-multi indexes.

## Definition


## Detailed Description
This function takes a Ranges structure containing both multi-value ranges and single-point values and converts them into a unified array of ExpandedRange structures. This normalization allows the system to process ranges and individual points uniformly, simplifying subsequent operations like merging and compacting. The function allocates memory for an expanded representation where each range and each individual value becomes a separate ExpandedRange element. After creation, the expanded ranges are sorted and deduplicated to ensure consistent ordering and remove any overlapping entries.

The expanded representation is essential for range merging algorithms as it provides a consistent interface regardless of whether the original data was stored as ranges or individual points.

## Parameters / Member Variables
- : Function pointer to the comparison function for the data type
- : Collation identifier for proper sorting and comparison
- : Input structure containing both ranges and individual values
- : Output parameter that receives the number of expanded ranges created

## Dependencies
- Functions called/Symbols referenced:
  - [fill_expanded_ranges](../f/fill_expanded_ranges.md)
  - [sort_expanded_ranges](../s/sort_expanded_ranges.md)
  - [palloc0](../p/palloc0.md)
- Types referenced:
  - [Ranges](../R/Ranges.md)
  - [ExpandedRange](../E/ExpandedRange.md)
- Called from:
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [compactify_ranges](../c/compactify_ranges.md)

## Notes and Other Information
- Returns a newly allocated array of ExpandedRange structures
- The total number of expanded ranges equals the sum of original ranges plus individual values
- Sorting and deduplication are performed to handle potentially unsorted input data
- The function is static and used internally within the BRIN minmax-multi implementation
- Memory allocation uses palloc0 to ensure proper initialization of the expanded ranges
- Part of the BRIN index optimization process for managing multiple values per block range