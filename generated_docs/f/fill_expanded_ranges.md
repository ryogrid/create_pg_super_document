# fill_expanded_ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:1134-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1134-L1178)

## Overview
Expands Ranges data structure into an ExpandedRange array, converting both range intervals and individual values into a uniform expanded range format.

## Definition
static void fill_expanded_ranges(ExpandedRange *eranges, int neranges, Ranges *ranges)

## Detailed Description
This function converts the compact Ranges representation into an expanded ExpandedRange array format for easier processing. It performs two main operations:

1. **Range Expansion**: Converts stored intervals from the ranges structure into ExpandedRange elements, where each range keeps its min/max bounds and is marked as not collapsed.

2. **Value Expansion**: Converts individual stored values into ExpandedRange elements where both minval and maxval are set to the same value, and the element is marked as collapsed (representing a single point).

The function processes ranges first (which are sorted), followed by values (which may include unsorted values), resulting in a partially ordered array where the range portion is sorted but the value portion may not be.

## Parameters / Member Variables
- : Pre-allocated ExpandedRange array to fill (must have correct size)
- : Number of elements in the eranges array (must equal ranges->nranges + ranges->nvalues)
- : Source Ranges structure containing intervals and values to expand

## Dependencies
- Functions called/Symbols referenced:
  - [ExpandedRange](../E/ExpandedRange.md)
  - [Ranges](../R/Ranges.md)

- Called from (representative examples):
  - [build_expanded_ranges](../b/build_expanded_ranges.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- The function includes assertion checks to verify the output array size matches the expected total elements
- The resulting ExpandedRange array has ranges first (sorted) followed by values (potentially unsorted)
- Each expanded range is marked with a collapsed flag: false for true ranges, true for single values
- This expansion facilitates subsequent operations like sorting and merging that work more easily with uniform range structures
- The function is static and used internally within the BRIN minmax_multi implementation