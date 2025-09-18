# find_next_mcelem

## Location
src/backend/utils/adt/array_selfuncs.c: 1130 - 1164

## Overview
Binary-searches a most common elements array for the first element greater than or equal to a given value, starting from a specified index position.

## Definition


## Detailed Description
This function performs a binary search on an array of most common elements (mcelem) to locate the first element that is greater than or equal to the specified value. The search begins from the position indicated by the *index parameter. The function assumes that mcelem elements are distinct, ensuring at most one exact match can exist.

The function updates the *index parameter to point to the position of the found element (exact match) or the position where the value would be inserted (first element >= value). It returns true for an exact match and false otherwise.

## Parameters / Member Variables
- : Array of Datum values representing the most common elements, assumed to be sorted
- : Number of elements in the mcelem array
- : The Datum value to search for
- : Pointer to starting search position; updated to position of match or insertion point
- : Type cache entry used for element comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - element_compare
- Called from (representative examples):
  - mcelem_array_contain_overlap_selec

## Notes and Other Information
- Uses standard binary search algorithm with left and right bounds
- Modifies the index parameter as both input (starting position) and output (result position)
- Critical for PostgreSQL's array selectivity estimation functionality
- Assumes mcelem array is pre-sorted and contains distinct elements
- Part of the array statistics and query planning infrastructure