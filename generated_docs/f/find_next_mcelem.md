# find_next_mcelem

## Location
[src/backend/utils/adt/array_selfuncs.c:1130-1164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L1130-L1164)

## Overview
Binary-searches a most common elements array for the first element greater than or equal to a given value, starting from a specified index position.

## Definition

```c
static bool
find_next_mcelem(Datum *mcelem, int nmcelem, Datum value, int *index,
				 TypeCacheEntry *typentry)
```
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
  - [element_compare](../e/element_compare.md)
- Called from (representative examples):
  - [mcelem_array_contain_overlap_selec](../m/mcelem_array_contain_overlap_selec.md)

## Notes and Other Information
- Uses standard binary search algorithm with left and right bounds
- Modifies the index parameter as both input (starting position) and output (result position)
- Critical for PostgreSQL's array selectivity estimation functionality
- Assumes mcelem array is pre-sorted and contains distinct elements
- Part of the array statistics and query planning infrastructure

## Simplified Source

```c
static bool find_next_mcelem(Datum *mcelem, int nmcelem, Datum value, int *index,
                             TypeCacheEntry *typentry) {
    // Initialize binary search bounds
    int l = *index;
    int r = nmcelem - 1;

    // Standard binary search algorithm
    while (l <= r) {
        int i = (l + r) / 2;
        int res = element_compare(&mcelem[i], &value, typentry);

        if (res == 0) {
            // Exact match found
            *index = i;
            return true;
        } else if (res < 0) {
            l = i + 1; // Search right half
        } else {
            r = i - 1; // Search left half
        }
    }

    // Not found - return insertion position
    *index = l;
    return false;
}
```