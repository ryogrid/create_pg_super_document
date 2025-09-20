# removeDontCares

## Location
[src/backend/access/gist/gistsplit.c:167-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L167-L199)

## Overview
Removes tuples marked as "don't care" from a tuple index array, compacting the array in-place.

## Definition

```c
static void
removeDontCares(OffsetNumber *a, int *len, const bool *dontcare)
```
## Detailed Description
This utility function processes an array of tuple indices (typically  or  from a GistSplitVector) and removes entries that correspond to tuples marked as "don't care" in the provided boolean array. The function compacts the array in-place by copying non-don't-care entries to the beginning of the array and updating the length accordingly. This is used during GiST index splitting to exclude certain tuples from split calculations while maintaining the original array structure.

## Parameters / Member Variables
- : Array of OffsetNumber values representing tuple indices to be filtered
- : Pointer to integer containing the current length of array ; updated to reflect the new length after removal
- : Boolean array where  indicates whether tuple at offset  should be considered a don't-care entry

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (PostgreSQL type for tuple offsets)
  - No function calls (purely algorithmic processing)
- Called from:
  - [gistUserPicksplit](../g/gistUserPicksplit.md) (at src/backend/access/gist/gistsplit.c:513)
  - [gistUserPicksplit](../g/gistUserPicksplit.md) (at src/backend/access/gist/gistsplit.c:514)

## Notes and Other Information
- This is a static function, only accessible within the gistsplit.c file
- Performs in-place array compaction without additional memory allocation
- The function preserves the order of non-don't-care entries in the original array
- Used in conjunction with  to first identify and then remove don't-care tuples
- Applied separately to both left and right side arrays during split processing
- The dontcare array is indexed by tuple offset numbers, not by position in the input array 
- Essential for split optimization where some tuples can be freely reassigned between split sides