# ginMergeItemPointers

## Location
[src/backend/access/gin/ginpostinglist.c:378-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L378-L434)

## Overview
Merges two sorted arrays of ItemPointers into a single sorted array while eliminating duplicates, providing efficient set union functionality for GIN index operations.

## Definition

```c
ItemPointer
ginMergeItemPointers(ItemPointerData *a, uint32 na,
					 ItemPointerData *b, uint32 nb,
					 int *nmerged)
```
## Detailed Description
This function implements an optimized merge algorithm for combining two pre-sorted arrays of ItemPointers. It employs multiple strategies depending on the relationship between the input arrays to maximize efficiency:

1. **Non-overlapping optimization**: If the arrays don't overlap (all elements in one array are less than all elements in the other), it simply concatenates them using fast memory copy operations.

2. **Standard merge**: When arrays overlap, it performs a classic merge algorithm, comparing elements from both arrays and advancing pointers appropriately while eliminating duplicates.

The function automatically detects duplicates during the merge process and ensures each unique ItemPointer appears only once in the result. The output is always a properly sorted array suitable for further GIN index operations.

## Parameters / Member Variables
- : First sorted array of ItemPointers to merge
- : Number of elements in array 'a'
- : Second sorted array of ItemPointers to merge  
- : Number of elements in array 'b'
- : Output parameter that receives the number of items in the merged result

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [ginCompareItemPointers](ginCompareItemPointers.md)
  - memcpy (for non-overlapping optimization)
- Called from (representative examples):
  - [addItemsToLeaf](../a/addItemsToLeaf.md) (gin/gindatapage.c:1525)
  - [leafRepackItems](../l/leafRepackItems.md) (gin/gindatapage.c:1658)
  - [addItemPointersToLeafTuple](../a/addItemPointersToLeafTuple.md) (gin/gininsert.c:69)
  - [ginRedoRecompress](ginRedoRecompress.md) (gin/ginxlog.c:233)

## Notes and Other Information
- The function allocates space for the worst-case scenario (na + nb items) but the actual result may be smaller due to duplicate elimination
- Optimized for common cases where input arrays don't overlap, using fast memory operations instead of element-by-element comparison
- Critical for GIN index maintenance operations like page splits, tuple insertions, and vacuum processes
- The returned array must be freed by the caller using pfree()
- Maintains strict ordering requirements essential for GIN index correctness
- Used extensively during index updates where new items need to be merged with existing posting lists

## Simplified Source

```c
ItemPointer ginMergeItemPointers(ItemPointerData *a, uint32 na,
                                ItemPointerData *b, uint32 nb,
                                int *nmerged) {
    ItemPointerData *dst;

    // Allocate worst-case space (all items unique)
    dst = (ItemPointer) palloc((na + nb) * sizeof(ItemPointerData));

    // Optimization: if arrays don't overlap, just concatenate
    if (na == 0 || nb == 0 || ginCompareItemPointers(&a[na - 1], &b[0]) < 0) {
        // Array 'a' is entirely before 'b'
        memcpy(dst, a, na * sizeof(ItemPointerData));
        memcpy(&dst[na], b, nb * sizeof(ItemPointerData));
        *nmerged = na + nb;
    } else if (ginCompareItemPointers(&b[nb - 1], &a[0]) < 0) {
        // Array 'b' is entirely before 'a'
        memcpy(dst, b, nb * sizeof(ItemPointerData));
        memcpy(&dst[nb], a, na * sizeof(ItemPointerData));
        *nmerged = na + nb;
    } else {
        // Arrays overlap - need standard merge with duplicate elimination
        ItemPointerData *dptr = dst;
        ItemPointerData *aptr = a;
        ItemPointerData *bptr = b;

        // Merge while both arrays have elements
        while (aptr - a < na && bptr - b < nb) {
            int cmp = ginCompareItemPointers(aptr, bptr);

            if (cmp > 0)
                *dptr++ = *bptr++;      // b element is smaller
            else if (cmp == 0) {
                *dptr++ = *bptr++;      // Equal - keep one copy
                aptr++;                  // Skip duplicate from a
            } else
                *dptr++ = *aptr++;      // a element is smaller
        }

        // Copy remaining elements from whichever array isn't exhausted
        while (aptr - a < na)
            *dptr++ = *aptr++;
        while (bptr - b < nb)
            *dptr++ = *bptr++;

        *nmerged = dptr - dst;
    }

    return dst;
}
```