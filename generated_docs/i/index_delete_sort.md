# index_delete_sort

## Location
[src/backend/access/heap/heapam.c:8440-8536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8440-L8536)

## Overview
Sorts a deltids array from delstate by TID to prepare it for further processing by heap_index_delete_tuples(), using a specialized shellsort implementation optimized for performance.

## Definition
```c
static void index_delete_sort(TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function implements a highly optimized sorting routine specifically designed for TM_IndexDelete arrays. It uses shellsort with a carefully chosen gap sequence from the Sedgewick-Incerpi paper, which provides excellent performance for the typical array sizes encountered in PostgreSQL (up to ~4500 elements, covering all supported BLCKSZ values).

The function is performance-critical and includes several micro-optimizations:
- Specialized shellsort algorithm that compiles to few instructions
- Adaptive behavior for presorted inputs/subsets (common in this context)  
- Static assertion ensuring TM_IndexDelete elements are ≤8 bytes to keep swaps cheap
- Gap sequence optimized for PostgreSQL's typical usage patterns

The sorting prepares the deletion array for efficient processing by ensuring items are ordered by their physical location (TID), which improves I/O patterns during actual deletion operations.

## Parameters / Member Variables
- `delstate`: Pointer to TM_IndexDeleteOp structure containing the deltids array to sort and related metadata

## Dependencies
- Functions called/Symbols referenced:
  - [index_delete_sort_cmp](index_delete_sort_cmp.md)
  - StaticAssertDecl
  - lengthof
  - [TM_IndexDeleteOp](../T/TM_IndexDeleteOp.md) (structure type)
  - [TM_IndexDelete](../T/TM_IndexDelete.md) (structure type)
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)

## Notes and Other Information
- Uses shellsort with specific gap sequence: {1968, 861, 336, 112, 48, 21, 7, 3, 1}
- Optimized for array sizes up to ~4500 elements (covers all supported BLCKSZ values)
- Includes compile-time assertion ensuring element size ≤8 bytes for performance
- The comment warns to "think carefully before changing anything" due to performance sensitivity
- Adaptive to presorted inputs, which are typical in this usage context
- Located in src/backend/access/heap/heapam.c:8440-8536

## Simplified Source

```c
static void index_delete_sort(TM_IndexDeleteOp *delstate)
{
    TM_IndexDelete *deltids = delstate->deltids;
    int ndeltids = delstate->ndeltids;

    // Optimized shellsort gap sequence from Sedgewick-Incerpi paper
    // Efficient for arrays up to ~4500 elements (covers all BLCKSZ values)
    const int gaps[9] = {1968, 861, 336, 112, 48, 21, 7, 3, 1};

    // Ensure element size is suitable for fast swaps
    StaticAssertDecl(sizeof(TM_IndexDelete) <= 8, "element size exceeds 8 bytes");

    // Shellsort with specialized gap sequence
    for (int g = 0; g < lengthof(gaps); g++)
    {
        int gap = gaps[g];

        // Insertion sort with gap spacing
        for (int i = gap; i < ndeltids; i++)
        {
            TM_IndexDelete temp = deltids[i];
            int j = i;

            // Move elements that are greater than temp
            while (j >= gap && index_delete_sort_cmp(&deltids[j - gap], &temp) >= 0)
            {
                deltids[j] = deltids[j - gap];
                j -= gap;
            }

            deltids[j] = temp;
        }
    }
}
```